"""数据卷文件操作：内容寻址、原子落盘、安全读取、跨卷复制与删除。

两个数据卷
----------
- ``old``：``DATA_DIR``，既有数据的来源卷，SQLite 始终留在这里；
- ``new``：``NEW_DATA_DIR``，迁移目标卷，受理迁移后新上传直接落到这里。

同一 SHA-256 在任一时刻只有一份“有效”副本：``blobs.volume`` 指向它。
迁移会先把完整副本写到新卷临时文件、校验 SHA-256，再原子发布到新卷正式
路径，最后才删除旧卷原件。未复制完整的文件只存在于带 ``.`` 前缀的临时名
下，永远不可按 blob 路径读到。
"""
from __future__ import annotations

import errno
import hashlib
import os
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import BinaryIO, Optional, Tuple

from .config import Settings, settings as default_settings

OLD = "old"
NEW = "new"
VOLUMES = (OLD, NEW)


class UploadTooLarge(Exception):
    """上传字节数超过配置上限。"""


class BlobCopyError(Exception):
    """跨卷复制失败或校验不通过（源文件损坏/复制不完整）。"""


def volume_root(volume: str, cfg: Settings = default_settings) -> Path:
    if volume == OLD:
        return cfg.data_dir
    if volume == NEW:
        return Path(cfg.new_data_dir)
    raise ValueError(f"未知数据卷: {volume!r}")


def blobs_dir(volume: str, cfg: Settings = default_settings) -> Path:
    return volume_root(volume, cfg) / cfg.blobs_dirname


def tmp_dir(volume: str, cfg: Settings = default_settings) -> Path:
    return volume_root(volume, cfg) / cfg.tmp_dirname


def blob_path(
    sha256: str, cfg: Settings = default_settings, volume: str = OLD
) -> Path:
    # 两级目录分片，避免单目录文件过多
    return blobs_dir(volume, cfg) / sha256[:2] / sha256[2:4] / sha256


def _is_under(path: Path, directory: Path) -> bool:
    target = Path(path).resolve()
    root = Path(directory).resolve()
    try:
        return target.is_relative_to(root)
    except AttributeError:  # pragma: no cover - Python < 3.9
        return str(target).startswith(str(root) + os.sep)


def tmp_volume(tmp_path: Path, cfg: Settings = default_settings) -> str:
    """判定临时文件所在的数据卷（发布时据此选择目标卷）。"""
    if _is_under(tmp_path, tmp_dir(NEW, cfg)):
        return NEW
    return OLD


def other_volume(volume: str) -> str:
    return NEW if volume == OLD else OLD


def ensure_dirs(cfg: Settings = default_settings) -> None:
    for volume in VOLUMES:
        blobs_dir(volume, cfg).mkdir(parents=True, exist_ok=True)
        tmp_dir(volume, cfg).mkdir(parents=True, exist_ok=True)


def stream_to_tmp(
    source: BinaryIO,
    cfg: Settings = default_settings,
    chunk_size: int = 1024 * 1024,
    volume: str = OLD,
) -> Tuple[str, int, Path]:
    """流式读取上传内容到临时文件，同时计算 SHA-256。

    ``volume`` 决定临时文件放在哪个数据卷：它必须与最终发布目标同卷，
    这样发布是同一文件系统内的原子 ``os.replace``。
    成功返回 (sha256, size, tmp_path)；超限抛出 :class:`UploadTooLarge`，
    临时文件由调用方在失败时清理（这里也会尽力清理）。
    """
    digest = hashlib.sha256()
    total = 0
    target_tmp = tmp_dir(volume, cfg)
    target_tmp.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix="upload-", dir=target_tmp)
    tmp_path = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as out:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    break
                total += len(chunk)
                if total > cfg.max_upload_bytes:
                    raise UploadTooLarge(f"upload exceeds {cfg.max_upload_bytes} bytes")
                digest.update(chunk)
                out.write(chunk)
            out.flush()
            os.fsync(out.fileno())
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
    return digest.hexdigest(), total, tmp_path


def move_tmp_to_volume(
    tmp_path: Path, volume: str, cfg: Settings = default_settings
) -> Path:
    """确保临时文件位于目标卷的 tmp 目录。

    正常情况下上传在开始时就按目标卷选择 tmp，不需要搬运；只有“上传途中
    才有人受理迁移”这种竞态才会触发一次跨卷拷贝，保证随后仍是原子发布。
    """
    target_tmp = tmp_dir(volume, cfg)
    target_tmp.mkdir(parents=True, exist_ok=True)
    tmp_path = Path(tmp_path)
    if _is_under(tmp_path, target_tmp):
        return tmp_path
    fd, name = tempfile.mkstemp(prefix="upload-", dir=target_tmp)
    new_path = Path(name)
    try:
        with open(tmp_path, "rb") as src, os.fdopen(fd, "wb") as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
    except Exception:
        new_path.unlink(missing_ok=True)
        raise
    tmp_path.unlink(missing_ok=True)
    return new_path


def _stage_in_dir(tmp_path: Path, directory: Path) -> Path:
    """把临时文件放到目标目录的隐藏暂存名，必要时跨文件系统拷贝。"""
    directory.mkdir(parents=True, exist_ok=True)
    staged = directory / f".staging-{uuid.uuid4().hex}"
    try:
        os.replace(tmp_path, staged)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        with open(tmp_path, "rb") as src, open(staged, "wb") as dst:
            shutil.copyfileobj(src, dst, 1024 * 1024)
            dst.flush()
            os.fsync(dst.fileno())
        tmp_path.unlink(missing_ok=True)
    return staged


def publish_blob(
    tmp_path: Path, sha256: str, cfg: Settings = default_settings
) -> Path:
    """把临时文件原子地发布为数据卷文件。

    目标卷由临时文件所在卷决定（上传时已按目标卷选择 tmp）。同内容
    （同 SHA-256）共用同一文件：目标已存在时直接丢弃临时文件。
    os.replace 是同文件系统内的原子操作，任何时刻读者要么看不到该文件，
    要么看到完整文件。
    """
    target = blob_path(sha256, cfg, tmp_volume(tmp_path, cfg))
    if target.exists():
        Path(tmp_path).unlink(missing_ok=True)
        return target
    staged = _stage_in_dir(Path(tmp_path), target.parent)
    try:
        os.replace(staged, target)
    except FileExistsError:  # pragma: no cover - os.replace 通常直接覆盖
        staged.unlink(missing_ok=True)
    return target


def discard_tmp(tmp_path: Path) -> None:
    Path(tmp_path).unlink(missing_ok=True)


def open_blob(
    sha256: str, cfg: Settings = default_settings, volume: str = OLD
) -> BinaryIO:
    """以只读方式打开数据卷文件（调用方负责关闭）。"""
    return open(blob_path(sha256, cfg, volume), "rb")


def stage_copy(
    sha256: str,
    cfg: Settings = default_settings,
    source_volume: str = OLD,
    target_volume: str = NEW,
    chunk_size: int = 1024 * 1024,
) -> Tuple[Path, int]:
    """把源卷 blob 完整复制到目标卷 tmp，并校验 SHA-256。

    返回 (暂存路径, 字节数)。任何读取/写入/校验失败都会删除暂存文件并抛出
    :class:`BlobCopyError` 或底层 OSError；正式 blob 路径此时尚不存在，因此
    “未复制完整的文件不可读取”。校验通过后由调用方在元数据临界区内发布。
    """
    source = blob_path(sha256, cfg, source_volume)
    target_tmp = tmp_dir(target_volume, cfg)
    target_tmp.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    total = 0
    fd, name = tempfile.mkstemp(prefix="migration-", dir=target_tmp)
    staged = Path(name)
    try:
        with open(source, "rb") as src, os.fdopen(fd, "wb") as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                digest.update(chunk)
                total += len(chunk)
                dst.write(chunk)
            dst.flush()
            os.fsync(dst.fileno())
    except Exception as exc:
        staged.unlink(missing_ok=True)
        raise BlobCopyError(f"复制 blob {sha256} 失败: {exc!r}") from exc

    if digest.hexdigest() != sha256:
        staged.unlink(missing_ok=True)
        raise BlobCopyError(f"blob {sha256} 复制校验失败：内容哈希不一致")
    return staged, total


def remove_blob_file(
    sha256: str, cfg: Settings = default_settings, volume: str = OLD
) -> bool:
    """删除数据卷文件，返回是否真的执行了删除。

    文件不存在视为已删除，保证“删除失败/中断后重试”可以幂等推进。
    """
    path = blob_path(sha256, cfg, volume)
    try:
        path.unlink()
    except FileNotFoundError:
        return False
    # 清理可能留下的空分片目录（非空时静默跳过）
    for parent in (path.parent, path.parent.parent):
        try:
            parent.rmdir()
        except OSError:
            pass
    return True


def blob_exists(
    sha256: str, cfg: Settings = default_settings, volume: str = OLD
) -> bool:
    return blob_path(sha256, cfg, volume).exists()


def list_blob_files(cfg: Settings = default_settings, volume: str = OLD):
    """枚举某数据卷上现存的 blob 文件（用于重启后的对账）。"""
    directory = blobs_dir(volume, cfg)
    if not directory.exists():
        return []
    return [
        p.name
        for p in directory.glob("*/*/*")
        if p.is_file() and not p.name.startswith(".")
    ]


def find_readable_volume(
    sha256: str, cfg: Settings = default_settings, preferred: str = OLD
) -> Optional[str]:
    """返回可读取该 blob 的卷：优先登记卷，其次另一卷（崩溃窗口的兜底）。"""
    if blob_exists(sha256, cfg, preferred):
        return preferred
    alternate = other_volume(preferred)
    if blob_exists(sha256, cfg, alternate):
        return alternate
    return None
