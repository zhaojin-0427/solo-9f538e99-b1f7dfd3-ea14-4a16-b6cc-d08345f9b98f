"""运行期配置：全部可通过环境变量覆盖，便于容器与测试使用。

双数据卷
--------
- ``data_dir``（旧卷）：SQLite 数据库、旧 ``blobs/`` 与上传临时目录；
- ``new_data_dir``（新卷）：迁移目标 ``blobs/`` 与拷贝临时目录；SQLite 不迁移。

``NEW_DATA_DIR`` 未设置时默认取 ``data_dir`` 的同级目录 ``new-data``，
方便本地无 Docker 运行；Docker Compose 中显式设为 ``/new-data``。
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_path(name: str) -> Optional[Path]:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    return Path(raw)


@dataclass(frozen=True)
class Settings:
    # 旧卷根目录：SQLite 数据库与旧 blobs 数据卷都在其下
    data_dir: Path = field(default_factory=lambda: Path(os.environ.get("DATA_DIR", "/data")))
    # 新卷根目录：仅承载迁移后的 blobs（SQLite 始终留在旧卷）
    new_data_dir: Optional[Path] = field(
        default_factory=lambda: _env_path("NEW_DATA_DIR")
    )
    db_name: str = field(default_factory=lambda: os.environ.get("DB_NAME", "moisture.db"))
    blobs_dirname: str = "blobs"
    tmp_dirname: str = "tmp"
    # 下载租约的生命周期保护在进程内完成；此参数仅用于防御性清理
    lease_ttl_seconds: int = field(default_factory=lambda: _env_int("LEASE_TTL_SECONDS", 3600))
    # 触发回收后，遇到正在下载的文件时的重试次数（每次重新校验引用）
    gc_max_passes: int = field(default_factory=lambda: _env_int("GC_MAX_PASSES", 3))
    # 单次请求允许的最大 CSV 字节数（默认 64 MiB）
    max_upload_bytes: int = field(default_factory=lambda: _env_int("MAX_UPLOAD_BYTES", 64 * 1024 * 1024))
    # 一次迁移运行内，对同一批失败项的连续重试轮数
    migration_max_attempts: int = field(
        default_factory=lambda: _env_int("MIGRATION_MAX_ATTEMPTS", 3)
    )
    # 失败重试的退避基数（秒），第 n 轮等待 n 倍
    migration_retry_delay_seconds: float = field(
        default_factory=lambda: _env_float("MIGRATION_RETRY_DELAY_SECONDS", 0.2)
    )

    def __post_init__(self) -> None:
        if self.new_data_dir is None:
            object.__setattr__(self, "new_data_dir", self.data_dir.parent / "new-data")
        else:
            object.__setattr__(self, "new_data_dir", Path(self.new_data_dir))
        if Path(self.new_data_dir).resolve() == Path(self.data_dir).resolve():
            raise ValueError("DATA_DIR 与 NEW_DATA_DIR 必须是两个不同的数据卷根目录")

    @property
    def db_path(self) -> Path:
        return self.data_dir / self.db_name

    @property
    def blobs_dir(self) -> Path:
        """旧卷 blobs 目录（兼容既有调用）。"""
        return self.data_dir / self.blobs_dirname

    @property
    def tmp_dir(self) -> Path:
        """旧卷临时目录（兼容既有调用）。"""
        return self.data_dir / self.tmp_dirname

    @property
    def new_blobs_dir(self) -> Path:
        return Path(self.new_data_dir) / self.blobs_dirname

    @property
    def new_tmp_dir(self) -> Path:
        return Path(self.new_data_dir) / self.tmp_dirname


settings = Settings()
