"""业务服务层：分区上传替换、快照固定、下载租约、安全回收与双卷迁移。

引用模型
--------
一个 blob（SHA-256 标识的不可变文件）在以下任一位置被引用即不可回收：
  1. 某分区的当前版本（partitions.current_blob / partition_entries）；
  2. 任一未释放快照的条目（snapshot_entries，所属 snapshots.released_at IS NULL）。
回收前会在同一把 ``metadata_lock`` 临界区内重新校验引用与下载租约，
因此与上传、释放、下载、回收交错时仍有引用的文件绝不会被删除。

迁移模型
--------
``POST /migrations`` 受理后立即返回进度号，后台线程把**已登记的旧卷 blob**
逐个搬到新卷；SQLite 与上传临时目录始终留在旧卷。每个文件：
  1. 在锁外把源文件复制到新卷 tmp，并校验 SHA-256（未完成前没有正式路径）；
  2. 在 ``metadata_lock`` 短临界区内复查登记：登记已消失说明已被回收，
     丢弃副本、绝不重建登记（迟到的复制结果不能复活已回收文件）；
  3. 原子发布完整副本到新卷，并把 ``blobs.volume`` 切到 ``new``；
  4. 另一次短临界区复查登记与下载租约后删除旧卷原件，成功后才算“已删除”。
受理之后的新上传直接落到新卷；已受理的下载在打开文件时持有句柄与租约，
旧副本删除会跳过有租约的 blob，因此不会打断下载。
"""
from __future__ import annotations

import csv
import io
import math
import sqlite3
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import BinaryIO, Dict, List, Optional, Sequence

import anyio

from . import storage
from .config import Settings, settings as default_settings
from .db import connect, metadata_lock, transaction

# 迁移状态机取值（持久化在 SQLite，重启后可继续）
JOB_PENDING = "pending"
JOB_RUNNING = "running"
JOB_FAILED = "failed"
JOB_COMPLETED = "completed"

ITEM_PENDING = "pending"   # 尚未复制，或复制失败待重试
ITEM_COPIED = "copied"     # 新卷已有完整副本，旧卷原件待删除
ITEM_DELETED = "deleted"   # 旧卷原件已实际移除
ITEM_GONE = "gone"         # 没有可迁移原件（登记已不在 / 原件本就缺失）
ITEM_FAILED = "failed"     # 重试耗尽，等待再次发起或重启接续

ITEM_DONE = (ITEM_DELETED, ITEM_GONE)
# 单向迁移的指纹：同一部署只有这一次迁移，重复发起沿用同一进度号。
MIGRATION_FINGERPRINT = "soil-moisture-csv:old->new"


class ServiceError(Exception):
    """业务错误基类。"""


class CsvValidationError(ServiceError):
    """CSV 内容不符合要求。"""


class NotFound(ServiceError):
    """分区、快照或迁移任务不存在。"""


class SnapshotReleased(ServiceError):
    """快照已释放，拒绝新的读取。"""


class MissingBlobFile(ServiceError):
    """登记在册的 blob 在数据卷上缺失（数据卷损坏）。"""


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _iso(ts: datetime) -> str:
    return ts.isoformat(timespec="seconds")


# --------------------------------------------------------------------------- #
# CSV 解析
# --------------------------------------------------------------------------- #
def parse_csv_bytes(data: bytes) -> List[Dict[str, object]]:
    """解析上传的 CSV。

    要求：UTF-8（允许 BOM）；首行表头必须包含“站点”和“湿度”两列
    （大小写/前后空格不敏感，兼容 station / moisture 英文名）；
    湿度必须是有限数值；站点不能为空。返回 [{"station", "moisture"}, ...]。
    """
    if not data:
        raise CsvValidationError("CSV 内容为空")
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise CsvValidationError(f"CSV 必须是 UTF-8 编码: {exc}") from exc

    reader = csv.reader(io.StringIO(text))
    try:
        header = next(reader)
    except StopIteration as exc:
        raise CsvValidationError("CSV 缺少表头") from exc

    normalized = [h.strip().lower() for h in header]
    station_aliases = {"站点", "station", "site", "station_id", "site_id"}
    moisture_aliases = {"湿度", "moisture", "soil_moisture", "sm"}
    try:
        station_idx = next(i for i, h in enumerate(normalized) if h in station_aliases)
        moisture_idx = next(i for i, h in enumerate(normalized) if h in moisture_aliases)
    except StopIteration as exc:
        raise CsvValidationError(
            f"表头必须包含站点列和湿度列，实际表头: {header}"
        ) from exc

    rows: List[Dict[str, object]] = []
    for line_no, raw in enumerate(reader, start=2):
        if not raw or all(not c.strip() for c in raw):
            continue  # 容忍空行
        if max(station_idx, moisture_idx) >= len(raw):
            raise CsvValidationError(f"第 {line_no} 行列数不足: {raw}")
        station = raw[station_idx].strip()
        if not station:
            raise CsvValidationError(f"第 {line_no} 行站点为空")
        moisture_raw = raw[moisture_idx].strip()
        try:
            moisture = float(moisture_raw)
        except ValueError as exc:
            raise CsvValidationError(
                f"第 {line_no} 行湿度不是数值: {moisture_raw!r}"
            ) from exc
        if not math.isfinite(moisture):
            raise CsvValidationError(f"第 {line_no} 行湿度不是有限数值: {moisture_raw!r}")
        rows.append({"station": station, "moisture": moisture})

    if not rows:
        raise CsvValidationError("CSV 没有任何数据行")
    return rows


# --------------------------------------------------------------------------- #
# 服务
# --------------------------------------------------------------------------- #
class MoistureService:
    def __init__(self, cfg: Settings = default_settings):
        self.cfg = cfg
        # 进程内活动下载租约计数：lease_id -> blob_sha
        self._active_readers: Dict[str, str] = {}
        self._readers_lock = threading.Lock()
        # 活动下载流的强引用集合，防止异步生成器在响应结束前被 GC
        # （否则其 finally 会提前释放租约）
        self._active_streams: set = set()
        # 每个迁移任务至多一个后台线程
        self._migration_threads: Dict[int, threading.Thread] = {}
        self._migration_threads_lock = threading.Lock()
        self._migration_stop = threading.Event()

    # -- 基础工具 ---------------------------------------------------------- #
    def _conn(self) -> sqlite3.Connection:
        return connect(self.cfg)

    def startup(self) -> None:
        """建表、清理临时文件、对账两个数据卷、继续回收与未完成迁移。"""
        from .db import init_db

        self._migration_stop.clear()
        init_db(self.cfg)
        self._cleanup_tmp()
        self.reconcile_disk()
        self.run_gc()
        self.resume_migrations()

    def shutdown(self) -> None:
        """停止后台迁移线程（进度已持久化，下次启动接续）。"""
        self._migration_stop.set()
        with self._migration_threads_lock:
            threads = list(self._migration_threads.values())
            self._migration_threads.clear()
        for thread in threads:
            thread.join(timeout=5)

    def _cleanup_tmp(self) -> None:
        for volume in storage.VOLUMES:
            tmp = storage.tmp_dir(volume, self.cfg)
            if not tmp.exists():
                continue
            for p in tmp.iterdir():
                if p.is_file():
                    p.unlink(missing_ok=True)
            # 发布/迁移在 rename 前崩溃可能留下隐藏暂存文件；它们从不被当作
            # 正式 blob，启动时一并清掉，未完成复制的内容绝不可读。
            blobs = storage.blobs_dir(volume, self.cfg)
            if blobs.exists():
                for p in blobs.glob("*/*/.staging-*"):
                    if p.is_file():
                        p.unlink(missing_ok=True)

    # -- 引用判定（必须在 metadata_lock 下调用） --------------------------- #
    def _is_referenced(self, conn: sqlite3.Connection, sha: str) -> bool:
        row = conn.execute(
            """
            SELECT
                EXISTS(SELECT 1 FROM partitions WHERE current_blob = ?)
              OR EXISTS(SELECT 1 FROM partition_entries WHERE blob_sha = ?)
              OR EXISTS(
                    SELECT 1 FROM snapshot_entries se
                    JOIN snapshots s ON s.snapshot_id = se.snapshot_id
                    WHERE se.blob_sha = ? AND s.released_at IS NULL)
            AS referenced
            """,
            (sha, sha, sha),
        ).fetchone()
        return bool(row["referenced"])

    def _has_lease(self, conn: sqlite3.Connection, sha: str) -> bool:
        # 仅统计未过期租约；过期租约由 sweep_expired_leases 定期清理
        row = conn.execute(
            "SELECT COUNT(*) AS n FROM download_leases"
            " WHERE blob_sha = ? AND expires_at > ?",
            (sha, _iso(_utcnow())),
        ).fetchone()
        return row["n"] > 0

    def sweep_expired_leases(self) -> int:
        """删除超过 TTL 的租约（防御进程崩溃遗留的死租约）。"""
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    cur = conn.execute(
                        "DELETE FROM download_leases WHERE expires_at <= ?",
                        (_iso(_utcnow()),),
                    )
                    return cur.rowcount
            finally:
                conn.close()

    def _enqueue_gc(self, conn: sqlite3.Connection, shas: Sequence[str]) -> None:
        now = _iso(_utcnow())
        for sha in shas:
            if not sha:
                continue
            # 重新引用（同内容再次上传）时旧队列项作废
            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha,))
            conn.execute(
                "INSERT INTO gc_queue(sha256, enqueued_at, attempts) VALUES (?, ?, 0)",
                (sha, now),
            )

    # -- 上传：整份替换当前分区 ------------------------------------------- #
    def _migration_accepted(self, conn: sqlite3.Connection) -> bool:
        return (
            conn.execute("SELECT 1 FROM migration_jobs LIMIT 1").fetchone()
            is not None
        )

    def upload_partition(
        self, partition_id: str, stream: BinaryIO
    ) -> Dict[str, object]:
        """按日期分区上传 CSV，整份替换当前分区。

        流程：先把字节完整落到临时文件并算出 SHA-256（此阶段不触碰任何
        现有登记），然后在一个事务里登记 blob + 替换分区；任何一步失败
        原分区保持不变。迁移受理后新上传直接落到新卷；若上传途中才受理，
        登记前会把临时文件搬到新卷 tmp 再原子发布。
        """
        # 迁移一旦受理就不会撤销，因此这个前置判断只会“偏旧”，随后在
        # metadata_lock 内还会以数据库为准复核一次。
        conn = self._conn()
        try:
            target_volume = (
                storage.NEW if self._migration_accepted(conn) else storage.OLD
            )
        finally:
            conn.close()

        sha, size, tmp_path = storage.stream_to_tmp(
            stream, self.cfg, volume=target_volume
        )
        replaced = False
        published = False
        wake_migration = False
        try:
            # 解析在事务外完成：校验失败时绝不动现有分区
            with open(tmp_path, "rb") as fh:
                rows = parse_csv_bytes(fh.read())

            now = _iso(_utcnow())
            with metadata_lock:
                conn = self._conn()
                try:
                    # 复核目标卷；上传途中才受理迁移时，把临时文件搬到新卷
                    accepted = self._migration_accepted(conn)
                    target_volume = storage.NEW if accepted else storage.OLD
                    if storage.tmp_volume(tmp_path, self.cfg) != target_volume:
                        tmp_path = storage.move_tmp_to_volume(
                            tmp_path, target_volume, self.cfg
                        )

                    with transaction(conn):
                        # 登记 blob（若同 SHA 已存在则复用，实现数据卷共用）
                        existing = conn.execute(
                            "SELECT sha256, volume FROM blobs WHERE sha256 = ?", (sha,)
                        ).fetchone()
                        if existing:
                            # 同内容重新上传/仍被引用：取消任何待回收记录
                            conn.execute("DELETE FROM gc_queue WHERE sha256 = ?", (sha,))
                        else:
                            conn.execute(
                                "INSERT INTO blobs(sha256, size, created_at, volume)"
                                " VALUES (?, ?, ?, ?)",
                                (sha, size, now, target_volume),
                            )
                        storage.publish_blob(tmp_path, sha, self.cfg)
                        published = True

                        if existing and existing["volume"] != target_volume:
                            # 同内容重传落在新卷：登记切到新卷（新副本刚由
                            # 本事务完整发布），旧卷副本交给迁移/回收清理。
                            conn.execute(
                                "UPDATE blobs SET volume = ? WHERE sha256 = ?",
                                (target_volume, sha),
                            )
                            if target_volume == storage.NEW:
                                wake_migration = self._mark_item_copied(conn, sha, size)

                        old = conn.execute(
                            "SELECT current_blob FROM partitions WHERE partition_id = ?",
                            (partition_id,),
                        ).fetchone()
                        replaced = old is not None
                        if old is None:
                            conn.execute(
                                "INSERT INTO partitions(partition_id, current_blob,"
                                " row_count, created_at, updated_at)"
                                " VALUES (?, ?, ?, ?, ?)",
                                (partition_id, sha, len(rows), now, now),
                            )
                        else:
                            conn.execute(
                                "UPDATE partitions SET current_blob = ?, row_count = ?,"
                                " updated_at = ? WHERE partition_id = ?",
                                (sha, len(rows), now, partition_id),
                            )
                            conn.execute(
                                "DELETE FROM partition_entries WHERE partition_id = ?",
                                (partition_id,),
                            )
                        conn.executemany(
                            "INSERT INTO partition_entries(partition_id, row_index,"
                            " station, moisture, blob_sha) VALUES (?, ?, ?, ?, ?)",
                            [
                                (partition_id, i, r["station"], r["moisture"], sha)
                                for i, r in enumerate(rows)
                            ],
                        )
                        orphaned: List[str] = []
                        if old is not None and old["current_blob"]:
                            old_sha = old["current_blob"]
                            if old_sha != sha and not self._is_referenced(conn, old_sha):
                                orphaned.append(old_sha)
                        self._enqueue_gc(conn, orphaned)
                finally:
                    conn.close()
        finally:
            if not published:
                storage.discard_tmp(tmp_path)

        if wake_migration:
            self._wake_migration_worker()
        return {
            "partition_id": partition_id,
            "sha256": sha,
            "size": size,
            "row_count": len(rows),
            "replaced": replaced,
            "volume": target_volume,
        }

    # -- 快照：固定全部当前分区 ------------------------------------------- #
    def create_snapshot(self) -> Dict[str, object]:
        now = _iso(_utcnow())
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    cur = conn.execute(
                        "INSERT INTO snapshots(created_at) VALUES (?)", (now,)
                    )
                    snapshot_id = cur.lastrowid
                    conn.execute(
                        """
                        INSERT INTO snapshot_entries(snapshot_id, partition_id, blob_sha, row_count)
                        SELECT ?, partition_id, current_blob, row_count
                          FROM partitions
                         WHERE current_blob IS NOT NULL
                        """,
                        (snapshot_id,),
                    )
                    n = conn.execute(
                        "SELECT COUNT(*) AS n FROM snapshot_entries WHERE snapshot_id = ?",
                        (snapshot_id,),
                    ).fetchone()["n"]
                return {"snapshot_id": snapshot_id, "created_at": now, "partition_count": n}
            finally:
                conn.close()

    def _get_live_snapshot(self, conn: sqlite3.Connection, snapshot_id: int) -> sqlite3.Row:
        snap = conn.execute(
            "SELECT * FROM snapshots WHERE snapshot_id = ?", (snapshot_id,)
        ).fetchone()
        if snap is None:
            raise NotFound(f"快照 {snapshot_id} 不存在")
        if snap["released_at"] is not None:
            raise SnapshotReleased(f"快照 {snapshot_id} 已释放，拒绝新的读取")
        return snap

    def release_snapshot(self, snapshot_id: int) -> Dict[str, object]:
        now = _iso(_utcnow())
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    self._get_live_snapshot(conn, snapshot_id)
                    conn.execute(
                        "UPDATE snapshots SET released_at = ? WHERE snapshot_id = ?",
                        (now, snapshot_id),
                    )
                    # 该快照独占的 blob 立刻进入回收队列（仍有租约者稍后重试）
                    candidates = conn.execute(
                        "SELECT DISTINCT blob_sha FROM snapshot_entries WHERE snapshot_id = ?",
                        (snapshot_id,),
                    ).fetchall()
                    orphaned = [
                        r["blob_sha"]
                        for r in candidates
                        if not self._is_referenced(conn, r["blob_sha"])
                    ]
                    self._enqueue_gc(conn, orphaned)
                return {"snapshot_id": snapshot_id, "released_at": now}
            finally:
                conn.close()

    def list_snapshots(self) -> List[Dict[str, object]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                """
                SELECT s.snapshot_id, s.created_at, s.released_at,
                       (SELECT COUNT(*) FROM snapshot_entries se
                         WHERE se.snapshot_id = s.snapshot_id) AS partition_count
                  FROM snapshots s ORDER BY s.snapshot_id
                """
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # -- 下载 -------------------------------------------------------------- #
    def _acquire_lease(self, sha: str) -> str:
        lease_id = uuid.uuid4().hex
        now = _utcnow()
        expires = now + timedelta(seconds=self.cfg.lease_ttl_seconds)
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    conn.execute(
                        "INSERT INTO download_leases(lease_id, blob_sha, created_at, expires_at)"
                        " VALUES (?, ?, ?, ?)",
                        (lease_id, sha, _iso(now), _iso(expires)),
                    )
            finally:
                conn.close()
        with self._readers_lock:
            self._active_readers[lease_id] = sha
        return lease_id

    def _release_lease(self, lease_id: str) -> None:
        with metadata_lock:
            conn = self._conn()
            try:
                conn.execute("DELETE FROM download_leases WHERE lease_id = ?", (lease_id,))
            finally:
                conn.close()
        with self._readers_lock:
            self._active_readers.pop(lease_id, None)

    def _resolve_partition_blob(
        self, conn: sqlite3.Connection, partition_id: str, snapshot_id: Optional[int]
    ) -> str:
        if snapshot_id is not None:
            self._get_live_snapshot(conn, snapshot_id)
            row = conn.execute(
                "SELECT blob_sha FROM snapshot_entries"
                " WHERE snapshot_id = ? AND partition_id = ?",
                (snapshot_id, partition_id),
            ).fetchone()
            if row is None:
                raise NotFound(
                    f"快照 {snapshot_id} 中没有分区 {partition_id}"
                )
            return row["blob_sha"]
        row = conn.execute(
            "SELECT current_blob FROM partitions WHERE partition_id = ?",
            (partition_id,),
        ).fetchone()
        if row is None or row["current_blob"] is None:
            raise NotFound(f"分区 {partition_id} 不存在")
        return row["current_blob"]

    def _readable_path(self, conn: sqlite3.Connection, sha: str) -> Path:
        row = conn.execute(
            "SELECT volume FROM blobs WHERE sha256 = ?", (sha,)
        ).fetchone()
        preferred = row["volume"] if row is not None else storage.OLD
        volume = storage.find_readable_volume(sha, self.cfg, preferred)
        if volume is None:
            raise MissingBlobFile(f"blob {sha} 的数据卷文件缺失")
        return storage.blob_path(sha, self.cfg, volume)

    def open_partition_download(
        self, partition_id: str, snapshot_id: Optional[int] = None
    ):
        """在元数据临界区内解析、取租约并**打开文件句柄**。

        返回 (sha, 已打开文件, 关闭回调)。句柄在临界区内打开，之后迁移删除
        旧副本或回收 unlink 都不会影响已受理下载（Linux 打开句柄仍可读完），
        租约还会让删除逻辑主动跳过该 blob。流式读取期间不持有 metadata_lock。
        """
        with metadata_lock:
            conn = self._conn()
            try:
                sha = self._resolve_partition_blob(conn, partition_id, snapshot_id)
                path = self._readable_path(conn, sha)
                lease_id = self._acquire_lease(sha)
                try:
                    handle = open(path, "rb")
                except OSError as exc:
                    self._release_lease(lease_id)
                    raise MissingBlobFile(
                        f"blob {sha} 的数据卷文件无法打开: {exc}"
                    ) from exc
            finally:
                conn.close()

        closed = False

        def cleanup() -> None:
            nonlocal closed
            if closed:
                return
            closed = True
            try:
                handle.close()
            finally:
                self._release_lease(lease_id)

        return sha, handle, cleanup

    # -- 当前分区元数据 ---------------------------------------------------- #
    def get_partition(self, partition_id: str) -> Dict[str, object]:
        conn = self._conn()
        try:
            row = conn.execute(
                "SELECT * FROM partitions WHERE partition_id = ?", (partition_id,)
            ).fetchone()
            if row is None:
                raise NotFound(f"分区 {partition_id} 不存在")
            return dict(row)
        finally:
            conn.close()

    def list_partitions(self) -> List[Dict[str, object]]:
        conn = self._conn()
        try:
            rows = conn.execute(
                "SELECT partition_id, current_blob, row_count, created_at, updated_at"
                " FROM partitions ORDER BY partition_id"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # -- 回收 -------------------------------------------------------------- #
    def reconcile_disk(self) -> Dict[str, int]:
        """重启/启动对账：把两个数据卷上无登记的 blob 纳入回收队列。"""
        enqueued = 0
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    known = {r["sha256"] for r in conn.execute("SELECT sha256 FROM blobs")}
                    on_disk = set()
                    for volume in storage.VOLUMES:
                        on_disk |= set(storage.list_blob_files(self.cfg, volume))
                    now = _iso(_utcnow())
                    for sha in on_disk - known:
                        conn.execute(
                            "INSERT OR IGNORE INTO gc_queue(sha256, enqueued_at, attempts)"
                            " VALUES (?, ?, 0)",
                            (sha, now),
                        )
                        enqueued += 1
            finally:
                conn.close()
        return {"orphan_files_enqueued": enqueued}

    def run_gc(self) -> Dict[str, object]:
        """处理回收队列，可安全反复调用、可在重启后继续。

        每个候选 blob 在真正 unlink 前于同一临界区内重新校验：
        仍被当前分区/未释放快照引用 -> 出队保留；
        仍有下载租约 -> 留在队列等下一轮（慢下载不阻塞其他文件回收）；
        否则删除两个数据卷上的副本并移除登记。任何单文件失败只记录错误。
        """
        deleted: List[str] = []
        kept_referenced: List[str] = []
        busy: List[str] = []
        failed: List[Dict[str, str]] = []

        # 先清扫崩溃遗留的过期死租约，避免它们永久阻塞回收
        self.sweep_expired_leases()

        # 多轮处理：让“租约刚结束”的文件在同一次调用内尽快回收；
        # 仍在下载的文件留在队列，且不阻塞其他文件的删除。
        for _ in range(max(1, self.cfg.gc_max_passes)):
            busy_this_pass: List[str] = []
            with metadata_lock:
                conn = self._conn()
                try:
                    queued = conn.execute(
                        "SELECT sha256 FROM gc_queue ORDER BY enqueued_at"
                    ).fetchall()
                    for item in queued:
                        sha = item["sha256"]
                        # 每文件一个独立短事务 + 短临界区
                        try:
                            with transaction(conn):
                                referenced = self._is_referenced(conn, sha)
                                leased = self._has_lease(conn, sha)
                                blob_exists = conn.execute(
                                    "SELECT 1 FROM blobs WHERE sha256 = ?", (sha,)
                                ).fetchone()
                                if referenced:
                                    conn.execute(
                                        "DELETE FROM gc_queue WHERE sha256 = ?", (sha,)
                                    )
                                    kept_referenced.append(sha)
                                    continue
                                if leased:
                                    conn.execute(
                                        "UPDATE gc_queue SET attempts = attempts + 1,"
                                        " last_error = 'active download lease'"
                                        " WHERE sha256 = ?",
                                        (sha,),
                                    )
                                    busy_this_pass.append(sha)
                                    continue
                                # 引用与租约都为空：在同一临界区内 unlink，
                                # 之后任何并发上传都不可能再让“旧文件”复活，
                                # 同内容重新上传会走全新的原子发布。
                                # 先清理已释放快照留下的条目（其快照已不可读，
                                # 这些行不再构成引用；不清理会触发外键约束）。
                                conn.execute(
                                    """
                                    DELETE FROM snapshot_entries
                                     WHERE blob_sha = ?
                                       AND snapshot_id IN (
                                           SELECT snapshot_id FROM snapshots
                                            WHERE released_at IS NOT NULL)
                                    """,
                                    (sha,),
                                )
                                # 两个卷上的副本都清掉：既覆盖登记所在卷，也
                                # 覆盖迁移/崩溃留下的重复副本。
                                for volume in storage.VOLUMES:
                                    storage.remove_blob_file(sha, self.cfg, volume)
                                if blob_exists:
                                    conn.execute(
                                        "DELETE FROM blobs WHERE sha256 = ?", (sha,)
                                    )
                                conn.execute(
                                    "DELETE FROM gc_queue WHERE sha256 = ?", (sha,)
                                )
                                deleted.append(sha)
                        except Exception as exc:
                            # 上面的事务已回滚；在新事务里记录错误，留待下次重试
                            try:
                                with transaction(conn):
                                    conn.execute(
                                        "UPDATE gc_queue SET attempts = attempts + 1,"
                                        " last_error = ? WHERE sha256 = ?",
                                        (repr(exc), sha),
                                    )
                            except Exception:
                                pass
                            failed.append({"sha256": sha, "error": repr(exc)})
                finally:
                    conn.close()
            busy.extend(busy_this_pass)
            if not busy_this_pass:
                break

        return {
            "deleted": deleted,
            "kept_referenced": kept_referenced,
            "busy_with_active_download": sorted(set(busy)),
            "failed": failed,
            "queue_remaining": self.gc_queue_size(),
        }

    def gc_queue_size(self) -> int:
        conn = self._conn()
        try:
            return conn.execute("SELECT COUNT(*) AS n FROM gc_queue").fetchone()["n"]
        finally:
            conn.close()

    def active_reader_count(self) -> int:
        with self._readers_lock:
            return len(self._active_readers)

    def blob_file_counts(self) -> Dict[str, int]:
        counts = {
            volume: len(storage.list_blob_files(self.cfg, volume))
            for volume in storage.VOLUMES
        }
        counts["total"] = sum(counts.values())
        return counts

    def stream_blob(self, handle: BinaryIO, cleanup, chunk_size: int = 1024 * 1024):
        """构造受租约保护的异步文件流（文件句柄已在临界区内打开）。

        使用 async 生成器而非同步生成器：Starlette 对同步迭代器会用
        ``iterate_in_threadpool`` 临时包装，内层生成器在两次取值之间可能被
        GC，导致 finally 提前释放租约。async 生成器会被响应迭代器强引用
        到流结束；这里再用 ``_active_streams`` 集合保留一份强引用做双保险。
        阻塞的文件读取放进 anyio 工作线程，不卡住事件循环。
        """
        async def generator():
            stream_token = object()
            self._active_streams.add(stream_token)
            try:
                while True:
                    chunk = await anyio.to_thread.run_sync(handle.read, chunk_size)
                    if not chunk:
                        break
                    yield chunk
            finally:
                self._active_streams.discard(stream_token)
                cleanup()

        return generator()

    # ------------------------------------------------------------------ #
    # 双卷迁移
    # ------------------------------------------------------------------ #
    @staticmethod
    def _job_progress(conn: sqlite3.Connection, job: sqlite3.Row) -> Dict[str, object]:
        counts = {
            ITEM_PENDING: 0,
            ITEM_COPIED: 0,
            ITEM_DELETED: 0,
            ITEM_GONE: 0,
            ITEM_FAILED: 0,
        }
        bytes_by_status = {k: 0 for k in counts}
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n, COALESCE(SUM(size), 0) AS bytes"
            " FROM migration_items WHERE job_id = ? GROUP BY status",
            (job["job_id"],),
        ).fetchall()
        for row in rows:
            status = row["status"]
            if status not in counts:
                continue
            counts[status] = row["n"]
            bytes_by_status[status] = row["bytes"]
        total = sum(counts.values())
        done = counts[ITEM_DELETED] + counts[ITEM_GONE]
        return {
            "job_id": job["job_id"],
            "migration_id": job["job_id"],
            "progress_id": job["job_id"],
            "status": job["status"],
            "completed": job["status"] == JOB_COMPLETED,
            "total": total,
            "pending": counts[ITEM_PENDING],
            "copied": counts[ITEM_COPIED],
            "deleted": counts[ITEM_DELETED],
            "gone": counts[ITEM_GONE],
            "failed": counts[ITEM_FAILED],
            "done": done,
            "percent": round(100.0 * done / total, 2) if total else 100.0,
            "bytes_total": sum(bytes_by_status.values()),
            "bytes_migrated": bytes_by_status[ITEM_DELETED]
            + bytes_by_status[ITEM_COPIED],
            "source_volume": storage.OLD,
            "target_volume": storage.NEW,
            "created_at": job["created_at"],
            "updated_at": job["updated_at"],
            "completed_at": job["completed_at"],
            "last_error": job["last_error"],
        }

    def request_migration(self) -> Dict[str, object]:
        """受理一次单向迁移并立即返回进度号；后台线程负责实际复制。

        同一部署重复发起沿用同一个任务/进度号：已完成则原样返回；失败或
        尚未完成则重置失败项的重试计数并继续。新增旧卷 blob 会自动补入条目。
        """
        now = _iso(_utcnow())
        spawn = False
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    job = conn.execute(
                        "SELECT * FROM migration_jobs WHERE fingerprint = ?",
                        (MIGRATION_FINGERPRINT,),
                    ).fetchone()
                    reused = job is not None
                    if job is None:
                        cur = conn.execute(
                            "INSERT INTO migration_jobs(fingerprint, status, created_at,"
                            " updated_at) VALUES (?, ?, ?, ?)",
                            (MIGRATION_FINGERPRINT, JOB_PENDING, now, now),
                        )
                        job_id = cur.lastrowid
                        job = conn.execute(
                            "SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,)
                        ).fetchone()
                    else:
                        job_id = job["job_id"]
                        if job["status"] == JOB_FAILED:
                            # 失败可接续：失败项回到 pending，重试计数按本次运行重置
                            conn.execute(
                                "UPDATE migration_items SET status = ?, attempts = 0,"
                                " last_error = NULL WHERE job_id = ? AND status = ?",
                                (ITEM_PENDING, job_id, ITEM_FAILED),
                            )
                            conn.execute(
                                "UPDATE migration_jobs SET status = ?, updated_at = ?,"
                                " last_error = NULL WHERE job_id = ?",
                                (JOB_PENDING, now, job_id),
                            )
                            job = conn.execute(
                                "SELECT * FROM migration_jobs WHERE job_id = ?",
                                (job_id,),
                            ).fetchone()

                    added = self._sync_migration_items(conn, job_id)
                    conn.execute(
                        "UPDATE migration_jobs SET total_items = ?, updated_at = ?"
                        " WHERE job_id = ?",
                        (self._count_items(conn, job_id), now, job_id),
                    )
                    job = conn.execute(
                        "SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,)
                    ).fetchone()
                    # 若又有旧卷 blob 出现（极端竞态），已完成的任务重新变为待处理
                    if added and job["status"] == JOB_COMPLETED:
                        conn.execute(
                            "UPDATE migration_jobs SET status = ?, completed_at = NULL,"
                            " updated_at = ? WHERE job_id = ?",
                            (JOB_PENDING, now, job_id),
                        )
                        job = conn.execute(
                            "SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,)
                        ).fetchone()
                    progress = self._job_progress(conn, job)
                    spawn = progress["status"] != JOB_COMPLETED
                progress["reused"] = reused
                progress["accepted"] = True
            finally:
                conn.close()

        if spawn:
            self._spawn_migration_worker(job_id)
        return progress

    def get_migration(self, job_id: int) -> Dict[str, object]:
        conn = self._conn()
        try:
            job = conn.execute(
                "SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if job is None:
                raise NotFound(f"迁移任务 {job_id} 不存在")
            return self._job_progress(conn, job)
        finally:
            conn.close()

    def list_migrations(self) -> List[Dict[str, object]]:
        conn = self._conn()
        try:
            jobs = conn.execute(
                "SELECT * FROM migration_jobs ORDER BY job_id"
            ).fetchall()
            return [self._job_progress(conn, job) for job in jobs]
        finally:
            conn.close()

    def resume_migrations(self) -> List[int]:
        """启动恢复：未完成任务重置本次重试计数并重新拉起后台线程。"""
        now = _iso(_utcnow())
        resumed: List[int] = []
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    jobs = conn.execute(
                        "SELECT job_id, status FROM migration_jobs"
                        " WHERE status IN (?, ?, ?)",
                        (JOB_PENDING, JOB_RUNNING, JOB_FAILED),
                    ).fetchall()
                    for job in jobs:
                        job_id = job["job_id"]
                        conn.execute(
                            "UPDATE migration_items SET status = ?, attempts = 0,"
                            " last_error = NULL WHERE job_id = ? AND status = ?",
                            (ITEM_PENDING, job_id, ITEM_FAILED),
                        )
                        conn.execute(
                            "UPDATE migration_items SET attempts = 0, last_error = NULL"
                            " WHERE job_id = ? AND status IN (?, ?)",
                            (job_id, ITEM_PENDING, ITEM_COPIED),
                        )
                        conn.execute(
                            "UPDATE migration_jobs SET status = ?, updated_at = ?,"
                            " last_error = NULL WHERE job_id = ?",
                            (JOB_PENDING, now, job_id),
                        )
                        resumed.append(job_id)
            finally:
                conn.close()
        for job_id in resumed:
            self._spawn_migration_worker(job_id)
        return resumed

    # -- 迁移状态机内部实现 ---------------------------------------------- #
    def _count_items(self, conn: sqlite3.Connection, job_id: int) -> int:
        return conn.execute(
            "SELECT COUNT(*) AS n FROM migration_items WHERE job_id = ?", (job_id,)
        ).fetchone()["n"]

    def _sync_migration_items(self, conn: sqlite3.Connection, job_id: int) -> int:
        """把旧卷上仍有登记的 blob 补成迁移条目；返回新增/重开的条目数。"""
        now = _iso(_utcnow())
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO migration_items
                   (job_id, sha256, status, size, attempts, updated_at)
            SELECT ?, sha256, ?, size, 0, ?
              FROM blobs WHERE volume = ?
            """,
            (job_id, ITEM_PENDING, now, storage.OLD),
        )
        added = max(0, cur.rowcount)
        # 极少数情况下条目已存在但曾被标记 done，而登记又回到旧卷：重开
        reopened = conn.execute(
            """
            UPDATE migration_items
               SET status = ?, attempts = 0, last_error = NULL, updated_at = ?
             WHERE job_id = ?
               AND status IN (?, ?)
               AND sha256 IN (SELECT sha256 FROM blobs WHERE volume = ?)
            """,
            (ITEM_PENDING, now, job_id, ITEM_DELETED, ITEM_GONE, storage.OLD),
        ).rowcount
        return added + max(0, reopened)

    def _mark_item_copied(
        self, conn: sqlite3.Connection, sha: str, size: int
    ) -> bool:
        """同内容重传已在新卷完整发布时，登记该条目的旧副本待清。

        返回 True 表示需要唤醒迁移线程。只对已经受理迁移的部署生效。
        """
        job = conn.execute(
            "SELECT job_id, status FROM migration_jobs ORDER BY job_id LIMIT 1"
        ).fetchone()
        if job is None:
            return False
        now = _iso(_utcnow())
        conn.execute(
            """
            INSERT INTO migration_items(job_id, sha256, status, size, attempts, updated_at)
            VALUES (?, ?, ?, ?, 0, ?)
            ON CONFLICT(job_id, sha256) DO UPDATE SET
                status = excluded.status, size = excluded.size, updated_at = excluded.updated_at
            """,
            (job["job_id"], sha, ITEM_COPIED, size, now),
        )
        conn.execute(
            "UPDATE migration_jobs SET total_items ="
            " (SELECT COUNT(*) FROM migration_items WHERE job_id = ?)"
            " WHERE job_id = ?",
            (job["job_id"], job["job_id"]),
        )
        if job["status"] in (JOB_COMPLETED, JOB_FAILED):
            conn.execute(
                "UPDATE migration_jobs SET status = ?, completed_at = NULL,"
                " last_error = NULL, updated_at = ? WHERE job_id = ?",
                (JOB_PENDING, now, job["job_id"]),
            )
        return True

    def _spawn_migration_worker(self, job_id: int) -> None:
        with self._migration_threads_lock:
            if self._migration_stop.is_set():
                return
            existing = self._migration_threads.get(job_id)
            if existing is not None and existing.is_alive():
                return
            thread = threading.Thread(
                target=self._migration_worker,
                args=(job_id,),
                name=f"migration-{job_id}",
                daemon=True,
            )
            self._migration_threads[job_id] = thread
            thread.start()

    def _wake_migration_worker(self) -> None:
        conn = self._conn()
        try:
            job = conn.execute(
                "SELECT job_id FROM migration_jobs ORDER BY job_id LIMIT 1"
            ).fetchone()
        finally:
            conn.close()
        if job is not None:
            self._spawn_migration_worker(job["job_id"])

    def _migration_worker(self, job_id: int) -> None:
        base = max(0.05, float(self.cfg.migration_retry_delay_seconds))
        delay = base
        while True:
            try:
                state = self._migration_pass(job_id)
            except Exception as exc:  # 防御：线程不应因意外异常静默退出
                self._set_job_error(job_id, repr(exc))
                return
            if state in (JOB_COMPLETED, JOB_FAILED, "missing"):
                return
            # running：可能只是旧副本仍被已受理的下载持有租约，等它结束。
            # 指数退避到 2 秒，长时间下载期间不做无谓的 20Hz 轮询。
            if self._migration_stop.wait(delay):
                return
            delay = min(2.0, delay * 2)

    def _set_job_error(self, job_id: int, error: str) -> None:
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    conn.execute(
                        "UPDATE migration_jobs SET status = ?, last_error = ?,"
                        " updated_at = ? WHERE job_id = ?",
                        (JOB_FAILED, error, _iso(_utcnow()), job_id),
                    )
            finally:
                conn.close()

    def _migration_pass(self, job_id: int) -> str:
        """推进一轮：同步新条目 -> 逐项复制/删旧 -> 更新任务状态。"""
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    job = conn.execute(
                        "SELECT * FROM migration_jobs WHERE job_id = ?", (job_id,)
                    ).fetchone()
                    if job is None:
                        return "missing"
                    if job["status"] in (JOB_COMPLETED, JOB_FAILED):
                        return job["status"]
                    self._sync_migration_items(conn, job_id)
                    conn.execute(
                        "UPDATE migration_jobs SET status = ?, updated_at = ?,"
                        " total_items = (SELECT COUNT(*) FROM migration_items"
                        "                WHERE job_id = ?)"
                        " WHERE job_id = ?",
                        (JOB_RUNNING, _iso(_utcnow()), job_id, job_id),
                    )
                    items = conn.execute(
                        "SELECT sha256, status, size FROM migration_items"
                        " WHERE job_id = ? AND status IN (?, ?)"
                        " ORDER BY sha256",
                        (job_id, ITEM_PENDING, ITEM_COPIED),
                    ).fetchall()
            finally:
                conn.close()

        for item in items:
            if item["status"] == ITEM_PENDING:
                outcome = self._copy_item(job_id, item["sha256"], int(item["size"]))
                if outcome in ("busy", "retry"):
                    # busy：登记暂不可迁移（已被回收/重传），本轮跳过；
                    # retry：失败计数已记录，等待下一轮或已置 failed。
                    continue
            # 复制完成（含原本就在新卷）后推进到删旧；busy 表示租约未结束，
            # 旧副本保留，任务保持 running 由下一轮重试。
            self._delete_old_item(job_id, item["sha256"])

        return self._refresh_job_state(job_id)

    def _bump_item_attempts(
        self,
        conn: sqlite3.Connection,
        job_id: int,
        sha: str,
        error: str,
        keep_status: Optional[str] = None,
    ) -> str:
        """在调用方已开启的事务里记录一次真实失败；返回 failed/retry。

        租约等待属于 busy，不经过这里，因此不会消耗重试次数。
        """
        now = _iso(_utcnow())
        item = conn.execute(
            "SELECT status, attempts FROM migration_items"
            " WHERE job_id = ? AND sha256 = ?",
            (job_id, sha),
        ).fetchone()
        if item is None:
            return "retry"
        attempts = int(item["attempts"]) + 1
        if attempts >= max(1, self.cfg.migration_max_attempts):
            conn.execute(
                "UPDATE migration_items SET status = ?, attempts = ?,"
                " last_error = ?, updated_at = ?"
                " WHERE job_id = ? AND sha256 = ?",
                (ITEM_FAILED, attempts, error, now, job_id, sha),
            )
            conn.execute(
                "UPDATE migration_jobs SET status = ?, last_error = ?,"
                " updated_at = ? WHERE job_id = ?",
                (JOB_FAILED, f"{sha}: {error}", now, job_id),
            )
            return "failed"
        status = keep_status or item["status"]
        conn.execute(
            "UPDATE migration_items SET status = ?, attempts = ?,"
            " last_error = ?, updated_at = ?"
            " WHERE job_id = ? AND sha256 = ?",
            (status, attempts, error, now, job_id, sha),
        )
        return "retry"

    def _record_item_error(
        self, job_id: int, sha: str, error: str, keep_status: Optional[str] = None
    ) -> str:
        """在独立事务里记录失败（调用时不得持有未结束的写事务）。"""
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    return self._bump_item_attempts(
                        conn, job_id, sha, error, keep_status
                    )
            finally:
                conn.close()

    def _copy_item(self, job_id: int, sha: str, size: int) -> str:
        """把旧卷源文件复制到新卷并切换登记；复制在锁外进行。"""
        # 复制前先看一眼登记：已不在或已在新卷就不必复制
        with metadata_lock:
            conn = self._conn()
            try:
                row = conn.execute(
                    "SELECT volume FROM blobs WHERE sha256 = ?", (sha,)
                ).fetchone()
            finally:
                conn.close()
        if row is None:
            with metadata_lock:
                conn = self._conn()
                try:
                    with transaction(conn):
                        conn.execute(
                            "UPDATE migration_items SET status = ?, attempts = 0,"
                            " last_error = NULL, updated_at = ?"
                            " WHERE job_id = ? AND sha256 = ?",
                            (ITEM_GONE, _iso(_utcnow()), job_id, sha),
                        )
                finally:
                    conn.close()
            return "done"
        if row["volume"] == storage.NEW:
            with metadata_lock:
                conn = self._conn()
                try:
                    with transaction(conn):
                        conn.execute(
                            "UPDATE migration_items SET status = ?, updated_at = ?"
                            " WHERE job_id = ? AND sha256 = ?",
                            (ITEM_COPIED, _iso(_utcnow()), job_id, sha),
                        )
                finally:
                    conn.close()
            return "done"

        staged: Optional[Path] = None
        try:
            # 锁外复制 + 校验；此时新卷正式路径尚不存在
            staged, copied_size = storage.stage_copy(
                sha, self.cfg, source_volume=storage.OLD, target_volume=storage.NEW
            )
        except Exception as exc:
            if staged is not None:
                storage.discard_tmp(staged)
            return self._record_item_error(job_id, sha, repr(exc))

        try:
            with metadata_lock:
                conn = self._conn()
                try:
                    with transaction(conn):
                        # 迟到复查：复制期间可能已被回收/重传
                        row = conn.execute(
                            "SELECT volume FROM blobs WHERE sha256 = ?", (sha,)
                        ).fetchone()
                        if row is None:
                            # 已被回收：丢弃副本，绝不恢复登记
                            storage.discard_tmp(staged)
                            staged = None
                            conn.execute(
                                "UPDATE migration_items SET status = ?, attempts = 0,"
                                " last_error = NULL, updated_at = ?"
                                " WHERE job_id = ? AND sha256 = ?",
                                (ITEM_GONE, _iso(_utcnow()), job_id, sha),
                            )
                            return "done"
                        if row["volume"] == storage.NEW:
                            # 期间同内容重传已发布了新副本
                            storage.discard_tmp(staged)
                            staged = None
                            conn.execute(
                                "UPDATE migration_items SET status = ?, updated_at = ?"
                                " WHERE job_id = ? AND sha256 = ?",
                                (ITEM_COPIED, _iso(_utcnow()), job_id, sha),
                            )
                            return "done"
                        # 完整副本已校验通过，原子发布到新卷，再切换登记
                        storage.publish_blob(staged, sha, self.cfg)
                        staged = None  # 已被移动/丢弃，正式路径已存在
                        conn.execute(
                            "UPDATE blobs SET volume = ? WHERE sha256 = ?",
                            (storage.NEW, sha),
                        )
                        conn.execute(
                            "UPDATE migration_items SET status = ?, size = ?,"
                            " attempts = 0, last_error = NULL, updated_at = ?"
                            " WHERE job_id = ? AND sha256 = ?",
                            (ITEM_COPIED, copied_size, _iso(_utcnow()), job_id, sha),
                        )
                    return "done"
                finally:
                    conn.close()
        except Exception as exc:
            if staged is not None:
                storage.discard_tmp(staged)
            return self._record_item_error(job_id, sha, repr(exc))

    def _delete_old_item(self, job_id: int, sha: str) -> str:
        """在临界区内复查登记与租约，删除旧卷原件后才标记 deleted。"""
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    row = conn.execute(
                        "SELECT volume FROM blobs WHERE sha256 = ?", (sha,)
                    ).fetchone()
                    if row is None:
                        # 登记已被回收：此时该文件不再有任何引用（外键保证），
                        # 顺手删掉旧卷残留；删除失败必须记账，不能谎报完成。
                        if not self._is_referenced(conn, sha):
                            try:
                                storage.remove_blob_file(sha, self.cfg, storage.OLD)
                            except Exception as exc:
                                return self._bump_item_attempts(
                                    conn, job_id, sha, repr(exc)
                                )
                        conn.execute(
                            "UPDATE migration_items SET status = ?, attempts = 0,"
                            " last_error = NULL, updated_at = ?"
                            " WHERE job_id = ? AND sha256 = ?",
                            (ITEM_GONE, _iso(_utcnow()), job_id, sha),
                        )
                        return "done"
                    if row["volume"] != storage.NEW:
                        # 登记又回到旧卷（例如极端重试路径）：重新复制
                        conn.execute(
                            "UPDATE migration_items SET status = ?, updated_at = ?"
                            " WHERE job_id = ? AND sha256 = ?",
                            (ITEM_PENDING, _iso(_utcnow()), job_id, sha),
                        )
                        return "retry"
                    if not storage.blob_exists(sha, self.cfg, storage.NEW):
                        # 新卷副本缺失，不能删旧卷唯一副本，改为重新复制
                        conn.execute(
                            "UPDATE migration_items SET status = ?, updated_at = ?"
                            " WHERE job_id = ? AND sha256 = ?",
                            (ITEM_PENDING, _iso(_utcnow()), job_id, sha),
                        )
                        return "retry"
                    if self._has_lease(conn, sha):
                        # 已受理的下载还在读：保留旧副本，等租约结束再删
                        return "busy"
                    try:
                        removed = storage.remove_blob_file(sha, self.cfg, storage.OLD)
                    except Exception as exc:
                        # 同一事务内记账，避免另开连接造成自锁
                        return self._bump_item_attempts(
                            conn, job_id, sha, repr(exc), keep_status=ITEM_COPIED
                        )
                    conn.execute(
                        "UPDATE migration_items SET status = ?, attempts = 0,"
                        " last_error = NULL, updated_at = ?"
                        " WHERE job_id = ? AND sha256 = ?",
                        (
                            ITEM_DELETED if removed else ITEM_GONE,
                            _iso(_utcnow()),
                            job_id,
                            sha,
                        ),
                    )
                    # 删旧卷副本不改变引用：登记与引用都仍指向新卷完整副本
                    return "done"
            finally:
                conn.close()

    def _sweep_old_volume(self, conn: sqlite3.Connection) -> Optional[str]:
        """完成前的最后一步：清掉旧卷残留 blob 文件。

        只删除「没有登记」或「登记已在新卷且新卷完整副本存在」的旧卷文件；
        若某个登记仍指向旧卷，或新卷副本缺失，则保留旧文件并返回错误，
        绝不让唯一可读副本被清掉。必须在外层 metadata_lock 下调用。
        """
        for sha in storage.list_blob_files(self.cfg, storage.OLD):
            row = conn.execute(
                "SELECT volume FROM blobs WHERE sha256 = ?", (sha,)
            ).fetchone()
            if row is not None:
                if row["volume"] == storage.OLD:
                    return f"{sha}: 旧卷登记尚未迁移"
                if not storage.blob_exists(sha, self.cfg, storage.NEW):
                    return f"{sha}: 新卷完整副本缺失，保留旧卷文件"
            elif self._is_referenced(conn, sha):
                # 无登记却仍被引用（理论上受外键保护不会出现）：绝不删除
                return f"{sha}: 旧卷残留仍被引用，保留"
            if row is not None and self._has_lease(conn, sha):
                # 已受理的下载仍持有旧副本：句柄可读，但仍等租约结束再删，
                # 与逐条删旧行为保持一致；此时不能报完成。
                return f"{sha}: 旧卷残留仍被下载租约持有"
            try:
                storage.remove_blob_file(sha, self.cfg, storage.OLD)
            except Exception as exc:
                return f"{sha}: 旧卷残留删除失败 {exc!r}"
        return None

    def _refresh_job_state(self, job_id: int) -> str:
        """根据条目状态与是否仍有旧卷登记，决定 running/failed/completed。"""
        now = _iso(_utcnow())
        with metadata_lock:
            conn = self._conn()
            try:
                with transaction(conn):
                    self._sync_migration_items(conn, job_id)
                    counts = {
                        row["status"]: row["n"]
                        for row in conn.execute(
                            "SELECT status, COUNT(*) AS n FROM migration_items"
                            " WHERE job_id = ? GROUP BY status",
                            (job_id,),
                        ).fetchall()
                    }
                    old_blobs = conn.execute(
                        "SELECT COUNT(*) AS n FROM blobs WHERE volume = ?",
                        (storage.OLD,),
                    ).fetchone()["n"]
                    conn.execute(
                        "UPDATE migration_jobs SET total_items = ?, updated_at = ?"
                        " WHERE job_id = ?",
                        (sum(counts.values()), now, job_id),
                    )
                    if counts.get(ITEM_FAILED, 0) > 0:
                        conn.execute(
                            "UPDATE migration_jobs SET status = ?, updated_at = ?"
                            " WHERE job_id = ?",
                            (JOB_FAILED, now, job_id),
                        )
                        return JOB_FAILED
                    unfinished = (
                        counts.get(ITEM_PENDING, 0)
                        + counts.get(ITEM_COPIED, 0)
                        + old_blobs
                    )
                    if unfinished == 0:
                        # 旧卷原件必须实际移除：先清残留，失败则继续运行重试
                        leftover_error = self._sweep_old_volume(conn)
                        if leftover_error is not None:
                            conn.execute(
                                "UPDATE migration_jobs SET status = ?, last_error = ?,"
                                " updated_at = ? WHERE job_id = ?",
                                (JOB_RUNNING, leftover_error, now, job_id),
                            )
                            return JOB_RUNNING
                        conn.execute(
                            "UPDATE migration_jobs SET status = ?, completed_at = ?,"
                            " updated_at = ?, last_error = NULL WHERE job_id = ?",
                            (JOB_COMPLETED, now, now, job_id),
                        )
                        return JOB_COMPLETED
                    conn.execute(
                        "UPDATE migration_jobs SET status = ?, updated_at = ?"
                        " WHERE job_id = ?",
                        (JOB_RUNNING, now, job_id),
                    )
                    return JOB_RUNNING
            finally:
                conn.close()
