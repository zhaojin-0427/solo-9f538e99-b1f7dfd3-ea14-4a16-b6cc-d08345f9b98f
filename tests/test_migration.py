"""双数据卷迁移：受理即返回进度号、进度查询、与下载/回收/重传协调、故障恢复。"""
import threading
import time
from pathlib import Path

import httpx

from conftest import make_csv, upload


def blob_files(root: Path):
    d = root / "blobs"
    if not d.exists():
        return []
    return [p for p in d.rglob("*") if p.is_file()]


def wait_migration(client, migration_id: int, timeout: float = 10.0) -> dict:
    deadline = time.time() + timeout
    body = {}
    while time.time() < deadline:
        body = client.get(f"/migrations/{migration_id}").json()
        if body["completed"] or body["status"] == "failed":
            return body
        time.sleep(0.02)
    raise AssertionError(f"迁移未在限定时间内结束: {body}")


def test_migration_returns_progress_id_and_moves_files(
    client, data_dir: Path, new_data_dir: Path
):
    csv_a = make_csv([("S1", 0.1), ("S2", 0.2)])
    csv_b = make_csv([("S3", 0.3)])
    sha_a = upload(client, "day-a", csv_a).json()["sha256"]
    sha_b = upload(client, "day-b", csv_b).json()["sha256"]
    assert len(blob_files(data_dir)) == 2

    r = client.post("/migrations")
    assert r.status_code == 202, r.text
    accepted = r.json()
    assert accepted["accepted"] is True
    assert accepted["reused"] is False
    progress_id = accepted["progress_id"]

    done = wait_migration(client, progress_id)
    assert done["status"] == "completed"
    assert done["completed_at"]
    assert done["deleted"] == 2
    assert done["percent"] == 100.0
    assert done["total"] == 2

    # 旧卷 blob 已实际移除，新卷各一份完整副本；SQLite 仍在旧卷
    assert blob_files(data_dir) == []
    assert sorted(p.name for p in blob_files(new_data_dir)) == sorted([sha_a, sha_b])
    assert (data_dir / "moisture.db").exists()

    # 原件内容与快照号读数保持不变
    assert client.get("/partitions/day-a/download").content == csv_a
    assert client.get("/partitions/day-b/download").content == csv_b


def test_repeat_request_reuses_progress_id(client):
    upload(client, "day", make_csv([("S1", 0.1)]))
    first = client.post("/migrations").json()
    wait_migration(client, first["progress_id"])

    second = client.post("/migrations").json()
    assert second["progress_id"] == first["progress_id"]
    assert second["reused"] is True
    assert second["status"] == "completed"
    assert client.get("/migrations").json()["migrations"][0]["progress_id"] == first[
        "progress_id"
    ]
    # 未知进度号 404
    assert client.get("/migrations/99999").status_code == 404


def test_new_uploads_land_on_new_volume_after_acceptance(
    client, data_dir: Path, new_data_dir: Path
):
    upload(client, "old", make_csv([("S1", 0.1)]))
    mid = client.post("/migrations").json()["progress_id"]
    wait_migration(client, mid)

    # 受理之后上传：只写新卷
    upload(client, "fresh", make_csv([("S2", 0.2)]))
    assert blob_files(data_dir) == []
    assert len(blob_files(new_data_dir)) == 2
    gc_status = client.get("/gc/status").json()
    assert gc_status["old_volume_blob_files"] == 0
    assert gc_status["new_volume_blob_files"] == 2


def test_snapshot_readable_through_migration(client):
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])
    upload(client, "day", csv1)
    snap_id = client.post("/snapshots").json()["snapshot_id"]

    mid = client.post("/migrations").json()["progress_id"]
    wait_migration(client, mid)

    r = client.get(f"/partitions/day/download?snapshot_id={snap_id}")
    assert r.status_code == 200 and r.content == csv1


def test_migration_cooperates_with_gc_of_unreferenced_blob(
    client, data_dir: Path, new_data_dir: Path
):
    old_csv = make_csv([("S1", 0.1)])
    old_sha = upload(client, "day", old_csv).json()["sha256"]
    # 替换掉旧 blob，使其无引用、进入回收队列
    new_sha = upload(client, "day", make_csv([("S1", 0.9)])).json()["sha256"]

    # 迁移与回收同时推进：无论谁先处理，旧 blob 都不得以“仍可读”的形式复活
    mid = client.post("/migrations").json()["progress_id"]
    gc = client.post("/gc").json()
    wait_migration(client, mid)
    client.post("/gc")

    assert old_sha not in [p.name for p in blob_files(new_data_dir)]
    assert old_sha not in [p.name for p in blob_files(data_dir)]
    assert new_sha in [p.name for p in blob_files(new_data_dir)]
    rows = client.get("/partitions/day").json()
    assert rows["current_blob"] == new_sha
    assert client.get("/partitions/day/download").content != old_csv
    assert client.get("/gc/status").json()["queue_remaining"] == 0


def test_late_copy_result_never_resurrects_recycled_registration(
    client, data_dir: Path, new_data_dir: Path, svc, monkeypatch
):
    """复制进行中 blob 被回收：迟到的复制结果必须丢弃，不得恢复登记。"""
    csv_old = make_csv([("S1", 0.1)])
    sha_old = upload(client, "day", csv_old).json()["sha256"]
    # 让旧 blob 变成无引用并进入回收队列
    upload(client, "day", make_csv([("S1", 0.2)]))
    client.post("/gc")
    upload(client, "keep", make_csv([("K", 0.5)]))
    # 重新构造一个待迁移旧 blob：再上传一份不同内容后替换
    sha_stale = upload(client, "stale", make_csv([("T", 0.7)])).json()["sha256"]
    upload(client, "stale", make_csv([("T", 0.8)]))

    from app import storage

    real_stage_copy = storage.stage_copy

    def slow_stage_copy(sha, *args, **kwargs):
        result = real_stage_copy(sha, *args, **kwargs)
        if sha == sha_stale:
            # 复制完成的瞬间，另一个请求把该 blob 回收掉
            client.post("/gc")
        return result

    monkeypatch.setattr(storage, "stage_copy", slow_stage_copy)
    mid = client.post("/migrations").json()["progress_id"]
    done = wait_migration(client, mid)
    monkeypatch.setattr(storage, "stage_copy", real_stage_copy)

    assert done["status"] == "completed"
    # 迟到的复制结果没有被登记，也没有留下可读文件
    assert sha_stale not in [p.name for p in blob_files(new_data_dir)]
    assert sha_stale not in [p.name for p in blob_files(data_dir)]
    assert sha_old not in [p.name for p in blob_files(new_data_dir)]
    # 仍被引用的文件未受影响
    assert client.get("/partitions/keep/download").status_code == 200


def test_copy_failure_is_reported_and_can_resume(
    client, data_dir: Path, new_data_dir: Path, svc, monkeypatch
):
    csv1 = make_csv([("S1", 0.1)])
    upload(client, "day", csv1)
    upload(client, "day2", make_csv([("S2", 0.2)]))

    from app import storage

    def broken_stage_copy(sha, *args, **kwargs):
        raise storage.BlobCopyError("simulated copy failure")

    monkeypatch.setattr(storage, "stage_copy", broken_stage_copy)
    mid = client.post("/migrations").json()["progress_id"]
    failed = wait_migration(client, mid)
    assert failed["status"] == "failed"
    assert failed["last_error"]

    # 失败期间原件仍可读（迁移失败时必须有完整副本可读）
    assert client.get("/partitions/day/download").content == csv1

    # 修复后再次发起：沿用同一进度号，接续完成
    monkeypatch.undo()
    again = client.post("/migrations").json()
    assert again["progress_id"] == mid
    done = wait_migration(client, mid)
    assert done["status"] == "completed"
    assert blob_files(data_dir) == []
    assert client.get("/partitions/day/download").content == csv1


def test_operations_continue_while_copy_inflight(
    live_server, settings, data_dir: Path, new_data_dir: Path, monkeypatch
):
    """旧文件仍在复制时：上传、快照固定/释放、下载、回收都能完成。"""
    http, svc, base = live_server

    from app import storage

    started = threading.Event()
    release = threading.Event()
    real_stage = storage.stage_copy

    def blocking_stage(sha, *args, **kwargs):
        started.set()
        assert release.wait(30), "测试未放行复制"
        return real_stage(sha, *args, **kwargs)

    monkeypatch.setattr(storage, "stage_copy", blocking_stage)

    csv_old = make_csv([("S1", 0.1)])
    upload(http, "day", csv_old)
    mid = http.post("/migrations").json()["progress_id"]
    assert started.wait(10)
    assert http.get(f"/migrations/{mid}").json()["status"] == "running"

    # 复制被卡住期间，所有既有操作照常完成
    csv_new = make_csv([("S2", 0.2)])
    r = upload(http, "fresh", csv_new)
    assert r.status_code == 200 and r.json()["volume"] == "new"
    snap = http.post("/snapshots").json()
    assert snap["partition_count"] == 2
    got = http.get(
        f"/partitions/day/download", params={"snapshot_id": snap["snapshot_id"]}
    )
    assert got.status_code == 200 and got.content == csv_old
    assert http.post(f"/snapshots/{snap['snapshot_id']}/release").status_code == 200
    assert http.get("/gc/status").status_code == 200
    assert http.post("/gc").status_code == 200
    assert http.get("/partitions/fresh/download").content == csv_new

    # 放行复制：迁移完成，旧卷清空，内容仍可读
    release.set()
    deadline = time.time() + 15
    while time.time() < deadline:
        progress = http.get(f"/migrations/{mid}").json()
        if progress["completed"]:
            break
        time.sleep(0.02)
    assert progress["completed"] is True
    assert blob_files(data_dir) == []
    assert http.get("/partitions/day/download").content == csv_old
    assert http.get("/partitions/fresh/download").content == csv_new


def test_same_content_reupload_during_copy_cooperates(
    live_server, data_dir: Path, new_data_dir: Path, monkeypatch
):
    """复制途中同内容重传到新卷：不产生重复文件，旧副本最终仍被清掉。"""
    http, svc, base = live_server

    from app import storage

    started = threading.Event()
    release = threading.Event()
    real_stage = storage.stage_copy
    csv1 = make_csv([("S1", 0.1), ("S2", 0.2)])

    def blocking_stage(sha, *args, **kwargs):
        started.set()
        assert release.wait(30)
        return real_stage(sha, *args, **kwargs)

    sha = upload(http, "day", csv1).json()["sha256"]
    monkeypatch.setattr(storage, "stage_copy", blocking_stage)
    mid = http.post("/migrations").json()["progress_id"]
    assert started.wait(10)

    # 复制被卡住时，同字节上传到另一个分区：直接发布到新卷并切换登记
    again = upload(http, "day-copy", csv1)
    assert again.status_code == 200 and again.json()["sha256"] == sha
    assert again.json()["volume"] == "new"
    # 回收不会删掉仍有引用的原件
    gc = http.post("/gc").json()
    assert sha not in gc["deleted"]
    assert http.get("/partitions/day/download").content == csv1

    release.set()
    deadline = time.time() + 15
    while time.time() < deadline:
        progress = http.get(f"/migrations/{mid}").json()
        if progress["completed"]:
            break
        time.sleep(0.02)
    assert progress["completed"] is True
    # 同内容只有一个新卷文件，旧卷清空
    assert [p.name for p in blob_files(new_data_dir)] == [sha]
    assert blob_files(data_dir) == []
    assert http.get("/partitions/day/download").content == csv1


def test_partial_copy_not_readable_and_failure_keeps_source(
    client, data_dir: Path, new_data_dir: Path, svc, monkeypatch
):
    """复制中途失败：新卷没有正式文件，未复制完整的内容不可读，旧卷仍可读。"""
    csv1 = make_csv([("S1", 0.1)])
    sha = upload(client, "day", csv1).json()["sha256"]

    from app import storage

    real_stage = storage.stage_copy

    def truncated_copy(sha_arg, *args, **kwargs):
        staged, _size = real_stage(sha_arg, *args, **kwargs)
        # 模拟只写了一半：截断暂存文件后按“校验失败”返回
        with open(staged, "r+b") as fh:
            fh.truncate(max(1, len(csv1) // 2))
        raise storage.BlobCopyError("blob 复制校验失败：内容哈希不一致")

    monkeypatch.setattr(storage, "stage_copy", truncated_copy)
    mid = client.post("/migrations").json()["progress_id"]
    failed = wait_migration(client, mid)
    assert failed["status"] == "failed"

    assert blob_files(new_data_dir) == []  # 未复制完整的文件没有正式路径
    assert not storage.blob_path(sha, svc.cfg, "new").exists()
    assert client.get("/partitions/day/download").content == csv1


def test_delete_old_failure_retries_then_completes(
    client, data_dir: Path, new_data_dir: Path, monkeypatch
):
    """删旧卷失败：不能谎报完成；重试成功后旧卷实际移除才 completed。"""
    csv1 = make_csv([("S1", 0.1)])
    upload(client, "day", csv1)

    from app import storage

    real_remove = storage.remove_blob_file
    failed_once = {"done": False}

    def flaky_remove(sha, cfg, volume="old"):
        if volume == "old" and not failed_once["done"]:
            failed_once["done"] = True
            raise OSError("simulated unlink failure")
        return real_remove(sha, cfg, volume)

    monkeypatch.setattr(storage, "remove_blob_file", flaky_remove)
    mid = client.post("/migrations").json()["progress_id"]
    done = wait_migration(client, mid, timeout=15)
    assert failed_once["done"] is True
    assert done["status"] == "completed"
    assert done["deleted"] == 1
    assert blob_files(data_dir) == []
    assert client.get("/partitions/day/download").content == csv1


def test_concurrent_migration_requests_share_one_progress_id(
    client, data_dir: Path, new_data_dir: Path
):
    upload(client, "day", make_csv([("S1", 0.1)]))
    results = []
    barrier = threading.Barrier(6)

    def fire():
        barrier.wait()
        results.append(client.post("/migrations").json()["progress_id"])

    threads = [threading.Thread(target=fire) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=20)
        assert not t.is_alive()
    assert len(set(results)) == 1
    assert len(client.get("/migrations").json()["migrations"]) == 1
    done = wait_migration(client, results[0])
    assert done["status"] == "completed"
    assert len(blob_files(new_data_dir)) == 1
    assert blob_files(data_dir) == []


def test_restart_resumes_incomplete_migration(
    client, settings, data_dir: Path, new_data_dir: Path, svc
):
    csv1 = make_csv([("S1", 0.1)])
    upload(client, "day", csv1)
    mid = client.post("/migrations").json()["progress_id"]
    wait_migration(client, mid)

    # 构造“复制完成但旧副本未删”的中断现场：把登记卷退回 old，
    # 并把新卷完整副本复制回旧卷（真实中断时旧卷原件尚未删除）
    conn = svc._conn()
    try:
        sha = conn.execute("SELECT current_blob FROM partitions WHERE partition_id='day'").fetchone()[0]
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute("UPDATE blobs SET volume='old' WHERE sha256=?", (sha,))
        conn.execute("UPDATE migration_jobs SET status='running', completed_at=NULL")
        conn.execute("UPDATE migration_items SET status='pending' WHERE sha256=?", (sha,))
        conn.commit()
    finally:
        conn.close()

    from app import storage

    old_path = storage.blob_path(sha, settings, "old")
    old_path.parent.mkdir(parents=True, exist_ok=True)
    old_path.write_bytes(storage.blob_path(sha, settings, "new").read_bytes())
    assert old_path.exists() and storage.blob_path(sha, settings, "new").exists()

    from app.service import MoistureService

    restarted = MoistureService(settings)
    restarted.startup()  # 应自动接续未完成迁移并跑完
    deadline = time.time() + 10
    state = None
    while time.time() < deadline:
        conn = restarted._conn()
        try:
            state = conn.execute(
                "SELECT status FROM migration_jobs WHERE job_id=?", (mid,)
            ).fetchone()[0]
        finally:
            conn.close()
        if state == "completed":
            break
        time.sleep(0.02)
    restarted.shutdown()
    assert state == "completed"
    assert blob_files(data_dir) == []
    assert client.get("/partitions/day/download").content == csv1


def test_inflight_download_finishes_while_old_copy_removed(
    live_server, data_dir: Path, new_data_dir: Path
):
    """已受理的慢下载结束前，迁移不删旧副本；删除后快照读数仍完整。"""
    http, svc, base = live_server
    csv_a = make_csv([("S%07d" % i, (i % 1000) / 1000.0) for i in range(600000)])
    assert len(csv_a) > 8 * 1024 * 1024
    sha_a = upload(http, "day", csv_a).json()["sha256"]
    snap = http.post("/snapshots").json()["snapshot_id"]

    errors = []
    state = {}

    def slow_download():
        try:
            with httpx.stream(
                "GET",
                f"{base}/partitions/day/download",
                params={"snapshot_id": snap},
                timeout=120,
            ) as resp:
                assert resp.status_code == 200
                it = resp.iter_bytes(chunk_size=64)
                first = next(it)
                assert first
                # 下载受理后发起迁移：旧副本因租约仍在而被保留
                mid = http.post("/migrations").json()["progress_id"]
                state["mid"] = mid
                time.sleep(0.3)
                progress = http.get(f"/migrations/{mid}").json()
                state["during"] = progress
                assert progress["status"] == "running"
                assert progress["copied"] == 1
                assert progress["deleted"] == 0
                assert (data_dir / "blobs").exists()
                # 新卷已有完整副本，旧卷原件仍在
                assert [p.name for p in blob_files(new_data_dir)] == [sha_a]
                assert [p.name for p in blob_files(data_dir)] == [sha_a]
                rest = b"".join(it)
                state["body"] = first + rest
        except Exception as exc:  # pragma: no cover
            errors.append(exc)

    t = threading.Thread(target=slow_download)
    t.start()
    t.join(timeout=120)
    assert not t.is_alive()
    assert errors == []
    assert state["body"] == csv_a

    # 下载结束后迁移继续：旧副本被移除，任务报完成
    mid = state["mid"]
    deadline = time.time() + 15
    while time.time() < deadline:
        progress = http.get(f"/migrations/{mid}").json()
        if progress["completed"]:
            break
        time.sleep(0.05)
    assert progress["completed"] is True
    assert progress["deleted"] == 1
    assert blob_files(data_dir) == []
    assert [p.name for p in blob_files(new_data_dir)] == [sha_a]
    # 已释放/新读取都从新卷读到同一内容
    assert http.get(f"/partitions/day/download?snapshot_id={snap}").content == csv_a
    http.post(f"/snapshots/{snap}/release")
