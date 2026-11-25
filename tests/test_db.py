"""Concurrency and durability tests against the real SQLite file and blob store.

These deliberately bypass the API and call the storage/db layers from several
threads to show that ingest and GC are serialised by SQLite transactions rather
than than by the process-global GIL, and that slow body staging never blocks
withdrawal or garbage collection.
"""

import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db  # noqa: E402
from app.storage import ContentStore  # noqa: E402


@pytest.fixture()
def store(tmp_path):
    return ContentStore(tmp_path / "blobs")


@pytest.fixture()
def db_path(tmp_path, store):
    path = tmp_path / "mirror.db"
    db.init_db(path, store)
    return path


def stage(store, data: bytes, media_type="application/octet-stream"):
    """Materialise a staged body the same way the web layer does."""

    import hashlib

    digest = hashlib.sha256(data).hexdigest()
    staging = store.staging_dir / f"staging-test-{digest[:12]}.part"
    staging.write_bytes(data)
    return {
        "staging_path": staging,
        "sha256": digest,
        "size": len(data),
        "media_type": media_type,
    }


def make_publish(store, seq, path, data=b"data"):
    return {"seq": seq, "path": path, "operation": "publish", **stage(store, data)}


def test_parallel_ingest_keeps_all_receipts(db_path, store):
    events = [
        make_publish(store, seq, f"p{seq}.tgz", b"data")
        for seq in range(1, 51)
    ]
    errors = []

    def worker(batch):
        try:
            db.ingest_events(db_path, batch, store)
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(exc)

    threads = [
        threading.Thread(target=worker, args=(events[i:i + 5],))
        for i in range(0, len(events), 5)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert not errors
    conn = db.connect(db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM receipts").fetchone()["n"]
        refs = {
            row["content_ref"]
            for row in conn.execute(
                "SELECT content_ref FROM artifacts WHERE content_ref IS NOT NULL"
            )
        }
    finally:
        conn.close()
    assert count == 50
    # Every committed reference resolves to an adopted blob on disk.
    for digest in refs:
        assert store.blob_path(digest).is_file()
    # Nothing was left behind in staging.
    assert list(store.staging_dir.iterdir()) == []


def test_concurrent_gc_never_passes_a_gap(db_path, store):
    db.ingest_events(
        db_path,
        [{"seq": seq, "path": f"p{seq}", "operation": "delete"}
         for seq in (1, 2, 3, 5, 6)],
        store,
    )
    results = []
    lock = threading.Lock()

    def worker():
        result = db.run_gc(db_path, 3)
        with lock:
            results.append(result)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    cleared = sorted(seq for result in results for seq in result["cleared"])
    # Each number is reclaimed exactly once, and seq 4 is never skipped.
    assert cleared == [1, 2, 3]
    status = db.gc_status(db_path)
    assert status["watermark"] == 3
    assert status["next_received_seq"] == 5


def test_data_survives_reopen(db_path, store):
    db.ingest_events(
        db_path,
        [make_publish(store, 1, "a", b"persisted", )],
        store,
    )
    # A new connection is a new "process" view of the same file.
    conn = db.connect(db_path)
    try:
        row = conn.execute("SELECT * FROM artifacts WHERE path = 'a'").fetchone()
    finally:
        conn.close()
    assert row["operation"] == "publish"
    assert row["content"] is None
    assert row["content_ref"] is not None
    assert store.blob_path(row["content_ref"]).read_bytes() == b"persisted"


def test_slow_staging_does_not_block_delete_or_gc(db_path, store):
    """The headline scenario: a big package mid-staging holds no SQLite lock.

    Receipts 1..3 are already in.  A large re-upload (seq 4) is then held in
    staging while the client is "paused".  A higher-numbered withdrawal
    (seq 5) and a GC run must both commit during that pause; GC clears the
    contiguous prefix 1..3 and stops at the gap left by the missing seq 4.
    """

    db.ingest_events(db_path, [make_publish(store, 1, "pkg/big.tgz", b"v1")], store)
    db.ingest_events(db_path, [make_publish(store, 2, "pkg/other.tgz", b"o")], store)
    db.ingest_events(
        db_path,
        [{"seq": 3, "path": "pkg/other.tgz", "operation": "delete"}],
        store,
    )

    big = b"x" * 32 * 1024 * 1024
    staged_publish = make_publish(store, 4, "pkg/big.tgz", big)
    # The body is fully written to staging but no transaction has opened yet:
    # this is exactly the state during a slow/paused upload.
    assert Path(staged_publish["staging_path"]).is_file()

    delete_committed = threading.Event()
    gc_done = threading.Event()
    timings: dict[str, float] = {}
    gc_result: dict = {}

    def withdraw():
        start = time.monotonic()
        db.ingest_events(
            db_path,
            [{"seq": 5, "path": "pkg/big.tgz", "operation": "delete"}],
            store,
        )
        timings["delete"] = time.monotonic() - start
        delete_committed.set()

    def collect():
        delete_committed.wait(5)
        start = time.monotonic()
        gc_result.update(db.run_gc(db_path, 10))
        timings["gc"] = time.monotonic() - start
        gc_done.set()

    t1 = threading.Thread(target=withdraw)
    t2 = threading.Thread(target=collect)
    t1.start()
    t2.start()
    t1.join(5)
    t2.join(5)
    assert not t1.is_alive() and not t2.is_alive()
    assert gc_done.is_set()
    assert timings["delete"] < 2 and timings["gc"] < 2

    # GC completed while the big upload was still paused, clearing the
    # contiguous prefix and stopping at the gap (seq 4 not yet a receipt).
    assert gc_result["cleared"] == [1, 2, 3]
    assert gc_result["watermark_after"] == 3
    assert gc_result["stopped_reason"] == "gap"

    # Withdrawal is effective immediately and survives the late big publish.
    assert db.get_artifact(db_path, "pkg/big.tgz")["operation"] == "delete"

    results, watermark = db.ingest_events(db_path, [staged_publish], store)
    # seq 4 is older than the effective delete (seq 5): receipt stored, not
    # applied, and the path stays withdrawn.
    result = results[0]
    assert result["status"] == "accepted"
    assert result["applied"] is False
    assert result["superseded_by"] == 5
    assert watermark == 3
    assert db.get_artifact(db_path, "pkg/big.tgz")["operation"] == "delete"

    # Its staged body was cleaned up; the blob was never adopted.
    assert not Path(staged_publish["staging_path"]).exists()

    # Now the prefix is contiguous again: GC can reclaim seq 4 and 5 as well.
    second_gc = db.run_gc(db_path, 10)
    assert second_gc["cleared"] == [4, 5]
    assert second_gc["watermark_after"] == 5


def test_ingest_failure_rolls_back_everything_and_cleans_staging(db_path, store, monkeypatch):
    good = make_publish(store, 1, "ok.tgz", b"ok")
    bad = make_publish(store, 2, "bad.tgz", b"bad")

    real_adopt = store.adopt
    calls = {"n": 0}

    def failing_adopt(staging_path, digest):
        calls["n"] += 1
        if digest == bad["sha256"]:
            raise OSError("simulated adoption failure")
        return real_adopt(staging_path, digest)

    monkeypatch.setattr(store, "adopt", failing_adopt)
    with pytest.raises(OSError):
        db.ingest_events(db_path, [good, bad], store)

    status = db.gc_status(db_path)
    assert status["receipt_count"] == 0
    assert status["published_count"] == 0
    # Both staging files are gone.  A blob adopted just before the failure is
    # unreferenced after the rollback and reclaimed by the startup sweep (the
    # documented restart cleanup), never by a committed row.
    assert list(store.staging_dir.iterdir()) == []
    refs = db.active_content_refs(db_path)
    assert refs == set()
    leftover = {p.name for p in store.blob_dir.iterdir() if p.is_file()}
    assert store.sweep_orphans(refs) == sorted(leftover)
    assert not [p for p in store.blob_dir.iterdir() if p.is_file()]


def test_restart_cleans_staging_and_orphan_blobs(db_path, store, tmp_path):
    db.ingest_events(db_path, [make_publish(store, 1, "keep.tgz", b"keep")], store)

    # Simulate a crash mid-ingest: an abandoned staging file and an adopted
    # blob that no committed row references.
    abandoned = store.staging_dir / "staging-abandoned.part"
    abandoned.write_bytes(b"half-written")
    orphan_digest = "deadbeef" * 8
    (store.blob_dir / orphan_digest).write_bytes(b"orphan")

    restarted_store = ContentStore(store.blob_dir)
    removed_staging = restarted_store.cleanup_staging()
    removed_orphans = restarted_store.sweep_orphans(db.active_content_refs(db_path))

    assert "staging-abandoned.part" in removed_staging
    assert orphan_digest in removed_orphans
    assert not abandoned.exists()
    # The referenced blob and its row survive the restart.
    row = db.get_artifact(db_path, "keep.tgz")
    assert restarted_store.blob_path(row["content_ref"]).is_file()


def test_legacy_blob_database_migrates_in_place(tmp_path):
    """A database written by the old BLOB-based version keeps working."""

    old_db = tmp_path / "old" / "mirror.db"
    old_db.parent.mkdir(parents=True)
    import hashlib
    import sqlite3

    legacy_body = b"legacy"
    legacy_digest = hashlib.sha256(legacy_body).hexdigest()
    blob_literal = "X'" + legacy_body.hex() + "'"
    conn = sqlite3.connect(old_db)
    conn.executescript(
        f"""
        CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        INSERT INTO meta VALUES ('gc_watermark', '0');
        CREATE TABLE receipts (
            seq INTEGER PRIMARY KEY, path TEXT NOT NULL,
            operation TEXT NOT NULL, received_at TEXT NOT NULL);
        CREATE TABLE artifacts (
            path TEXT PRIMARY KEY, latest_seq INTEGER NOT NULL,
            operation TEXT NOT NULL, content BLOB, media_type TEXT,
            sha256 TEXT, size INTEGER NOT NULL DEFAULT 0, updated_at TEXT NOT NULL);
        INSERT INTO receipts VALUES (1, 'legacy/a.tgz', 'publish', '2026-01-01T00:00:00+00:00');
        INSERT INTO artifacts VALUES ('legacy/a.tgz', 1, 'publish',
            {blob_literal}, 'application/octet-stream',
            '{legacy_digest}',
            {len(legacy_body)}, '2026-01-01T00:00:00+00:00');
        """
    )
    conn.commit()
    conn.close()

    store = ContentStore(tmp_path / "blobs")
    db.init_db(old_db, store)  # migration runs here

    conn = db.connect(old_db)
    try:
        row = conn.execute(
            "SELECT * FROM artifacts WHERE path = 'legacy/a.tgz'"
        ).fetchone()
    finally:
        conn.close()
    assert row["content"] is None
    assert row["content_ref"] is not None
    assert store.blob_path(row["content_ref"]).read_bytes() == b"legacy"
    # Receipts and watermark are untouched.
    assert db.gc_status(old_db)["receipt_count"] == 1


def test_superseded_publish_unlinks_its_blob(db_path, store):
    db.ingest_events(db_path, [make_publish(store, 1, "p.tgz", b"v1")], store)
    first_ref = db.get_artifact(db_path, "p.tgz")["content_ref"]
    assert store.blob_path(first_ref).is_file()

    db.ingest_events(db_path, [make_publish(store, 2, "p.tgz", b"v2-bytes")], store)
    second_ref = db.get_artifact(db_path, "p.tgz")["content_ref"]
    assert second_ref != first_ref
    assert not store.blob_path(first_ref).exists()
    assert store.blob_path(second_ref).read_bytes() == b"v2-bytes"
