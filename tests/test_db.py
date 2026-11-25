"""Concurrency and durability tests against the real SQLite file.

These deliberately bypass the API and call the db layer from several threads
to show that ingest and GC are serialised by SQLite transactions rather than
by the process-global GIL.
"""

import sqlite3
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app import db  # noqa: E402


@pytest.fixture()
def db_path(tmp_path):
    path = tmp_path / "mirror.db"
    db.init_db(path)
    return path


def test_parallel_ingest_keeps_all_receipts(db_path):
    events = [
        {"seq": seq, "path": f"p{seq}.tgz", "operation": "publish",
         "content_bytes": b"data", "media_type": "application/octet-stream"}
        for seq in range(1, 51)
    ]
    errors = []

    def worker(batch):
        try:
            db.ingest_events(db_path, batch)
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
    finally:
        conn.close()
    assert count == 50


def test_concurrent_gc_never_passes_a_gap(db_path):
    db.ingest_events(
        db_path,
        [{"seq": seq, "path": f"p{seq}", "operation": "delete"}
         for seq in (1, 2, 3, 5, 6)],
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


def test_data_survives_reopen(db_path):
    db.ingest_events(
        db_path,
        [{"seq": 1, "path": "a", "operation": "publish",
          "content_bytes": b"persisted", "media_type": "text/plain"}],
    )
    # A new connection is a new "process" view of the same file.
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT * FROM artifacts WHERE path = 'a'").fetchone()
    finally:
        conn.close()
    assert row["operation"] == "publish"
    assert bytes(row["content"]) == b"persisted"
