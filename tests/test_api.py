import json
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MIRROR_DB_PATH", str(tmp_path / "mirror.db"))
    # The application reads MIRROR_DB_PATH at import time.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app import main

    with TestClient(main.app, raise_server_exceptions=False) as test_client:
        yield test_client


def send(client, events):
    """Send events as multipart form data with manifest + binary fields."""
    manifest = []
    files = {}
    for index, event in enumerate(events):
        item = {k: v for k, v in event.items() if k != "body"}
        if "body" in event:
            field = f"f{index}"
            item["file"] = field
            files[field] = (item["path"].rsplit("/", 1)[-1], event["body"])
        manifest.append(item)
    # Passing "files" (even with only the manifest field) forces a multipart
    # body, which is what the endpoint expects for every batch.
    files["manifest"] = (None, json.dumps(manifest))
    response = client.post("/events", files=files)
    return response


def publish(seq, path, body=b"x"):
    return {"seq": seq, "path": path, "operation": "publish", "body": body}


def delete(seq, path):
    return {"seq": seq, "path": path, "operation": "delete"}


def test_publish_then_download(client):
    r = send(client, [publish(1, "a/one.tgz", b"hello")])
    assert r.status_code == 200, r.text
    assert r.json()["results"][0]["applied"] is True

    r = client.get("/artifacts/a/one.tgz")
    assert r.status_code == 200
    assert r.content == b"hello"
    assert r.headers["x-sequence"] == "1"


def test_latest_operation_wins_regardless_of_arrival_order(client):
    send(client, [delete(5, "pkg/x.tgz"), publish(2, "pkg/x.tgz", b"old")])

    # Highest numbered operation (delete) is in effect.
    assert client.get("/artifacts/pkg/x.tgz").status_code == 404
    events = client.get("/events").json()["events"]
    assert [e["seq"] for e in events] == [2, 5]
    assert {e["operation"] for e in events} == {"publish", "delete"}


def test_late_older_publish_does_not_resurrect(client):
    send(client, [delete(5, "pkg/x.tgz")])
    r = send(client, [publish(2, "pkg/x.tgz", b"old")])
    assert r.json()["results"][0]["applied"] is False
    assert r.json()["results"][0]["superseded_by"] == 5
    assert client.get("/artifacts/pkg/x.tgz").status_code == 404


def test_resend_is_acknowledged_and_idempotent(client):
    first = send(client, [publish(1, "a.tgz", b"v1")]).json()["results"][0]
    second = send(client, [publish(1, "a.tgz", b"v1")]).json()["results"][0]
    assert first["status"] == "accepted"
    assert second["status"] == "duplicate"
    assert second["acknowledged"] is True
    assert client.get("/artifacts/a.tgz").content == b"v1"
    assert client.get("/status").json()["receipt_count"] == 1


def test_gap_stops_gc(client):
    send(client, [publish(1, "a.tgz"), delete(2, "b.tgz"), publish(4, "c.tgz", b"c")])
    result = client.post("/gc", json={"limit": 10}).json()
    assert result["cleared"] == [1, 2]
    assert result["stopped_reason"] == "gap"
    assert result["watermark_after"] == 2

    # Progress resumes from the saved watermark after the gap is filled.
    send(client, [publish(3, "d.tgz")])
    result = client.post("/gc", json={"limit": 10}).json()
    assert result["cleared"] == [3, 4]
    assert result["watermark_after"] == 4


def test_gc_honours_limit_and_resumes(client):
    send(client, [publish(i, f"p{i}.tgz") for i in range(1, 6)])
    first = client.post("/gc", json={"limit": 2}).json()
    assert first["cleared"] == [1, 2]
    assert first["stopped_reason"] == "limit_reached"

    second = client.post("/gc", json={"limit": 2}).json()
    assert second["cleared"] == [3, 4]
    assert second["watermark_after"] == 4


def test_gc_reclaims_effective_tombstones_only(client):
    send(
        client,
        [
            publish(1, "a.tgz"),           # receipt, no tombstone
            delete(2, "gone.tgz"),         # effective tombstone
            delete(3, "revived.tgz"),      # tombstone superseded by publish 4
            publish(4, "revived.tgz", b"back"),
        ],
    )
    result = client.post("/gc", json={"limit": 10}).json()
    assert result["cleared"] == [1, 2, 3, 4]
    assert [t["path"] for t in result["tombstones_reclaimed"]] == ["gone.tgz"]

    # Tombstone is gone, but the publish still serves.
    assert client.get("/artifacts/revived.tgz").content == b"back"
    status = client.get("/status").json()
    assert status["receipt_count"] == 0
    assert status["tombstone_count"] == 0


def test_resend_cleared_seq_only_acknowledged_after_restart(client, tmp_path, monkeypatch):
    send(client, [publish(1, "a.tgz", b"v1"), delete(2, "a.tgz")])
    client.post("/gc", json={"limit": 10})

    # Simulate a restart: rebuild the app against the same SQLite file.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app import main

    with TestClient(main.app) as restarted:
        result = send(restarted, [publish(1, "a.tgz", b"v1")]).json()["results"][0]
        assert result["status"] == "already_cleared"
        assert result["acknowledged"] is True
        assert result["applied"] is False
        # The late resend cannot bring back the deleted package.
        assert restarted.get("/artifacts/a.tgz").status_code == 404


def test_gc_failure_rolls_back_receipts_and_progress(client, monkeypatch):
    send(client, [publish(1, "a.tgz"), publish(2, "b.tgz")])
    from app import db

    def failing_set(conn, value):
        raise RuntimeError("simulated failure during progress update")

    monkeypatch.setattr(db, "_set_watermark", failing_set)
    r = client.post("/gc", json={"limit": 10})
    assert r.status_code == 500
    monkeypatch.undo()

    status = client.get("/status").json()
    assert status["watermark"] == 0
    assert status["receipt_count"] == 2
    assert status["next_expected"] == 1


def test_gc_failure_after_deletes_rolls_back(client, monkeypatch):
    send(client, [publish(1, "a.tgz"), delete(2, "gone.tgz")])
    from app import db

    real_set = db._set_watermark

    calls = {"n": 0}

    def failing_set(conn, value):
        calls["n"] += 1
        raise RuntimeError("boom")

    monkeypatch.setattr(db, "_set_watermark", failing_set)
    r = client.post("/gc", json={"limit": 10})
    assert r.status_code == 500
    monkeypatch.setattr(db, "_set_watermark", real_set)

    status = client.get("/status").json()
    assert status["watermark"] == 0
    assert status["receipt_count"] == 2
    assert status["tombstone_count"] == 1


def test_validate_bad_manifest(client):
    assert client.post("/events", json={"events": []}).status_code == 415
    r = client.post("/events", files={"manifest": (None, "not json")})
    assert r.status_code == 422
    r = client.post(
        "/events",
        files={
            "manifest": (
                None,
                json.dumps([{"seq": 1, "path": "a", "operation": "publish"}]),
            )
        },
    )
    assert r.status_code == 422
    r = client.post(
        "/events",
        files={
            "manifest": (
                None,
                json.dumps([{"seq": 0, "path": "a", "operation": "delete"}]),
            )
        },
    )
    assert r.status_code == 422


def test_duplicate_seq_within_batch_rejected(client):
    r = send(
        client,
        [
            {"seq": 1, "path": "a", "operation": "delete"},
            {"seq": 1, "path": "b", "operation": "delete"},
        ],
    )
    assert r.status_code == 422


def test_filled_gap_allows_late_older_operations_to_be_reclaimed(client):
    send(client, [delete(5, "x.tgz"), publish(2, "x.tgz", b"old"), publish(1, "y.tgz")])
    assert client.get("/artifacts/x.tgz").status_code == 404
    result = client.post("/gc", json={"limit": 10}).json()
    # seq 3 and 4 are missing, so nothing past 2 can be reclaimed.
    assert result["cleared"] == [1, 2]
    assert result["tombstones_reclaimed"] == []
    assert client.get("/artifacts/x.tgz").status_code == 404


def test_gc_can_resume_after_a_failed_attempt(client, monkeypatch):
    send(client, [publish(1, "a.tgz", b"a"), delete(2, "a.tgz")])
    from app import db

    real_set = db._set_watermark
    monkeypatch.setattr(
        db, "_set_watermark", lambda conn, value: (_ for _ in ()).throw(RuntimeError("boom"))
    )
    assert client.post("/gc", json={"limit": 10}).status_code == 500
    monkeypatch.setattr(db, "_set_watermark", real_set)

    # Nothing was consumed by the failed attempt, so the retry starts at 1.
    result = client.post("/gc", json={"limit": 10}).json()
    assert result["cleared"] == [1, 2]
    assert result["watermark_before"] == 0
    assert result["watermark_after"] == 2


def test_gc_limit_validation(client):
    assert client.post("/gc", json={"limit": 0}).status_code == 422
    assert client.post("/gc", json={"limit": -3}).status_code == 422
    assert client.post("/gc", json={"limit": 100000}).status_code == 422
    assert client.post("/gc", json={}).status_code == 422


def test_binary_content_is_returned_verbatim(client):
    body = bytes(range(256)) * 4
    send(client, [publish(1, "blob/data.bin", body)])
    r = client.get("/artifacts/blob/data.bin")
    assert r.status_code == 200
    assert r.content == body
    assert r.headers["etag"].startswith('"')
    assert r.headers["content-disposition"] == 'inline; filename="data.bin"'


def test_deleted_then_republished_by_newer_seq(client):
    send(client, [publish(1, "p.tgz", b"v1"), delete(2, "p.tgz")])
    assert client.get("/artifacts/p.tgz").status_code == 404
    send(client, [publish(3, "p.tgz", b"v3")])
    assert client.get("/artifacts/p.tgz").content == b"v3"
    # A late delete with an older number must not remove it again.
    send(client, [delete(2, "p.tgz")])
    assert client.get("/artifacts/p.tgz").content == b"v3"


def test_path_validation(client):
    r = send(client, [{"seq": 1, "path": "../etc/passwd", "operation": "delete"}])
    assert r.status_code == 422
    r = send(client, [{"seq": 1, "path": "/abs", "operation": "delete"}])
    assert r.status_code == 422
    r = send(client, [{"seq": 1, "path": "a//b", "operation": "delete"}])
    assert r.status_code == 422
    assert client.get("/artifacts/../etc/passwd").status_code in (400, 404)


def test_unknown_path_is_404(client):
    assert client.get("/artifacts/no/such.tgz").status_code == 404


def test_event_feed_reflects_receipts_even_when_superseded(client):
    send(client, [publish(1, "x.tgz", b"1"), publish(3, "x.tgz", b"3"), delete(2, "x.tgz")])
    events = client.get("/events", params={"after": 0, "limit": 10}).json()["events"]
    assert [(e["seq"], e["operation"]) for e in events] == [
        (1, "publish"),
        (2, "delete"),
        (3, "publish"),
    ]
    assert client.get("/artifacts/x.tgz").content == b"3"
    # Pagination after a cursor.
    tail = client.get("/events", params={"after": 1}).json()["events"]
    assert [e["seq"] for e in tail] == [2, 3]


def test_gc_does_not_remove_publish_rows(client):
    send(client, [publish(1, "keep.tgz", b"keep")])
    result = client.post("/gc", json={"limit": 10}).json()
    assert result["cleared"] == [1]
    assert client.get("/artifacts/keep.tgz").content == b"keep"
    catalog = client.get("/artifacts").json()
    assert [a["path"] for a in catalog["artifacts"]] == ["keep.tgz"]


def test_oversized_artifact_rejected(client, monkeypatch):
    from app import main

    monkeypatch.setattr(main, "MAX_ARTIFACT_BYTES", 8)
    r = send(client, [publish(1, "big.bin", b"x" * 9)])
    assert r.status_code == 413
    # A rejected batch leaves no partial receipt behind.
    assert client.get("/status").json()["receipt_count"] == 0


def test_exact_size_limit_accepted(client, monkeypatch):
    from app import main

    monkeypatch.setattr(main, "MAX_ARTIFACT_BYTES", 8)
    r = send(client, [publish(1, "ok.bin", b"x" * 8)])
    assert r.status_code == 200
    assert client.get("/artifacts/ok.bin").content == b"x" * 8


def test_unicode_and_spaces_in_path(client):
    path = "团队/构建 产物 v1.tgz"
    r = send(client, [publish(1, path, b"u")])
    assert r.status_code == 200, r.text
    assert client.get("/artifacts/" + path).content == b"u"



def test_multiple_paths_in_one_batch_are_independent(client):
    send(
        client,
        [
            publish(1, "a.tgz", b"a1"),
            publish(2, "b.tgz", b"b1"),
            delete(3, "a.tgz"),
            publish(4, "a.tgz", b"a2"),
            delete(5, "b.tgz"),
        ],
    )
    assert client.get("/artifacts/a.tgz").content == b"a2"
    assert client.get("/artifacts/b.tgz").status_code == 404
    catalog = client.get("/artifacts").json()
    assert [(a["path"], a["latest_seq"]) for a in catalog["artifacts"]] == [("a.tgz", 4)]
    assert client.get("/status").json()["receipt_count"] == 5
