"""End-to-end HTTP tests for the streaming ingest and pause-time withdrawal.

These exercise the real ASGI endpoint (via TestClient) including the hand
rolled streaming multipart parser, and prove that while a large publish is
still streaming into staging, a higher sequence delete and a GC run commit
successfully and downloads immediately return 404.
"""

import asyncio
import json
import sys
import uuid
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("MIRROR_DB_PATH", str(tmp_path / "mirror.db"))
    monkeypatch.setenv("MIRROR_BLOB_DIR", str(tmp_path / "blobs"))
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app import main

    with TestClient(main.app, raise_server_exceptions=False) as test_client:
        yield test_client


def boundary():
    return f"----b-{uuid.uuid4().hex}"


def build_parts(parts, boundary):
    out = bytearray()
    for name, filename, ctype, data in parts:
        out.extend(f"--{boundary}\r\n".encode())
        if filename is None:
            out.extend(f'Content-Disposition: form-data; name="{name}"\r\n\r\n'.encode())
        else:
            out.extend(
                f'Content-Disposition: form-data; name="{name}"; filename="{filename}"\r\n'
                .encode()
            )
            out.extend(f"Content-Type: {ctype or 'application/octet-stream'}\r\n\r\n".encode())
        out.extend(data)
        out.extend(b"\r\n")
    out.extend(f"--{boundary}--\r\n".encode())
    return bytes(out)


def publish_request(seq, path, body, media_type=None):
    manifest = [{"seq": seq, "path": path, "operation": "publish", "file": "f0"}]
    if media_type:
        manifest[0]["media_type"] = media_type
    return [
        ("manifest", None, None, json.dumps(manifest).encode()),
        ("f0", path.rsplit("/", 1)[-1], media_type or "application/octet-stream", body),
    ]


def delete_request(seq, path):
    manifest = [{"seq": seq, "path": path, "operation": "delete"}]
    return [("manifest", None, None, json.dumps(manifest).encode())]


def post_parts(client, parts, b):
    data = build_parts(parts, b)
    return client.post(
        "/events",
        content=data,
        headers={"Content-Type": f"multipart/form-data; boundary={b}"},
    )


def test_streaming_parser_roundtrip_with_multiple_files(client):
    body_a = bytes(range(256)) * 4096  # 1 MiB of binary noise
    body_b = b"second-file\n"
    b = boundary()
    manifest = [
        {"seq": 1, "path": "d/a.bin", "operation": "publish", "file": "f0"},
        {"seq": 2, "path": "d/b.txt", "operation": "publish", "file": "f1",
         "media_type": "text/plain"},
    ]
    parts = [
        ("manifest", None, None, json.dumps(manifest).encode()),
        ("f0", "a.bin", "application/octet-stream", body_a),
        ("f1", "b.txt", "text/plain", body_b),
    ]
    r = post_parts(client, parts, b)
    assert r.status_code == 200, r.text
    results = r.json()["results"]
    assert [item["applied"] for item in results] == [True, True]

    r = client.get("/artifacts/d/a.bin")
    assert r.status_code == 200
    assert r.content == body_a
    assert r.headers["x-sequence"] == "1"
    r = client.get("/artifacts/d/b.txt")
    assert r.status_code == 200
    assert r.content == body_b
    assert r.headers["content-type"] == "text/plain; charset=utf-8"


def test_missing_file_field_is_422(client):
    b = boundary()
    r = post_parts(client, publish_request(1, "a.tgz", b"x")[:1], b)
    assert r.status_code == 422
    assert "missing file" in r.text


def test_unknown_file_field_is_422(client):
    b = boundary()
    parts = publish_request(1, "a.tgz", b"x")
    parts[1] = ("rogue", "a.tgz", None, b"x")
    r = post_parts(client, parts, b)
    assert r.status_code == 422


def test_malformed_multipart_is_422(client):
    r = client.post(
        "/events",
        content=b"this is not multipart at all",
        headers={"Content-Type": "multipart/form-data; boundary=zzz"},
    )
    assert r.status_code == 422


def test_oversized_body_leaves_no_staging_and_no_receipt(client, tmp_path):
    from app import main

    original = main.MAX_ARTIFACT_BYTES
    main.MAX_ARTIFACT_BYTES = 8
    try:
        b = boundary()
        r = post_parts(client, publish_request(1, "big.bin", b"x" * 9), b)
        assert r.status_code == 413
    finally:
        main.MAX_ARTIFACT_BYTES = original
    assert client.get("/status").json()["receipt_count"] == 0
    staging = Path(client.app.state.store.staging_dir)
    assert list(staging.iterdir()) == []


def test_withdrawal_and_gc_commit_while_a_publish_is_paused(client, tmp_path):
    """Drive the actual ASGI endpoint with a deliberately slow request body.

    The big publish (seq 4) is sent in two chunks with an awaitable pause in
    between.  During the pause another concurrent request sends the seq 5
    withdrawal and an operator triggers GC; both must succeed, and a new
    download must already be 404 before the big upload finishes.
    """

    # Prefix receipts 1..3 so GC has contiguous work to do.
    assert post_parts(client, publish_request(1, "pkg/big.tgz", b"v1"), boundary()).status_code == 200
    assert post_parts(client, publish_request(2, "pkg/other.tgz", b"o"), boundary()).status_code == 200
    assert post_parts(client, delete_request(3, "pkg/other.tgz"), boundary()).status_code == 200

    from app import main

    b = boundary()
    big_body = b"Z" * (4 * 1024 * 1024)
    full = build_parts(publish_request(4, "pkg/big.tgz", big_body), b)
    head, tail = full[: len(full) // 2], full[len(full) // 2:]
    observations: dict[str, object] = {}

    async def run():
        transport = httpx.ASGITransport(app=main.app)
        pause = asyncio.Event()

        async def slow_stream():
            yield head
            await asyncio.wait_for(pause.wait(), timeout=10)
            yield tail

        async with httpx.AsyncClient(
            transport=transport, base_url="http://mirror"
        ) as http:
            ingest_task = asyncio.ensure_future(
                http.post(
                    "/events",
                    content=slow_stream(),
                    headers={
                        "Content-Type": f"multipart/form-data; boundary={b}"
                    },
                )
            )
            # Let the first chunk arrive and start staging.
            await asyncio.sleep(0.5)

            delete_boundary = "d"
            delete_resp = await http.post(
                "/events",
                content=build_parts(delete_request(5, "pkg/big.tgz"), delete_boundary),
                headers={
                    "Content-Type": f"multipart/form-data; boundary={delete_boundary}"
                },
            )
            observations["delete_status"] = delete_resp.status_code

            download = await http.get("/artifacts/pkg/big.tgz")
            observations["download_after_delete"] = download.status_code

            gc_resp = await http.post("/gc", json={"limit": 10})
            observations["gc_status"] = gc_resp.status_code
            observations["gc_cleared"] = gc_resp.json()["cleared"]
            observations["watermark"] = gc_resp.json()["watermark_after"]

            pause.set()
            big_resp = await ingest_task
            observations["big_status"] = big_resp.status_code
            observations["big_applied"] = big_resp.json()["results"][0]["applied"]
            observations["big_superseded_by"] = big_resp.json()["results"][0][
                "superseded_by"
            ]

    asyncio.run(run())

    assert observations["delete_status"] == 200
    assert observations["download_after_delete"] == 404
    assert observations["gc_status"] == 200
    assert observations["gc_cleared"] == [1, 2, 3]
    assert observations["watermark"] == 3
    # The late big package is still acknowledged (receipt stored) but not
    # applied because seq 5's withdrawal wins.
    assert observations["big_status"] == 200
    assert observations["big_applied"] is False
    assert observations["big_superseded_by"] == 5
    assert client.get("/artifacts/pkg/big.tgz").status_code == 404


def test_restart_cleans_staging_and_keeps_committed_data(client, tmp_path, monkeypatch):
    assert post_parts(client, publish_request(1, "keep.tgz", b"keep"), boundary()).status_code == 200

    store = client.app.state.store
    abandoned = Path(store.staging_dir) / "staging-leftover.part"
    abandoned.write_bytes(b"partial")
    orphan = Path(store.blob_dir) / ("a" * 64)
    orphan.write_bytes(b"orphan")

    # Simulate a restart against the same directories.
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    from app import main

    with TestClient(main.app) as restarted:
        assert not abandoned.exists()
        assert not orphan.exists()
        r = restarted.get("/artifacts/keep.tgz")
        assert r.status_code == 200
        assert r.content == b"keep"
        assert restarted.get("/status").json()["receipt_count"] == 1


def test_batch_commit_is_atomic(client):
    b = boundary()
    manifest = [
        {"seq": 1, "path": "a.tgz", "operation": "publish", "file": "f0"},
        {"seq": 1, "path": "b.tgz", "operation": "delete"},
    ]
    parts = [
        ("manifest", None, None, json.dumps(manifest).encode()),
        ("f0", "a.tgz", None, b"data"),
    ]
    r = post_parts(client, parts, b)
    assert r.status_code == 422  # duplicate seq within the batch
    assert client.get("/status").json()["receipt_count"] == 0
    assert client.get("/artifacts").json()["count"] == 0
    assert list(Path(client.app.state.store.staging_dir).iterdir()) == []
