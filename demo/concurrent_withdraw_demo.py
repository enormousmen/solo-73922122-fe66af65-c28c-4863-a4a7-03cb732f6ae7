#!/usr/bin/env python3
"""End-to-end demo: withdraw a package while its large re-upload is still writing.

Uses only the Python standard library so it runs inside the slim container as
well as on a host with no third-party packages.

Scenario (one global sequence space, mirror starts empty):

    seq 1..3   small prefix events, already committed
    seq 4      *large* re-upload of pkg/big.tgz, streamed slowly with a pause
    seq 5      withdrawal (delete) of pkg/big.tgz with a HIGHER sequence,
               sent on a second connection while seq 4 is mid-upload
    /gc        runs while seq 4 is still paused; reclaims the contiguous
               prefix 1..3 and stops at the gap left by the missing seq 4

Expected outcome: the delete and GC return 200 while the big upload is still
in flight, a fresh download already 404s, and when the upload finally
completes it is acknowledged (receipt stored) but never applied; the package
stays withdrawn.  Its late response still uses the original receipt format
with the watermark at commit time.

Usage:
    python3 demo/concurrent_withdraw_demo.py [base_url]
Default base URL: http://127.0.0.1:8000
"""

from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.client import HTTPConnection

BASE_URL = sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:8000"
HOST_PORT = BASE_URL.replace("http://", "").replace("https://", "")
if "/" in HOST_PORT:
    HOST_PORT = HOST_PORT.split("/", 1)[0]
HOST, _, PORT = HOST_PORT.partition(":")
PORT = int(PORT or 80)

BIG_BODY = b"Z" * (20 * 1024 * 1024)  # 20 MiB, large enough to make the pause obvious


def log(step: str, **fields) -> None:
    print(f"\n== {step} ==")
    for key, value in fields.items():
        print(f"  {key}: {value}")


def post_json(path: str, payload: dict) -> dict:
    request = urllib.request.Request(
        BASE_URL + path,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def get_status_code(path: str) -> int:
    try:
        with urllib.request.urlopen(BASE_URL + path, timeout=30):
            return 200
    except urllib.error.HTTPError as exc:
        return exc.code


def build_multipart(manifest: list[dict], files: dict[str, tuple[str, bytes]],
                    boundary: str) -> bytes:
    out = bytearray()
    out += f"--{boundary}\r\n".encode()
    out += b'Content-Disposition: form-data; name="manifest"\r\n\r\n'
    out += json.dumps(manifest).encode() + b"\r\n"
    for field, (filename, data) in files.items():
        out += f"--{boundary}\r\n".encode()
        out += (
            f'Content-Disposition: form-data; name="{field}"; '
            f'filename="{filename}"\r\n'
        ).encode()
        out += b"Content-Type: application/octet-stream\r\n\r\n"
        out += data + b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return bytes(out)


def post_fast(manifest: list[dict], files: dict[str, tuple[str, bytes]]) -> dict:
    boundary = f"----demo-{uuid.uuid4().hex}"
    body = build_multipart(manifest, files, boundary)
    request = urllib.request.Request(
        BASE_URL + "/events",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def post_slow_with_pause(manifest: list[dict],
                         files: dict[str, tuple[str, bytes]],
                         pause: threading.Event,
                         result: dict) -> None:
    """Stream one big request on a raw socket, halving the body at the pause."""

    boundary = f"----demo-{uuid.uuid4().hex}"
    body = build_multipart(manifest, files, boundary)
    head, tail = body[: len(body) // 2], body[len(body) // 2:]
    try:
        conn = HTTPConnection(HOST, PORT, timeout=30)
        conn.putrequest("POST", "/events")
        conn.putheader("Content-Type", f"multipart/form-data; boundary={boundary}")
        conn.putheader("Content-Length", str(len(body)))
        conn.endheaders()
        conn.send(head)
        result["head_sent_at"] = time.monotonic()
        print("  big upload: first half sent, connection now pauses...")

        # Block until the operator thread has withdrawn + GC'd during the pause.
        if not pause.wait(timeout=30):
            raise RuntimeError("pause was never released")
        conn.send(tail)
        response = conn.getresponse()
        result["status"] = response.status
        result["body"] = json.loads(response.read().decode())
        conn.close()
    except Exception as exc:  # surface the failure to the main thread
        result["error"] = repr(exc)


def main() -> int:
    initial_status = json.loads(
        urllib.request.urlopen(BASE_URL + "/status").read().decode()
    )
    log("0. initial status", **initial_status)

    log("1. ingest the small contiguous prefix seq 1..3")
    print(" ", post_fast(
        [{"seq": 1, "path": "pkg/big.tgz", "operation": "publish", "file": "f0"}],
        {"f0": ("big.tgz", b"package-big-v1")},
    ))
    print(" ", post_fast(
        [{"seq": 2, "path": "pkg/other.tgz", "operation": "publish", "file": "f0"}],
        {"f0": ("other.tgz", b"package-other-v1")},
    ))
    print(" ", post_fast(
        [{"seq": 3, "path": "pkg/other.tgz", "operation": "delete"}], {},
    ))

    log("2. start the large re-upload seq 4 (20 MiB) on a slow connection")
    pause = threading.Event()
    big_result: dict = {}
    uploader = threading.Thread(
        target=post_slow_with_pause,
        args=(
            [{"seq": 4, "path": "pkg/big.tgz", "operation": "publish", "file": "f0"}],
            {"f0": ("big.tgz", BIG_BODY)},
            pause,
            big_result,
        ),
    )
    uploader.start()
    time.sleep(1.5)  # let the first half reach staging
    print("  big upload is paused mid-body; no SQLite write lock is held.")

    log("3. DURING THE PAUSE: higher-sequence withdrawal seq 5 commits")
    delete_response = post_fast(
        [{"seq": 5, "path": "pkg/big.tgz", "operation": "delete"}], {}
    )
    print(" ", delete_response)

    log("4. DURING THE PAUSE: download already reflects the withdrawal")
    code = get_status_code("/artifacts/pkg/big.tgz")
    print(f"  GET /artifacts/pkg/big.tgz -> {code} (404 expected)")

    log("5. DURING THE PAUSE: /gc runs and reclaims the contiguous prefix 1..3")
    gc_response = post_json("/gc", {"limit": 10})
    print(" ", gc_response)

    log("6. release the paused big upload and let seq 4 finish")
    pause.set()
    uploader.join(timeout=30)
    if "error" in big_result:
        print("  big upload failed:", big_result["error"])
        return 1
    print(f"  HTTP {big_result['status']}:", json.dumps(big_result["body"]))

    log("7. verify final state")
    final_code = get_status_code("/artifacts/pkg/big.tgz")
    status = json.loads(urllib.request.urlopen(BASE_URL + "/status").read().decode())
    print(f"  GET /artifacts/pkg/big.tgz -> {final_code} (404 expected, stays withdrawn)")
    print("  /status:", status)

    receipt = big_result["body"]["results"][0]
    checks = {
        "delete committed during pause": delete_response["acknowledged"] is True,
        "download 404 during pause": code == 404,
        "GC cleared [1, 2, 3] at the gap": gc_response["cleared"] == [1, 2, 3],
        "late big upload acknowledged": receipt["status"] == "accepted",
        "late big upload not applied": receipt["applied"] is False,
        "late big upload superseded by seq 5": receipt["superseded_by"] == 5,
        "watermark reported at commit time": big_result["body"]["watermark"] == 3,
        "package stays withdrawn": final_code == 404,
    }
    log("8. checks")
    ok = True
    for name, passed in checks.items():
        print(f"  [{'PASS' if passed else 'FAIL'}] {name}")
        ok = ok and passed
    print("\nRESULT:", "all checks passed" if ok else "FAILURES PRESENT")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
