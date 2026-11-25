"""FastAPI application for the artifact mirror.

The mirror is a read replica: the primary site sends numbered publish/delete
operations, possibly out of order and possibly more than once.  A download
always reflects the highest sequence number received for that path; if the
latest operation is a delete, the path is a 404.

Garbage collection is driven by an operator: it reclaims at most ``limit``
contiguous receipt records after the last saved progress point and stops at
the first gap.  Receipts and effective deletion tombstones are reclaimed
together with the progress update, atomically.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool

from . import db

logger = logging.getLogger("artifact_mirror")

DB_PATH = os.environ.get("MIRROR_DB_PATH", "data/mirror.db")
MAX_BATCH = 500
MAX_PATH_LENGTH = 1024
MAX_LIMIT = 1000
MAX_ARTIFACT_BYTES = int(os.environ.get("MIRROR_MAX_ARTIFACT_BYTES", 64 * 1024 * 1024))
MAX_BATCH_BYTES = int(os.environ.get("MIRROR_MAX_BATCH_BYTES", 256 * 1024 * 1024))


def _validate_path(value: str) -> str:
    if not isinstance(value, str):
        raise ValueError("path must be a string")
    if not value:
        raise ValueError("path must not be empty")
    if value != value.strip():
        raise ValueError("path must not start or end with whitespace")
    if len(value) > MAX_PATH_LENGTH:
        raise ValueError(f"path must be at most {MAX_PATH_LENGTH} characters")
    if value.startswith("/"):
        raise ValueError("path must be relative (no leading '/')")
    if any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in value):
        raise ValueError("path must not contain control characters")
    if any(segment in ("", ".", "..") for segment in value.split("/")):
        raise ValueError("path must not contain empty, '.' or '..' segments")
    return value


def _safe_filename(path: str) -> str:
    name = path.rsplit("/", 1)[-1]
    return "".join("_" if ch in '"\\' or ord(ch) < 0x20 or ord(ch) > 0x7E else ch
                   for ch in name)


class EventIn(BaseModel):
    seq: int = Field(ge=1, le=2**63 - 1)
    path: str
    operation: Literal["publish", "delete"]
    media_type: str | None = Field(default=None, max_length=255)

    @field_validator("path")
    @classmethod
    def _path(cls, value: str) -> str:
        return _validate_path(value)


class ManifestEntry(BaseModel):
    """One manifest element: a validated event plus its multipart file name."""

    event: EventIn
    file: str | None


class GcRequest(BaseModel):
    limit: int = Field(ge=1, le=MAX_LIMIT)


@asynccontextmanager
async def lifespan(_: FastAPI):
    db.init_db(DB_PATH)
    yield


app = FastAPI(
    title="Artifact Mirror",
    version="1.0.0",
    description="Read-only artifact mirror fed by numbered publish/delete events.",
    lifespan=lifespan,
)


@app.exception_handler(sqlite3.Error)
async def _sqlite_error_handler(_: Request, exc: sqlite3.Error) -> JSONResponse:
    logger.exception("database error")
    return JSONResponse(
        status_code=503,
        content={"detail": "database unavailable", "error": exc.__class__.__name__},
    )


@app.exception_handler(Exception)
async def _unexpected_error_handler(request: Request, exc: Exception) -> JSONResponse:
    # Every mutating operation is a single SQLite transaction, so an
    # unexpected failure leaves no partial state behind.
    logger.exception("unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={
            "detail": "internal error; no partial changes were committed",
            "error": exc.__class__.__name__,
        },
    )


def _parse_manifest(raw: str) -> list[ManifestEntry]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=422, detail=f"manifest is not valid JSON: {exc}")

    if isinstance(payload, dict):
        payload = payload.get("events")
    if not isinstance(payload, list):
        raise HTTPException(
            status_code=422, detail="manifest must be a JSON array of events"
        )
    if not payload:
        raise HTTPException(status_code=422, detail="manifest must not be empty")
    if len(payload) > MAX_BATCH:
        raise HTTPException(
            status_code=422, detail=f"at most {MAX_BATCH} events per request"
        )

    entries: list[ManifestEntry] = []
    for item in payload:
        if not isinstance(item, dict):
            raise HTTPException(
                status_code=422, detail="each manifest event must be an object"
            )
        file_name = item.get("file")
        if file_name is not None and not isinstance(file_name, str):
            raise HTTPException(
                status_code=422, detail="manifest 'file' must be a string"
            )
        try:
            event = EventIn.model_validate(item)
        except Exception as exc:  # pydantic.ValidationError
            raise HTTPException(status_code=422, detail=str(exc))
        entries.append(ManifestEntry(event=event, file=file_name))
    return entries


@app.post("/events")
async def ingest_events(request: Request) -> dict[str, Any]:
    """Ingest a batch of numbered operations.

    The request is ``multipart/form-data`` with one field:

    * ``manifest`` -- JSON array of
      ``{"seq": 1, "path": "a/b.tgz", "operation": "publish", "file": "f0"}``.
      ``file`` names another multipart field containing the artifact bytes and
      is required for ``publish``; ``delete`` events carry no file.

    Events may arrive in any order and may be resent verbatim.  Receipts are
    recorded even when superseded; only a newer operation changes the
    download state.  Sequence numbers already reclaimed by GC are acknowledged
    without being applied, which prevents a late resend from resurrecting a
    withdrawn package after a restart.
    """

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(
            status_code=415,
            detail="expected multipart/form-data with a 'manifest' field",
        )

    form = await request.form()
    raw_manifest = form.get("manifest")
    if not isinstance(raw_manifest, str):
        raise HTTPException(status_code=422, detail="missing 'manifest' field")
    entries = _parse_manifest(raw_manifest)

    seen: set[int] = set()
    prepared: list[dict[str, Any]] = []
    batch_bytes = 0
    for entry in entries:
        event = entry.event
        if event.seq in seen:
            raise HTTPException(
                status_code=422,
                detail=f"sequence {event.seq} appears more than once in the batch",
            )
        seen.add(event.seq)

        record: dict[str, Any] = {
            "seq": event.seq,
            "path": event.path,
            "operation": event.operation,
        }
        if event.operation == "publish":
            upload = form.get(entry.file) if entry.file else None
            if upload is None or isinstance(upload, str):
                raise HTTPException(
                    status_code=422,
                    detail=f"event {event.seq}: publish requires a file field",
                )
            # Read with a hard ceiling so a mis-sized upload cannot exhaust
            # memory before we notice it is too large.
            chunks: list[bytes] = []
            total = 0
            while True:
                chunk = await upload.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_ARTIFACT_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            f"event {event.seq}: artifact exceeds "
                            f"{MAX_ARTIFACT_BYTES} bytes"
                        ),
                    )
                chunks.append(chunk)
            batch_bytes += total
            if batch_bytes > MAX_BATCH_BYTES:
                raise HTTPException(
                    status_code=413,
                    detail=f"batch exceeds {MAX_BATCH_BYTES} bytes in total",
                )
            record["content_bytes"] = b"".join(chunks)
            record["media_type"] = (
                event.media_type
                or upload.content_type
                or "application/octet-stream"
            )
        prepared.append(record)

    # SQLite calls are blocking; keep the event loop free while the write
    # transaction runs.
    results, watermark = await run_in_threadpool(db.ingest_events, DB_PATH, prepared)
    return {"results": results, "watermark": watermark, "acknowledged": True}


@app.get("/artifacts")
async def list_artifacts(include_deleted: bool = False) -> dict[str, Any]:
    artifacts = await run_in_threadpool(
        db.list_artifacts, DB_PATH, include_deleted
    )
    return {"artifacts": artifacts, "count": len(artifacts)}


@app.get("/artifacts/{path:path}")
async def download(path: str) -> Response:
    try:
        path = _validate_path(path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    row = await run_in_threadpool(db.get_artifact, DB_PATH, path)
    if row is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if row["operation"] == "delete":
        raise HTTPException(status_code=404, detail="artifact was deleted")
    return Response(
        content=bytes(row["content"] or b""),
        media_type=row["media_type"] or "application/octet-stream",
        headers={
            "ETag": f'"{row["sha256"]}"',
            "X-Sequence": str(row["latest_seq"]),
            "Content-Disposition": f'inline; filename="{_safe_filename(path)}"',
        },
    )


@app.get("/events")
async def list_events(
    after: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
) -> dict[str, Any]:
    events = await run_in_threadpool(db.list_events, DB_PATH, after, limit)
    return {"events": events, "count": len(events)}


@app.post("/gc")
async def run_gc(payload: GcRequest) -> dict[str, Any]:
    """Reclaim up to ``limit`` contiguous receipts and their tombstones.

    Progress resumes from the saved watermark and stops at the first missing
    sequence number.  All changes are in one transaction: on failure nothing is
    reclaimed and the watermark does not move.
    """

    return await run_in_threadpool(db.run_gc, DB_PATH, payload.limit)


@app.get("/status")
async def status() -> dict[str, Any]:
    return await run_in_threadpool(db.gc_status, DB_PATH)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
