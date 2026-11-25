"""FastAPI application for the artifact mirror.

The mirror is a read replica: the primary site sends numbered publish/delete
operations, possibly out of order and possibly more than once.  A download
always reflects the highest sequence number received for that path; if the
latest operation is a delete, the path is a 404.

Garbage collection is driven by an operator: it reclaims at most ``limit``
contiguous receipt records after the last saved progress point and stops at
the first gap.  Receipts and effective deletion tombstones are reclaimed
together with the progress update, atomically.

Concurrency model
-----------------
Artifact bodies are streamed from the request socket straight into a staging
directory on the filesystem.  No SQLite lock is taken while bodies are being
received, so a slow or paused large-package upload never blocks a higher
sequence withdrawal (delete), another ingest, or a GC run.  Only once every
body of the batch is fully on disk does a short ``BEGIN IMMEDIATE``
transaction adopt the staged blobs and write the metadata: receipts, package
state and downloadable content become visible together, or not at all.
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, Field, field_validator
from starlette.concurrency import run_in_threadpool

from . import db
from .storage import (
    ContentStore,
    MalformedMultipart,
    PayloadTooLarge,
    StreamingMultipart,
)

logger = logging.getLogger("artifact_mirror")
if not logger.handlers:
    logging.basicConfig(
        level=os.environ.get("MIRROR_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

DB_PATH = os.environ.get("MIRROR_DB_PATH", "data/mirror.db")
BLOB_DIR = os.environ.get(
    "MIRROR_BLOB_DIR", str(Path(DB_PATH).parent / "blobs")
)
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


def _multipart_boundary(content_type: str) -> bytes:
    for piece in content_type.split(";"):
        piece = piece.strip()
        if piece.lower().startswith("boundary="):
            boundary = piece[len("boundary="):].strip().strip('"')
            if boundary:
                return boundary.encode("latin-1")
    raise HTTPException(
        status_code=415,
        detail="expected multipart/form-data with a boundary parameter",
    )


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
    store = ContentStore(BLOB_DIR)
    # Existing databases are migrated in place; existing blob directories are
    # left untouched apart from the two cleanups below.
    await run_in_threadpool(db.init_db, DB_PATH, store)

    # A staged file can only belong to an interrupted process: committed rows
    # reference adopted blobs, never staging paths.
    stale = await run_in_threadpool(store.cleanup_staging)
    if stale:
        logger.info("removed %d staging files left by an earlier run", len(stale))

    # Blobs that no committed row references anymore (e.g. a crash between
    # adoption and commit, or an old revision replaced before restart).
    refs = await run_in_threadpool(db.active_content_refs, DB_PATH)
    orphans = await run_in_threadpool(store.sweep_orphans, refs)
    if orphans:
        logger.info("reclaimed %d unreferenced content blobs", len(orphans))

    app.state.store = store
    yield


app = FastAPI(
    title="Artifact Mirror",
    version="1.1.0",
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

    The request is ``multipart/form-data`` with the ``manifest`` field first
    (a JSON array) followed by one binary field per publish, named by the
    manifest's ``file`` entry::

        manifest=[{"seq": 1, "path": "a/b.tgz", "operation": "publish",
                   "file": "f0"}]
        f0=<artifact bytes>

    The request is parsed in a streaming fashion: each binary field is written
    straight into the staging directory while the bytes arrive.  No SQLite
    lock is held during that phase, so a paused large upload does not block
    deletes, other ingests or GC.  After all bodies are staged, one short
    transaction adopts the blobs and records the receipts, making the whole
    batch (receipts, package state and downloadable bodies) effective
    atomically.  Any staging or commit failure discards the batch completely.
    """

    content_type = request.headers.get("content-type", "")
    if "multipart/form-data" not in content_type:
        raise HTTPException(
            status_code=415,
            detail="expected multipart/form-data with a 'manifest' field",
        )
    boundary = _multipart_boundary(content_type)

    store: ContentStore = request.app.state.store
    staged: list[Any] = []
    # Field name -> manifest index expecting that file.
    expected_files: dict[str, int] = {}
    entries: list[ManifestEntry] = []
    prepared: list[dict[str, Any] | None] = []

    try:
        parser = StreamingMultipart(request.stream(), boundary)
        manifest_seen = False
        while True:
            try:
                part = await parser.next_part()
            except MalformedMultipart as exc:
                raise HTTPException(status_code=422, detail=f"malformed multipart body: {exc}")
            if part is None:
                break

            if part.name == "manifest":
                if manifest_seen:
                    raise HTTPException(status_code=422, detail="duplicate 'manifest' field")
                try:
                    raw_manifest = await part.read_all(
                        4 * 1024 * 1024
                    )
                except PayloadTooLarge:
                    raise HTTPException(status_code=413, detail="manifest field too large")
                entries = _parse_manifest(raw_manifest.decode("utf-8"))
                prepared = [None] * len(entries)
                for index, entry in enumerate(entries):
                    if entry.event.operation == "publish":
                        if not entry.file:
                            raise HTTPException(
                                status_code=422,
                                detail=f"event {entry.event.seq}: publish requires a file field",
                            )
                        if entry.file in expected_files:
                            raise HTTPException(
                                status_code=422,
                                detail=f"file field {entry.file!r} is referenced more than once",
                            )
                        expected_files[entry.file] = index
                manifest_seen = True
                continue

            if not manifest_seen:
                raise HTTPException(
                    status_code=422,
                    detail="the 'manifest' field must precede every file field",
                )
            if part.filename is None:
                # Non-file fields other than manifest are not part of the API.
                raise HTTPException(
                    status_code=422, detail=f"unexpected form field {part.name!r}"
                )
            index = expected_files.pop(part.name, None)
            if index is None:
                raise HTTPException(
                    status_code=422,
                    detail=f"file field {part.name!r} is not referenced by the manifest",
                )
            entry = entries[index]
            try:
                body = await store.stream_part_to_staging(part, MAX_ARTIFACT_BYTES)
            except PayloadTooLarge:
                raise HTTPException(
                    status_code=413,
                    detail=(
                        f"event {entry.event.seq}: artifact exceeds "
                        f"{MAX_ARTIFACT_BYTES} bytes"
                    ),
                )
            if entry.event.media_type:
                body.media_type = entry.event.media_type
            staged.append(body)
            prepared[index] = {
                "seq": entry.event.seq,
                "path": entry.event.path,
                "operation": entry.event.operation,
                "staging_path": body.staging_path,
                "sha256": body.sha256,
                "size": body.size,
                "media_type": body.media_type,
            }

        if not manifest_seen:
            raise HTTPException(status_code=422, detail="missing 'manifest' field")
        if expected_files:
            missing = ", ".join(sorted(expected_files))
            raise HTTPException(status_code=422, detail=f"missing file field(s): {missing}")

        # Delete events need no staging slot; fill them in now, preserving
        # manifest order so the receipt results line up with the request.
        for index, entry in enumerate(entries):
            if prepared[index] is None:
                prepared[index] = {
                    "seq": entry.event.seq,
                    "path": entry.event.path,
                    "operation": entry.event.operation,
                }

        batch_bytes = sum(item.get("size", 0) for item in prepared)
        if batch_bytes > MAX_BATCH_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"batch exceeds {MAX_BATCH_BYTES} bytes in total",
            )

        seen: set[int] = set()
        for item in prepared:
            if item["seq"] in seen:
                raise HTTPException(
                    status_code=422,
                    detail=f"sequence {item['seq']} appears more than once in the batch",
                )
            seen.add(item["seq"])

        # The write phase: short metadata-only transaction.  SQLite blocking
        # calls run in a worker thread; staging cleanup is handled inside.
        results, watermark = await run_in_threadpool(
            db.ingest_events, DB_PATH, prepared, store
        )
        return {"results": results, "watermark": watermark, "acknowledged": True}
    except Exception:
        # Any failure before a successful commit leaves nothing behind: the
        # database is untouched and every staged body is removed.
        for body in staged:
            store.discard(body.staging_path)
        raise


@app.get("/artifacts")
async def list_artifacts(include_deleted: bool = False) -> dict[str, Any]:
    artifacts = await run_in_threadpool(
        db.list_artifacts, DB_PATH, include_deleted
    )
    return {"artifacts": artifacts, "count": len(artifacts)}


@app.get("/artifacts/{path:path}")
async def download(path: str, request: Request):
    try:
        path = _validate_path(path)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    row = await run_in_threadpool(db.get_artifact, DB_PATH, path)
    if row is None:
        raise HTTPException(status_code=404, detail="artifact not found")
    if row["operation"] == "delete":
        raise HTTPException(status_code=404, detail="artifact was deleted")

    headers = {
        "ETag": f'"{row["sha256"]}"',
        "X-Sequence": str(row["latest_seq"]),
        "Content-Disposition": f'inline; filename="{_safe_filename(path)}"',
    }
    store: ContentStore = request.app.state.store
    if row["content_ref"] is not None:
        blob_path = store.blob_path(row["content_ref"])
        if not blob_path.is_file():
            # A referenced blob should always exist; this signals disk damage.
            logger.error("missing blob %s for path %s", row["content_ref"], path)
            raise HTTPException(status_code=503, detail="stored body is unavailable")
        # FileResponse streams from disk instead of loading the body into RAM.
        return FileResponse(
            path=str(blob_path),
            media_type=row["media_type"] or "application/octet-stream",
            headers=headers,
        )
    # Legacy databases whose BLOB migration has not run yet.
    legacy = bytes(row["content"] or b"")
    return Response(
        content=legacy,
        media_type=row["media_type"] or "application/octet-stream",
        headers=headers,
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
    reclaimed and the watermark does not move.  GC touches metadata only, so it
    completes even while large uploads are still streaming into staging.
    """

    return await run_in_threadpool(db.run_gc, DB_PATH, payload.limit)


@app.get("/status")
async def status() -> dict[str, Any]:
    return await run_in_threadpool(db.gc_status, DB_PATH)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
