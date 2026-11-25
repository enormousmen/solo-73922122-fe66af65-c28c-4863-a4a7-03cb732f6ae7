"""Filesystem content store for artifact bodies.

Artifact bytes are streamed into a staging directory *before* any SQLite write
transaction opens.  Ingest therefore holds the SQLite write lock only for the
short metadata commit; a slow or paused body upload never blocks a higher
sequence delete, another ingest, or garbage collection.

Layout::

    <blob_dir>/<sha256>            content-addressed adopted bodies
    <blob_dir>/.staging/<name>     bodies received but not yet part of a commit

Adoption (a same-filesystem rename from staging into the blob directory) is
performed inside the ingest transaction, so a committed artifacts row always
points at a fully adopted blob.  Anything left in ``.staging`` after a restart
never belonged to a committed row and is removed on startup.
"""

from __future__ import annotations

import hashlib
import os
import uuid
from pathlib import Path

from starlette.concurrency import run_in_threadpool

STAGING_DIRNAME = ".staging"
# A manifest is small JSON; refuse anything bigger while streaming it.
MAX_MANIFEST_BYTES = 4 * 1024 * 1024


class StagedBody:
    """A body fully written to staging, not yet referenced by any commit."""

    __slots__ = ("staging_path", "size", "sha256", "media_type")

    def __init__(
        self, staging_path: Path, size: int, sha256: str, media_type: str
    ) -> None:
        self.staging_path = staging_path
        self.size = size
        self.sha256 = sha256
        self.media_type = media_type


class ContentStore:
    def __init__(self, root: str | Path) -> None:
        self.blob_dir = Path(root)
        self.staging_dir = self.blob_dir / STAGING_DIRNAME
        self.blob_dir.mkdir(parents=True, exist_ok=True)
        self.staging_dir.mkdir(parents=True, exist_ok=True)

    # -- staging -----------------------------------------------------------

    def _new_staging_path(self) -> Path:
        return self.staging_dir / f"staging-{uuid.uuid4().hex}.part"

    async def stream_part_to_staging(
        self, part: "MultipartPart", max_bytes: int
    ) -> StagedBody:
        """Stream one multipart file part into staging.

        Runs entirely outside any database transaction.  When the client pauses
        mid-upload the coroutine simply waits on the socket here, holding no
        SQLite lock at all.
        """

        staging_path = self._new_staging_path()
        digest = hashlib.sha256()
        total = 0
        # The blocking open/write/fsync calls go to a worker thread so the
        # event loop (and every other request) keeps making progress.
        def _open() -> object:
            return open(staging_path, "wb")

        fh = await run_in_threadpool(_open)
        try:
            while True:
                chunk = await part.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > max_bytes:
                    raise PayloadTooLarge(max_bytes, total)
                digest.update(chunk)
                await run_in_threadpool(fh.write, chunk)
            await run_in_threadpool(fh.flush)
            await run_in_threadpool(os.fsync, fh.fileno())
        except BaseException:
            # Never leave a partial staging file behind on size violations,
            # client disconnects or disk errors.
            await run_in_threadpool(fh.close)
            await run_in_threadpool(self.discard, staging_path)
            raise
        else:
            await run_in_threadpool(fh.close)

        return StagedBody(
            staging_path=staging_path,
            size=total,
            sha256=digest.hexdigest(),
            media_type=part.content_type or "application/octet-stream",
        )

    def write_blob(self, digest: str, data: bytes) -> str:
        """Write an already-materialised byte string as an adopted blob.

        Used by the one-time migration of databases whose bodies still live in
        the SQLite ``content`` column.  Returns the content reference.
        """

        target = self.blob_path(digest)
        if target.exists():
            return digest
        tmp = self._new_staging_path()
        with open(tmp, "wb") as fh:
            fh.write(data)
            fh.flush()
            os.fsync(fh.fileno())
        os.rename(tmp, target)
        self._fsync_dir(self.blob_dir)
        return digest

    # -- adoption / cleanup ------------------------------------------------

    def adopt(self, staging_path: str | Path, digest: str) -> str:
        """Move a staged body to its content-addressed home.

        A same-filesystem rename is atomic.  The same digest uploaded by two
        concurrent batches resolves to the same path, so adoption is
        idempotent: the winner keeps the file, the loser just drops its copy.
        """

        staging_path = Path(staging_path)
        target = self.blob_path(digest)
        if target.exists():
            staging_path.unlink(missing_ok=True)
            return digest
        os.rename(staging_path, target)
        self._fsync_dir(self.blob_dir)
        return digest

    def discard(self, staging_path: str | Path) -> None:
        """Best-effort removal of a staging file after commit or failure."""

        Path(staging_path).unlink(missing_ok=True)

    def blob_path(self, digest: str) -> Path:
        return self.blob_dir / digest

    def open_blob(self, digest: str):
        return open(self.blob_path(digest), "rb")

    def cleanup_staging(self) -> list[str]:
        """Remove every staging file left behind by a crashed/interrupted run.

        Staging files can only be referenced by in-flight requests, never by
        committed rows (adoption renames them away first), so wiping the
        directory at startup is safe.
        """

        removed: list[str] = []
        if not self.staging_dir.exists():
            return removed
        for entry in self.staging_dir.iterdir():
            if entry.is_file():
                entry.unlink(missing_ok=True)
                removed.append(entry.name)
        return removed

    def sweep_orphans(self, active_refs: set[str]) -> list[str]:
        """Delete blobs that no committed artifacts row references anymore."""

        removed: list[str] = []
        for entry in self.blob_dir.iterdir():
            if not entry.is_file():
                continue
            if entry.name not in active_refs:
                entry.unlink(missing_ok=True)
                removed.append(entry.name)
        return removed

    def delete_if_unreferenced(self, digest: str, active_refs: set[str]) -> bool:
        if digest in active_refs:
            return False
        self.blob_path(digest).unlink(missing_ok=True)
        return True

    @staticmethod
    def _fsync_dir(path: Path) -> None:
        try:
            fd = os.open(path, os.O_RDONLY)
        except OSError:
            return
        try:
            os.fsync(fd)
        except OSError:
            pass
        finally:
            os.close(fd)


class PayloadTooLarge(Exception):
    def __init__(self, limit: int, actual: int) -> None:
        self.limit = limit
        self.actual = actual
        super().__init__(f"artifact of {actual} bytes exceeds limit {limit}")


# ---------------------------------------------------------------------------
# Minimal streaming multipart/form-data parser
# ---------------------------------------------------------------------------


class MalformedMultipart(Exception):
    pass


class MultipartPart:
    """One form part: headers plus a streaming body reader."""

    def __init__(
        self,
        name: str,
        filename: str | None,
        content_type: str | None,
        reader: "StreamingMultipart",
    ) -> None:
        self.name = name
        self.filename = filename
        self.content_type = content_type
        self._reader = reader

    async def read(self, size: int = 1024 * 1024) -> bytes:
        """Return the next body chunk, or b"" at the part boundary."""

        return await self._reader.read_part_body(size)

    async def read_all(self, max_bytes: int) -> bytes:
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = await self.read(1024 * 1024)
            if not chunk:
                break
            total += len(chunk)
            if total > max_bytes:
                raise PayloadTooLarge(max_bytes, total)
            chunks.append(chunk)
        return b"".join(chunks)


class StreamingMultipart:
    """Parse ``multipart/form-data`` straight from the request stream.

    Unlike ``Request.form()`` (which spools the whole request before
    returning), this parser hands body bytes to the caller as they arrive, so
    large artifacts go directly from the socket to staging storage with no
    full-request buffering.  A client that pauses mid-body stalls inside
    :meth:`_fill`, never inside a database transaction.
    """

    def __init__(self, stream, boundary: bytes) -> None:
        self._stream = stream
        self._boundary = boundary
        self._delim = b"--" + boundary
        self._buf = b""
        self._eof = False
        self._finished = False
        # True once a part body reader has consumed the CRLF + boundary line;
        # the buffer then starts right after the delimiter ("--" to close or
        # "\r\n" to open another part).
        self._boundary_consumed = False

    async def _fill(self) -> None:
        if self._eof:
            return
        chunk = await anext(self._stream)  # raises StopAsyncIteration at end
        self._buf += chunk

    async def _fill_until(self, needle: bytes) -> None:
        while needle not in self._buf:
            if self._eof:
                raise MalformedMultipart(f"stream ended before {needle!r}")
            try:
                await self._fill()
            except StopAsyncIteration:
                self._eof = True

    async def next_part(self) -> MultipartPart | None:
        """Advance to the next part, or return None at the closing boundary."""

        if self._finished:
            return None
        if not self._buf and not self._boundary_consumed:
            try:
                await self._fill()
            except StopAsyncIteration:
                self._eof = True
                raise MalformedMultipart("empty multipart body")

        if self._boundary_consumed:
            # Buffer starts immediately after a delimiter already consumed.
            if self._buf.startswith(b"--"):
                self._finished = True
                return None
            if not self._buf.startswith(b"\r\n"):
                raise MalformedMultipart("malformed boundary line")
            self._buf = self._buf[2:]
        else:
            # First call: skip any preamble and consume the first boundary.
            await self._fill_until(self._delim)
            idx = self._buf.find(self._delim)
            self._buf = self._buf[idx + len(self._delim):]
            if self._buf.startswith(b"--"):
                self._finished = True
                return None
            await self._fill_until(b"\r\n")
            if not self._buf.startswith(b"\r\n"):
                raise MalformedMultipart("malformed boundary line")
            self._buf = self._buf[2:]
        self._boundary_consumed = False

        await self._fill_until(b"\r\n\r\n")
        head_end = self._buf.find(b"\r\n\r\n")
        raw_headers = self._buf[:head_end]
        self._buf = self._buf[head_end + 4:]
        headers = self._parse_headers(raw_headers)
        disp = headers.get("content-disposition", "")
        name, filename = self._parse_disposition(disp)
        return MultipartPart(
            name=name,
            filename=filename,
            content_type=headers.get("content-type"),
            reader=self,
        )

    async def read_part_body(self, size: int) -> bytes:
        marker = b"\r\n" + self._delim
        if self._boundary_consumed:
            return b""
        # Keep at least len(marker)-1 tail bytes so a marker split across
        # chunks can never be missed.
        while True:
            pos = self._buf.find(marker)
            if pos >= 0:
                data = self._buf[:pos]
                # Skip body CRLF + boundary line; next_part decides whether
                # another part or the closing marker follows.
                self._buf = self._buf[pos + len(marker):]
                self._boundary_consumed = True
                return data
            safe = len(self._buf) - (len(marker) - 1)
            if safe > 0:
                take = min(size, safe)
                data = self._buf[:take]
                self._buf = self._buf[take:]
                return data
            if self._eof:
                raise MalformedMultipart("stream ended inside a part body")
            try:
                await self._fill()
            except StopAsyncIteration:
                self._eof = True

    @staticmethod
    def _parse_headers(raw: bytes) -> dict[str, str]:
        headers: dict[str, str] = {}
        for line in raw.split(b"\r\n"):
            if b":" not in line:
                raise MalformedMultipart("malformed part header")
            key, value = line.split(b":", 1)
            headers[key.decode("latin-1").strip().lower()] = (
                value.decode("latin-1").strip()
            )
        return headers

    @staticmethod
    def _parse_disposition(disp: str) -> tuple[str, str | None]:
        # Expected: form-data; name="manifest"; filename="a.tgz"
        name: str | None = None
        filename: str | None = None
        for piece in disp.split(";"):
            piece = piece.strip()
            if piece.startswith("name="):
                name = piece[5:].strip('"')
            elif piece.startswith("filename="):
                filename = piece[9:].strip('"')
        if not name:
            raise MalformedMultipart("part without a name")
        return name, filename
