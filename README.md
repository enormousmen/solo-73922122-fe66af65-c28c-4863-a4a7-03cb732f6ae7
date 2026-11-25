# Artifact Mirror

A small, read-only artifact mirror fed by numbered `publish`/`delete`
operations from a primary site. Built with FastAPI and SQLite, started with
Docker Compose.

The mirror must satisfy two properties that drive the design:

1. **A withdrawn package never comes back because of a late sync.** Downloads
   always reflect the highest sequence number received for a path. A late
   duplicate of an older operation is acknowledged but cannot change state —
   even after a restart and after the older receipt was cleaned up.
2. **Ingest tolerates reordering and re-sending.** Operations may arrive in any
   order and any number of times; the mirror converges on the same state.

## Quick start

```bash
docker compose up --build -d
curl -s localhost:8000/status | jq .
./demo/demo.sh                          # out-of-order ingest, GC, resend
python3 demo/concurrent_withdraw_demo.py  # withdraw while a big upload pauses
```

Without Docker:

```bash
python3 -m pip install -r requirements.txt
MIRROR_DB_PATH=./data/mirror.db \
MIRROR_BLOB_DIR=./data/blobs \
uvicorn app.main:app --port 8000
```

Tests (no Docker needed):

```bash
python3 -m pip install -r requirements.txt pytest httpx
python3 -m pytest -q
```

## Why bodies are staged on the filesystem

Artifact bytes are **not** written through SQLite. `POST /events` parses the
multipart request in a streaming fashion (a small built-in parser, no
full-request buffering) and writes every body straight into a staging
directory while the bytes arrive. While a large package is still uploading —
including when the client pauses mid-body — the service holds **no SQLite
write lock**. Only after every body of the batch is fully on disk does one
short `BEGIN IMMEDIATE` transaction run: it adopts the staged blobs (atomic
same-filesystem renames into a content-addressed store) and inserts the
metadata rows. That transaction takes milliseconds regardless of body size, so:

- a higher-numbered withdrawal (`delete`) for the same path commits while the
  big re-upload is still in flight, and a new download immediately returns 404;
- `POST /gc` completes during the pause because it only touches metadata;
- receipts, package state and downloadable content become visible **together**,
  or not at all — any staging or commit failure discards the whole batch.

Content layout:

```
$MIRROR_BLOB_DIR/<sha256>          adopted, content-addressed artifact bodies
$MIRROR_BLOB_DIR/.staging/*.part   bodies received but not yet committed
```

On startup the service removes everything under `.staging` (a staged file can
never belong to a committed row — adoption renames it first) and sweeps blobs
that no committed `artifacts` row references. Databases written by older
versions, which kept bodies in an `artifacts.content` BLOB column, are migrated
in place on startup: each BLOB is moved into the blob store and the row is
rewritten with a `content_ref`, so an existing database keeps working.

## Data model

| table       | meaning |
|-------------|---------|
| `receipts`  | one row per sequence number actually received, including ones superseded by a newer operation. Cleared by GC. |
| `artifacts` | latest operation per path. `operation='publish'` serves the blob named by `content_ref`; `operation='delete'` is a tombstone that makes downloads 404. The legacy `content` BLOB column is only retained for migration. |
| `meta`      | `gc_watermark` — every number ≤ the watermark has been reclaimed. |

Sequence numbers form one global space across all paths, starting at 1, sent
by the primary along with the path. The mirror never renumbers and never
fills gaps itself.

## API

### `POST /events` — ingest

`multipart/form-data`; the `manifest` field must come **first**, followed by
one binary field per publish, named by the manifest's `file` entry.

```json
[{"seq": 1, "path": "team/a.tgz", "operation": "publish",  "file": "f0"},
 {"seq": 2, "path": "team/a.tgz", "operation": "delete"}]
```

```bash
curl -X POST localhost:8000/events \
  -F 'manifest=[{"seq":1,"path":"team/a.tgz","operation":"publish","file":"f0"}]' \
  -F 'f0=@a.tgz'
```

Per-event result:

| `status`          | meaning |
|-------------------|---------|
| `accepted`        | new receipt was stored. `applied` says whether it changed the download state; if not, `superseded_by` names the newer sequence in effect. |
| `duplicate`       | that sequence number was already received; acknowledged only. |
| `already_cleared` | the sequence number is at or below the GC watermark. Acknowledged only — never applied, so a late resend cannot resurrect a withdrawn package after a restart. |

A batch is one SQLite transaction: either every receipt in it is stored and
its blobs adopted, or none of it takes effect. Staging files left by a failed
batch are removed immediately; anything surviving a crash is removed on
restart. The response envelope stays unchanged across versions:
`{"results": [...], "watermark": <int>, "acknowledged": true}`, where the
watermark is the GC level observed at commit time.

### `GET /artifacts/{path}` — download

`200` streamed from the blob store (`ETag`, `X-Sequence`), or `404` when the
path was never published or its highest-numbered operation is a delete.

### `GET /artifacts` — catalog

Lists live artifacts (`?include_deleted=true` also shows tombstones). Rows
expose `content_ref` (the blob digest) instead of inline bytes.

### `GET /events` — received operations

Paginated receipt feed (`?after=<seq>&limit=<n>`), useful for auditing.

### `POST /gc` — reclaim receipts

```json
{"limit": 5}
```

Starts after the saved watermark, reclaims at most `limit` **contiguous**
receipts, stops at the first gap, and deletes deletion tombstones whose latest
sequence falls inside the reclaimed range. Returns
`cleared` (the numbers reclaimed), `watermark_before/after`,
`stopped_reason` (`limit_reached` or `gap`) and `tombstones_reclaimed`.

Receipt deletion, tombstone deletion and the watermark update are one
transaction. On any failure the whole run rolls back: no receipt is reclaimed
and progress does not move, so a retry starts from the same point. GC only
touches metadata and therefore never waits on an in-flight body upload.

### `GET /status`, `GET /healthz`

Watermark, next expected number, receipt counts and catalog counts.

## Behaviour walkthroughs

`demo/demo.sh` shows out-of-order ingest, gap-aware GC and idempotent
resends. `demo/concurrent_withdraw_demo.py` (standard library only) shows the
on-call scenario:

```text
seq 1..3 small prefix events committed
seq 4    large (20 MiB) re-upload of pkg/big.tgz starts, then pauses mid-body
seq 5    delete pkg/big.tgz (higher seq) commits on another connection
         -> GET pkg/big.tgz already 404
POST /gc -> clears [1, 2, 3], stops at the gap (seq 4 still missing)
seq 4    upload resumes and finishes
         -> acknowledged receipt, applied=false, superseded_by=5,
            watermark=3 in the response; the package stays 404
```

## Limits and configuration

| variable | default | purpose |
|----------|---------|---------|
| `MIRROR_DB_PATH` | `data/mirror.db` | SQLite file (persisted in the Compose volume) |
| `MIRROR_BLOB_DIR` | `<db dir>/blobs` | content store: adopted blobs and `.staging` |
| `MIRROR_MAX_ARTIFACT_BYTES` | 64 MiB | per-artifact upload cap (413) |
| `MIRROR_MAX_BATCH_BYTES` | 256 MiB | per-request upload cap (413) |
| `MIRROR_LOG_LEVEL` | `INFO` | logging level |

At most 500 events per request, GC `limit` ≤ 1000.

## Known limitations

No authentication; bodies are stored as plain files on one volume (suitable
for a single-instance mirror, not a multi-node deployment); no background
retry — ingest is driven by the primary; multipart parsing is implemented
in-process rather than via `python-multipart` so bodies can be streamed
straight to disk.
