#!/usr/bin/env bash
# End-to-end demonstration of the artifact mirror.
#
#   ./demo/demo.sh                      # against http://127.0.0.1:8000
#   BASE_URL=http://host:8000 ./demo/demo.sh
#
# Start the service first, either with:
#   docker compose up --build -d
# or locally:
#   MIRROR_DB_PATH=./data/mirror.db uvicorn app.main:app --port 8000
#
# The script is read-only with respect to the repository; its working files
# live in demo/work (git-ignored).
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
WORK="$(cd "$(dirname "$0")" && pwd)/work"
rm -rf "$WORK"
mkdir -p "$WORK"

step() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
# hr prints a separator between request and response.
hr() { printf '%s\n' '------------------------------------------------------------'; }

printf 'package-a-v1' > "$WORK/a.tgz"
printf 'package-b-v1' > "$WORK/b.tgz"

# The demo drives the global sequence from 1 and assumes a fresh mirror.
# Sequence numbers reclaimed by an earlier run can only be acknowledged, so a
# second run against the same data volume would show "already_cleared" for
# everything. Fail fast with a clear hint instead of printing confusing output.
existing_watermark=$(curl -sS "$BASE_URL/status" | jq -r '.watermark')
if [ "$existing_watermark" != "0" ]; then
  cat >&2 <<EOF
This mirror already has GC progress (watermark=$existing_watermark).
Run the demo against a fresh instance:
  docker compose down -v && docker compose up --build -d
or, locally, start uvicorn with a new MIRROR_DB_PATH.
EOF
  exit 1
fi

step "0. Service status before any events"
curl -sS "$BASE_URL/status" | jq .

step "1. Out-of-order ingest: delete arrives before the publish it supersedes"
# seq 4 (delete a.tgz) arrives first, then seq 2 and seq 1. seq 3 is missing.
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":4,"path":"pkg/a.tgz","operation":"delete"}]' | jq .
hr
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":2,"path":"pkg/a.tgz","operation":"publish","file":"f0","media_type":"application/gzip"},{"seq":1,"path":"pkg/b.tgz","operation":"publish","file":"f1"}]' \
  -F 'f0=@'"$WORK"'/a.tgz;type=application/gzip' \
  -F 'f1=@'"$WORK"'/b.tgz' | jq .

step "2. Download state follows the highest sequence per path"
for p in pkg/a.tgz pkg/b.tgz; do
  code=$(curl -sS -o "$WORK/out" -w '%{http_code}' "$BASE_URL/artifacts/$p")
  printf 'GET /artifacts/%-12s -> %s' "$p" "$code"
  if [ "$code" = "200" ]; then printf ' body=%s' "$(cat "$WORK/out")"; fi
  printf '\n'
done

step "3. Operator GC, limit=2: stops at the gap after seq 2"
curl -sS -X POST "$BASE_URL/gc" -H 'Content-Type: application/json' -d '{"limit":2}' | jq .

step "4. Fill the gap (seq 3), then GC again: resumes from saved progress"
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":3,"path":"pkg/c.tgz","operation":"publish","file":"f0"}]' \
  -F 'f0=@'"$WORK"'/a.tgz;filename=c.tgz' | jq .
hr
curl -sS -X POST "$BASE_URL/gc" -H 'Content-Type: application/json' -d '{"limit":10}' | jq .
# The effective tombstone for pkg/a.tgz (seq 4) is reclaimed with the receipts.
# The path still 404s because the artifact row is gone; a late resend of the
# reclaimed seq 2 can no longer be applied (step 6).

step "5. Status after GC: receipts and tombstones reclaimed, watermark advanced"
curl -sS "$BASE_URL/status" | jq .

step "6. Resend already-reclaimed sequence numbers: acknowledged, not applied"
# seq 2 is (publish, pkg/a.tgz). It was reclaimed, and seq 4 deleted the path.
# A late resend must not resurrect it.
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":2,"path":"pkg/a.tgz","operation":"publish","file":"f0"}]' \
  -F 'f0=@'"$WORK"'/a.tgz;filename=a.tgz' | jq .
hr
code=$(curl -sS -o /dev/null -w '%{http_code}' "$BASE_URL/artifacts/pkg/a.tgz")
printf 'GET /artifacts/pkg/a.tgz after resend -> %s (still deleted)\n' "$code"

step "7. A live duplicate (seq 5, not yet reclaimed): acknowledged, no state change"
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":5,"path":"pkg/d.tgz","operation":"publish","file":"f0"}]' \
  -F 'f0=@'"$WORK"'/b.tgz;filename=d.tgz' | jq .
hr
curl -sS -X POST "$BASE_URL/events" \
  -F 'manifest=[{"seq":5,"path":"pkg/d.tgz","operation":"publish","file":"f0"}]' \
  -F 'f0=@'"$WORK"'/b.tgz;filename=d.tgz' | jq .

step "8. Final status and catalog"
curl -sS "$BASE_URL/status" | jq .
curl -sS "$BASE_URL/artifacts" | jq .
