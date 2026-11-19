#!/usr/bin/env bash
# End-to-end check of meili-connect against real containers, through the
# HTTP API only.
#
#   scripts/e2e.sh            # Postgres scenarios, metering and credits, MongoDB, the host policy
#   E2E_SKIP_MONGO=1 scripts/e2e.sh
#
# The main server runs standalone (host policy `any`) with a fake Lab
# (scripts/fake_lab.py: signed usage events, account lookups) and
# METRICS_TOKEN; a second one runs with CONNECT_HOST_POLICY=public to check
# what it refuses.
#
# Needs: docker, curl, jq, python3, cargo. Starts containers named `mc-e2e-<pid>-*`
# on random host ports and removes them (and the server) on exit, whatever
# happens. Each scenario reports PASS or FAIL; the script goes on after a
# failure where it can and exits non-zero if any scenario failed.

set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PREFIX="mc-e2e-$$"
WORK="$(mktemp -d -t mc-e2e.XXXXXX)"
MEILI_IMAGE="${MEILI_IMAGE:-getmeili/meilisearch:v1.54.3}"
PG_IMAGE="${PG_IMAGE:-postgres:17}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8}"
MEILI_KEY="e2e-master-key-0123456789"
ADMIN_KEY="e2e-admin-$$"
ROWS="${E2E_ROWS:-5000}"
TIMEOUT="${E2E_TIMEOUT:-120}"
METRICS_TOKEN="e2e-metrics-$$"
LAB_INSTANCE="e2e-instance"
LAB_SECRET="e2e-lab-secret-$$"
# A Lab account id (a UUID): its syncs are metered and credit-checked.
TEAM="0b9f6c1e-5d1a-4c3e-9a7b-2f4e6d8c0a11"

SERVER_PID=""
PUBLIC_PID=""
LAB_PID=""
RESULTS=()
FAILED=0

log() { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
pass() { RESULTS+=("PASS  $1"); log "PASS: $1"; }
fail() { RESULTS+=("FAIL  $1: $2"); FAILED=1; printf '\033[1;31m[e2e] FAIL: %s: %s\033[0m\n' "$1" "$2"; }

cleanup() {
  for pid in "$SERVER_PID" "$PUBLIC_PID" "$LAB_PID"; do
    if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
      kill -9 "$pid" 2>/dev/null || true
    fi
  done
  docker ps -aq --filter "name=^${PREFIX}-" | xargs -r docker rm -f >/dev/null 2>&1 || true
  if [[ "${E2E_KEEP_LOGS:-}" != "" ]]; then
    log "logs kept in $WORK"
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT INT TERM

for bin in docker curl jq cargo python3; do
  command -v "$bin" >/dev/null || { echo "missing: $bin" >&2; exit 2; }
done

# ------------------------------------------------------------------ helpers

host_port() { docker port "$1" "$2" | head -n1 | sed 's/.*://'; }

wait_until() { # <description> <seconds> <command...>
  local what="$1" secs="$2"; shift 2
  local deadline=$((SECONDS + secs))
  while (( SECONDS < deadline )); do
    if "$@" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  log "timed out waiting for: $what"
  return 1
}

origin_sql() { docker exec -i "$PREFIX-origin" psql -qAt -v ON_ERROR_STOP=1 -U postgres -d app -c "$1"; }

api() { # <method> <path> [json body]
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "http://127.0.0.1:$API_PORT$path" \
      -H "Authorization: Bearer $ADMIN_KEY" -H 'Content-Type: application/json' \
      --data-binary "$body" -w '\n%{http_code}'
  else
    curl -sS -X "$method" "http://127.0.0.1:$API_PORT$path" \
      -H "Authorization: Bearer $ADMIN_KEY" -w '\n%{http_code}'
  fi
}
api_body() { api "$@" | sed '$d'; }
api_code() { api "$@" | tail -n1; }

meili() { curl -sS "http://127.0.0.1:$MEILI_PORT$1" -H "Authorization: Bearer $MEILI_KEY"; }

status_field() { api_body GET "/syncs/$1/status" | jq -r ".$2"; }
phase_is() { [[ "$(status_field "$1" phase)" == "$2" ]]; }

# "id|title" lines of the index, sorted.
index_lines() {
  meili "/indexes/$1/documents?limit=1000000&fields=id,title" \
    | jq -r '.results[] | "\(.id|tostring)|\(.title)"' | LC_ALL=C sort
}
index_count() {
  meili "/indexes/$1/stats" | jq -r '.numberOfDocuments // 0'
}
pg_lines() {
  origin_sql "SELECT id || '|' || title FROM items" | LC_ALL=C sort
}

# Compare origin and index until equal; on timeout, print the difference.
converge() { # <label> <origin lines fn> <index>
  local label="$1" source_fn="$2" index="$3"
  local deadline=$((SECONDS + TIMEOUT))
  while (( SECONDS < deadline )); do
    "$source_fn" >"$WORK/expected"
    index_lines "$index" >"$WORK/actual"
    if cmp -s "$WORK/expected" "$WORK/actual"; then
      return 0
    fi
    sleep 1
  done
  local missing extra
  missing=$(comm -23 "$WORK/expected" "$WORK/actual" | wc -l | tr -d ' ')
  extra=$(comm -13 "$WORK/expected" "$WORK/actual" | wc -l | tr -d ' ')
  log "$label: $missing missing or stale, $extra extra or stale in the index"
  comm -3 "$WORK/expected" "$WORK/actual" | head -n 10 >&2
  return 1
}

start_server() {
  log "starting the server (log: $WORK/server.log)"
  DATABASE_URL="postgres://postgres:state@127.0.0.1:$STATE_PORT/connect" \
  CONNECT_SECRET_KEY="$SECRET_KEY" \
  ADMIN_API_KEY="$ADMIN_KEY" \
  LISTEN_ADDR="127.0.0.1:$API_PORT" \
  METRICS_TOKEN="$METRICS_TOKEN" \
  LAB_URL="http://127.0.0.1:$LAB_PORT" \
  LAB_INSTANCE_ID="$LAB_INSTANCE" \
  LAB_INSTANCE_SECRET="$LAB_SECRET" \
  USAGE_FLUSH_INTERVAL_SECS=2 \
  LAB_SEND_INTERVAL_SECS=1 \
  CREDITS_CHECK_INTERVAL_SECS=2 \
  CREDITS_CACHE_FRESH_SECS=1 \
  CREDITS_CACHE_STALE_SECS=5 \
  RUST_LOG="${E2E_RUST_LOG:-info}" \
    "$BIN" >>"$WORK/server.log" 2>&1 &
  SERVER_PID=$!
  wait_until "server health" 60 curl -sf "http://127.0.0.1:$API_PORT/health"
}

kill_server() {
  kill -9 "$SERVER_PID" 2>/dev/null || true
  wait "$SERVER_PID" 2>/dev/null || true
  SERVER_PID=""
}

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1])'; }

# ------------------------------------------------------------------ setup

log "building the server"
(cd "$ROOT" && cargo build -q -p meili-connect-server) || { echo "build failed" >&2; exit 2; }
BIN="${CARGO_TARGET_DIR:-$ROOT/target}/debug/meili-connect"

log "starting containers ($PREFIX-*)"
docker run -d --name "$PREFIX-meili" -p 127.0.0.1::7700 \
  -e MEILI_MASTER_KEY="$MEILI_KEY" -e MEILI_NO_ANALYTICS=true "$MEILI_IMAGE" >/dev/null
docker run -d --name "$PREFIX-state" -p 127.0.0.1::5432 \
  -e POSTGRES_PASSWORD=state -e POSTGRES_DB=connect "$PG_IMAGE" >/dev/null
docker run -d --name "$PREFIX-origin" -p 127.0.0.1::5432 \
  -e POSTGRES_PASSWORD=origin -e POSTGRES_DB=app "$PG_IMAGE" \
  -c wal_level=logical -c max_replication_slots=10 -c max_wal_senders=10 >/dev/null

MEILI_PORT=$(host_port "$PREFIX-meili" 7700)
STATE_PORT=$(host_port "$PREFIX-state" 5432)
ORIGIN_PORT=$(host_port "$PREFIX-origin" 5432)
API_PORT=$(free_port)
SECRET_KEY=$(head -c 32 /dev/urandom | base64)

wait_until "meilisearch" 60 curl -sf "http://127.0.0.1:$MEILI_PORT/health" || exit 2
wait_until "state postgres" 60 docker exec "$PREFIX-state" pg_isready -U postgres -d connect || exit 2
wait_until "origin postgres" 60 origin_sql "SELECT 1" || exit 2

LAB_PORT=$(free_port)
log "starting the fake Lab on :$LAB_PORT"
python3 "$ROOT/scripts/fake_lab.py" "$LAB_PORT" "$LAB_INSTANCE" "$LAB_SECRET" "$WORK" \
  >>"$WORK/fake_lab.log" 2>&1 &
LAB_PID=$!
disown "$LAB_PID" 2>/dev/null || true
wait_until "fake Lab" 30 curl -s -o /dev/null "http://127.0.0.1:$LAB_PORT/" || exit 2

log "seeding $ROWS rows into the origin"
origin_sql "CREATE TABLE items (id int PRIMARY KEY, title text NOT NULL, price int NOT NULL)"
origin_sql "INSERT INTO items SELECT g, 'item ' || g, g % 100 FROM generate_series(1, $ROWS) g"

start_server || { fail "boot" "the server did not become healthy"; tail -50 "$WORK/server.log"; exit 1; }

ORIGIN_URL="postgres://postgres:origin@127.0.0.1:$ORIGIN_PORT/app?sslmode=disable"
SOURCE=$(jq -nc --arg url "$ORIGIN_URL" '{kind: "postgres", config: {url: $url}}')

# ------------------------------------------------------------------ postgres

scenario_check_discover() {
  local name="postgres: check and discover"
  local check discover
  # The checks cover the mapped tables, so pass the streams.
  check=$(api POST /sources/check "$(jq -nc --argjson s "$SOURCE" \
    '{source: $s, streams: [{collection: "public.items", index: "items"}]}')")
  if [[ "$(tail -n1 <<<"$check")" != 200 ]] || [[ "$(sed '$d' <<<"$check" | jq -r .passed)" != true ]]; then
    fail "$name" "check: $(sed '$d' <<<"$check")"; return
  fi
  discover=$(api_body POST /sources/discover "$(jq -nc --argjson s "$SOURCE" '{source: $s}')")
  if ! jq -e '.collections[] | select(.name == "public.items" and .key == ["id"])' <<<"$discover" >/dev/null; then
    fail "$name" "discover: $discover"; return
  fi
  # Nothing was created on the origin.
  local leftovers
  leftovers=$(origin_sql "SELECT (SELECT count(*) FROM pg_replication_slots) + (SELECT count(*) FROM pg_publication)")
  [[ "$leftovers" == 0 ]] || { fail "$name" "check/discover left $leftovers object(s) on the origin"; return; }
  pass "$name"
}

SYNC_ID=""
scenario_snapshot() {
  local name="postgres: create sync and snapshot $ROWS rows"
  local body resp code
  body=$(jq -nc --argjson s "$SOURCE" --arg mu "http://127.0.0.1:$MEILI_PORT" --arg mk "$MEILI_KEY" \
    '{name: "items", source: $s, destination: {url: $mu, api_key: $mk},
      streams: [{collection: "public.items", index: "items", fields: {include: ["id", "title", "price"]}}]}')
  resp=$(api POST /syncs "$body")
  code=$(tail -n1 <<<"$resp")
  if [[ "$code" != 201 ]]; then fail "$name" "create returned $code: $(sed '$d' <<<"$resp")"; return; fi
  if grep -q "origin@\|$MEILI_KEY" <<<"$resp"; then fail "$name" "the create response leaks a secret"; return; fi
  SYNC_ID=$(sed '$d' <<<"$resp" | jq -r .id)
  log "sync $SYNC_ID created"
  wait_until "phase streaming" "$TIMEOUT" phase_is "$SYNC_ID" streaming \
    || { fail "$name" "phase is $(status_field "$SYNC_ID" phase): $(status_field "$SYNC_ID" message)"; return; }
  if ! converge "snapshot" pg_lines items; then fail "$name" "index != table after the snapshot"; return; fi
  local n; n=$(index_count items)
  [[ "$n" == "$ROWS" ]] || { fail "$name" "index has $n documents, table $ROWS"; return; }
  pass "$name"
}

scenario_stream() {
  local name="postgres: insert, update (incl. primary key change), delete"
  origin_sql "INSERT INTO items SELECT g, 'new ' || g, 1 FROM generate_series($((ROWS+1)), $((ROWS+100))) g"
  origin_sql "UPDATE items SET title = title || ' (edited)' WHERE id BETWEEN 100 AND 149"
  origin_sql "UPDATE items SET id = id + 1000000 WHERE id BETWEEN 1 AND 10"
  origin_sql "DELETE FROM items WHERE id BETWEEN 200 AND 229"
  origin_sql "BEGIN; UPDATE items SET title = 'tx a' WHERE id = 300; DELETE FROM items WHERE id = 301; INSERT INTO items VALUES (301, 'reborn', 1); COMMIT;"
  if converge "stream" pg_lines items; then pass "$name"; else fail "$name" "index did not converge"; fi
}

WRITER_PID=""
writer() { # continuous writes for ~$1 seconds
  local end=$((SECONDS + $1)) i=0
  while (( SECONDS < end )); do
    i=$((i + 1))
    origin_sql "INSERT INTO items SELECT g, 'burst ' || g, 2 FROM generate_series($((2000000 + i * 20)), $((2000000 + i * 20 + 19))) g;
                UPDATE items SET title = 'burst-upd $i' WHERE id = $((1000 + i));
                DELETE FROM items WHERE id = $((2000 + i));" >/dev/null 2>&1 || true
    sleep 0.1
  done
}

scenario_crash() {
  local name="postgres: kill -9 mid-stream, restart, converge"
  writer 12 &
  WRITER_PID=$!
  sleep 4
  log "kill -9 the server mid-stream"
  kill_server
  sleep 2
  start_server || { fail "$name" "the server did not restart"; wait "$WRITER_PID"; return; }
  wait "$WRITER_PID"
  if converge "crash" pg_lines items; then
    pass "$name"
  else
    fail "$name" "missing or extra documents after the restart"
  fi
}

scenario_resync() {
  local name="postgres: /resync keeps the index queryable and converges"
  local code; code=$(api_code POST "/syncs/$SYNC_ID/resync")
  [[ "$code" == 200 ]] || { fail "$name" "resync returned $code"; return; }
  local deadline=$((SECONDS + TIMEOUT)) unqueryable=0 seen_resyncing=0
  while (( SECONDS < deadline )); do
    local hits
    hits=$(curl -sS "http://127.0.0.1:$MEILI_PORT/indexes/items/search" -H "Authorization: Bearer $MEILI_KEY" \
      -H 'Content-Type: application/json' -d '{"q":"item","limit":1}' | jq -r '.estimatedTotalHits // -1')
    (( hits > 0 )) || unqueryable=$((unqueryable + 1))
    [[ "$(status_field "$SYNC_ID" resyncing)" == true ]] && seen_resyncing=1
    if phase_is "$SYNC_ID" streaming && [[ "$(status_field "$SYNC_ID" resyncing)" == false ]] && (( SECONDS > deadline - TIMEOUT + 2 )); then
      break
    fi
    sleep 0.3
  done
  (( unqueryable == 0 )) || { fail "$name" "the index answered no hits $unqueryable times during the resync"; return; }
  if converge "resync" pg_lines items; then
    pass "$name (resyncing observed: $seen_resyncing)"
  else
    fail "$name" "index did not converge after the resync"
  fi
}

scenario_truncate() {
  local name="postgres: TRUNCATE holds the sync, /release empties the index"
  local before; before=$(index_count items)
  origin_sql "TRUNCATE items"
  if ! wait_until "phase held" "$TIMEOUT" phase_is "$SYNC_ID" held; then
    fail "$name" "phase is $(status_field "$SYNC_ID" phase) after TRUNCATE"; return
  fi
  sleep 2
  local held_count; held_count=$(index_count items)
  [[ "$held_count" == "$before" ]] || { fail "$name" "documents deleted while held: $before -> $held_count"; return; }
  log "held: $(status_field "$SYNC_ID" message)"
  local code; code=$(api_code POST "/syncs/$SYNC_ID/release")
  [[ "$code" == 200 ]] || { fail "$name" "release returned $code"; return; }
  if ! wait_until "index empty" "$TIMEOUT" bash -c "[[ \$(curl -sS 'http://127.0.0.1:$MEILI_PORT/indexes/items/stats' -H 'Authorization: Bearer $MEILI_KEY' | jq -r .numberOfDocuments) == 0 ]]"; then
    fail "$name" "index still has $(index_count items) documents after release"; return
  fi
  wait_until "phase streaming" 30 phase_is "$SYNC_ID" streaming || { fail "$name" "phase $(status_field "$SYNC_ID" phase) after release"; return; }
  # It keeps streaming afterwards.
  origin_sql "INSERT INTO items VALUES (1, 'after truncate', 1)"
  if converge "after release" pg_lines items; then pass "$name"; else fail "$name" "no streaming after release"; fi
}

scenario_delete() {
  local name="postgres: DELETE removes the slot and the publication"
  local code; code=$(api_code DELETE "/syncs/$SYNC_ID")
  [[ "$code" == 204 ]] || { fail "$name" "delete returned $code: $(api_body GET "/syncs/$SYNC_ID")"; return; }
  local slots pubs
  slots=$(origin_sql "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'meili_connect%'")
  pubs=$(origin_sql "SELECT count(*) FROM pg_publication WHERE pubname LIKE 'meili_connect%'")
  [[ "$slots" == 0 && "$pubs" == 0 ]] || { fail "$name" "left on the origin: $slots slot(s), $pubs publication(s)"; return; }
  [[ "$(api_code GET "/syncs/$SYNC_ID")" == 404 ]] || { fail "$name" "the sync still exists"; return; }
  pass "$name"
}

# The state database is never a source, whatever the policy: same host as the
# origin (127.0.0.1), the state database's port.
scenario_state_db_refused() {
  local name="host policy any: the state database is refused as an origin"
  local state_src resp code
  state_src=$(jq -nc --arg url "postgres://postgres:state@127.0.0.1:$STATE_PORT/connect?sslmode=disable" \
    '{kind: "postgres", config: {url: $url}}')
  for path in /sources/check /sources/discover; do
    resp=$(api POST "$path" "$(jq -nc --argjson s "$state_src" '{source: $s, streams: [{collection: "public.syncs", index: "x"}]}')")
    code=$(tail -n1 <<<"$resp")
    if [[ "$code" != 422 ]] || [[ "$(sed '$d' <<<"$resp" | jq -r .code)" != host_not_allowed ]]; then
      fail "$name" "$path answered $code: $(sed '$d' <<<"$resp")"; return
    fi
  done
  resp=$(api POST /syncs "$(jq -nc --argjson s "$state_src" --arg mu "http://127.0.0.1:$MEILI_PORT" --arg mk "$MEILI_KEY" \
    '{name: "steal", source: $s, destination: {url: $mu, api_key: $mk}, streams: [{collection: "public.syncs", index: "stolen"}]}')")
  code=$(tail -n1 <<<"$resp")
  [[ "$code" == 422 ]] || { fail "$name" "create answered $code"; return; }
  # The origin on the same host but another port is fine.
  [[ "$(api_code POST /sources/check "$(jq -nc --argjson s "$SOURCE" '{source: $s}')")" == 200 ]] \
    || { fail "$name" "the real origin was refused"; return; }
  pass "$name"
}

scenario_state_db_refused
scenario_check_discover
scenario_snapshot
if [[ -n "$SYNC_ID" ]]; then
  scenario_stream
  scenario_crash
  scenario_resync
  scenario_truncate
  scenario_delete
fi

# ------------------------------------------------------------------ metering, Lab, credits, metrics
#
# A tenant's sync (the admin key acting for the Lab account $TEAM) on its own
# table: its units must be exactly what was synced, the same in /usage, in
# the status and in the signed usage.recorded events the fake Lab received;
# then the fake Lab says the team cannot spend, and the sync is held and
# resumes with the credits.

tapi() { # <method> <path> [json body], as the admin acting for $TEAM
  local method="$1" path="$2" body="${3:-}"
  local args=(-sS -X "$method" "http://127.0.0.1:$API_PORT$path"
    -H "Authorization: Bearer $ADMIN_KEY" -H "X-Connect-Tenant-Id: $TEAM" -w '\n%{http_code}')
  if [[ -n "$body" ]]; then
    args+=(-H 'Content-Type: application/json' --data-binary "$body")
  fi
  curl "${args[@]}"
}
tapi_body() { tapi "$@" | sed '$d'; }
tapi_code() { tapi "$@" | tail -n1; }
tstatus() { tapi_body GET "/syncs/$1/status" | jq -r "$2"; }
tphase_is() { [[ "$(tstatus "$1" .phase)" == "$2" ]]; }
twait() { # <sync id> <jq filter> <expected> <seconds>
  local deadline=$((SECONDS + $4))
  while (( SECONDS < deadline )); do
    [[ "$(tstatus "$1" "$2")" == "$3" ]] && return 0
    sleep 1
  done
  return 1
}

metered_lines() { origin_sql "SELECT id || '|' || title FROM metered" | LC_ALL=C sort; }

# Bytes of the NDJSON lines the sink sends for the matching rows: each
# document as compact JSON with its id as a string and sorted keys (what
# serde_json writes), plus one newline.
line_bytes() {
  origin_sql "SELECT coalesce(json_agg(json_build_object('id', id, 'title', title, 'price', price)), '[]') FROM metered WHERE $1" \
    | python3 -c 'import json, sys
rows = json.load(sys.stdin)
print(sum(len(json.dumps({"id": str(r["id"]), "price": r["price"], "title": r["title"]},
                         separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()) + 1
          for r in rows))'
}
# Bytes of a delete-batch for ids <from>..<to>: each id as a JSON string
# plus one separator.
delete_bytes() { python3 -c "print(sum(len('\"%d\"' % i) + 1 for i in range($1, $2 + 1)))"; }

# Sum of one unit over the Lab events of a sync.
lab_units() { # <sync id> <unit>
  [[ -f "$WORK/lab_events.jsonl" ]] || { echo 0; return; }
  jq -s --arg id "$1" --arg u "$2" \
    '[.[] | select(.data.job_id == $id) | .data.units[$u]] | add // 0' "$WORK/lab_events.jsonl"
}

METERED_ID=""
scenario_metering() {
  local name="metering: /usage, status and Lab events equal what was synced, by kind"
  origin_sql "CREATE TABLE metered (id int PRIMARY KEY, title text NOT NULL, price int NOT NULL)"
  origin_sql "INSERT INTO metered SELECT g, 'metered ' || g, g % 7 FROM generate_series(1, 300) g"
  local snap_bytes; snap_bytes=$(line_bytes true)

  local body resp code
  body=$(jq -nc --argjson s "$SOURCE" --arg mu "http://127.0.0.1:$MEILI_PORT" --arg mk "$MEILI_KEY" \
    '{name: "metered", source: $s, destination: {url: $mu, api_key: $mk},
      streams: [{collection: "public.metered", index: "metered", fields: {include: ["id", "title", "price"]}}]}')
  resp=$(tapi POST /syncs "$body"); code=$(tail -n1 <<<"$resp")
  [[ "$code" == 201 ]] || { fail "$name" "create returned $code: $(sed '$d' <<<"$resp")"; return; }
  METERED_ID=$(sed '$d' <<<"$resp" | jq -r .id)
  wait_until "metered streaming" "$TIMEOUT" tphase_is "$METERED_ID" streaming \
    || { fail "$name" "phase $(tstatus "$METERED_ID" .phase): $(tstatus "$METERED_ID" .message)"; return; }
  converge "metered snapshot" metered_lines metered || { fail "$name" "snapshot did not converge"; return; }
  local got
  got=$(tstatus "$METERED_ID" '[.documents.documents_snapshot, .bytes] | map(tostring) | join("/")')
  [[ "$got" == "300/$snap_bytes" ]] || { fail "$name" "after the first sync: snapshot, bytes = $got, expected 300, $snap_bytes"; return; }

  # 50 inserts, 20 updates, 10 deletes, on distinct rows: 80 streamed.
  origin_sql "INSERT INTO metered SELECT g, 'metered ' || g, 1 FROM generate_series(301, 350) g"
  origin_sql "UPDATE metered SET title = title || ' v2' WHERE id BETWEEN 1 AND 20"
  origin_sql "DELETE FROM metered WHERE id BETWEEN 21 AND 30"
  local stream_bytes
  stream_bytes=$(( $(line_bytes "id BETWEEN 301 AND 350") + $(line_bytes "id BETWEEN 1 AND 20") + $(delete_bytes 21 30) ))
  converge "metered stream" metered_lines metered || { fail "$name" "stream did not converge"; return; }
  twait "$METERED_ID" .documents.documents_streamed 80 30 \
    || { fail "$name" "streamed = $(tstatus "$METERED_ID" .documents.documents_streamed), expected 80"; return; }

  # A resync the user asks for is a snapshot (340 rows now).
  local resync_bytes; resync_bytes=$(line_bytes true)
  [[ "$(tapi_code POST "/syncs/$METERED_ID/resync")" == 200 ]] || { fail "$name" "resync refused"; return; }
  twait "$METERED_ID" '[.documents.documents_snapshot, .resyncing, .phase] | map(tostring) | join("/")' 640/false/streaming "$TIMEOUT" \
    || { fail "$name" "after the resync: $(tapi_body GET "/syncs/$METERED_ID/status" | jq -c '{documents, resyncing, phase}')"; return; }

  # Paused: no more sync seconds accrue, so every unit can be compared.
  [[ "$(tapi_code POST "/syncs/$METERED_ID/pause")" == 200 ]] || { fail "$name" "pause refused"; return; }
  local expected_bytes=$((snap_bytes + stream_bytes + resync_bytes))
  local usage totals
  usage=$(tapi_body GET "/usage")
  totals=$(jq -c .totals <<<"$usage")
  if [[ "$(jq -r '[.documents_snapshot, .documents_streamed, .documents_resync, .bytes] | map(tostring) | join("/")' <<<"$totals")" \
        != "640/80/0/$expected_bytes" ]]; then
    fail "$name" "/usage totals $totals, expected snapshot 640 (300 + a user resync of 340), streamed 80, resync 0, bytes $expected_bytes"; return
  fi
  [[ "$(jq -r .source <<<"$usage")" == state ]] || { fail "$name" "/usage source $(jq -r .source <<<"$usage")"; return; }
  [[ "$(jq -r '.sync_seconds > 0' <<<"$totals")" == true ]] || { fail "$name" "no sync seconds: $totals"; return; }
  [[ "$(tstatus "$METERED_ID" .bytes)" == "$expected_bytes" ]] || { fail "$name" "status bytes $(tstatus "$METERED_ID" .bytes)"; return; }
  local by_sync; by_sync=$(tapi_body GET "/usage?group_by=sync")
  [[ "$(jq -c --arg id "$METERED_ID" '.rows | map(select(.sync_id == $id)) | .[0].units' <<<"$by_sync")" == "$totals" ]] \
    || { fail "$name" "/usage?group_by=sync differs: $by_sync"; return; }
  # The global scope's syncs are its own.
  [[ "$(api_body GET /usage | jq -r .totals.documents_snapshot)" != 640 ]] || { fail "$name" "tenant usage leaked into the global scope"; return; }

  # The signed events the fake Lab accepted add up to the same units.
  local unit want deadline=$((SECONDS + 30)) ok=0
  while (( SECONDS < deadline )); do
    ok=1
    for unit in documents_streamed documents_snapshot documents_resync bytes sync_seconds; do
      want=$(jq -r --arg u "$unit" '.[$u]' <<<"$totals")
      [[ "$(lab_units "$METERED_ID" "$unit")" == "$want" ]] || { ok=0; break; }
    done
    (( ok )) && break
    sleep 1
  done
  if (( ! ok )); then
    fail "$name" "Lab events sum to $(jq -s --arg id "$METERED_ID" '[.[] | select(.data.job_id == $id) | .data.units] | reduce .[] as $u ({}; . as $acc | $u | to_entries | reduce .[] as $e ($acc; .[$e.key] += $e.value))' "$WORK/lab_events.jsonl" 2>/dev/null), /usage says $totals"
    return
  fi
  local shape
  shape=$(jq -s -c --arg id "$METERED_ID" --arg team "$TEAM" \
    '[.[] | select(.data.job_id == $id)] | all(.type == "usage.recorded" and .product == "connect"
      and .account_id == $team and .api_key_id == null and .data.operation == "sync"
      and .data.description == "postgres → metered" and (.data.units | keys | length) == 5)' \
    "$WORK/lab_events.jsonl")
  [[ "$shape" == true ]] || { fail "$name" "an event does not have the contract's shape: $(head -n1 "$WORK/lab_events.jsonl")"; return; }
  # Global syncs are never sent to the Lab.
  [[ "$(jq -s --arg team "$TEAM" '[.[] | select(.account_id != $team)] | length' "$WORK/lab_events.jsonl")" == 0 ]] \
    || { fail "$name" "the Lab received usage of another scope"; return; }
  [[ "$(tapi_code POST "/syncs/$METERED_ID/resume")" == 200 ]] || { fail "$name" "resume refused"; return; }
  pass "$name ($expected_bytes bytes, $(jq -r .sync_seconds <<<"$totals") s, $(wc -l <"$WORK/lab_events.jsonl" | tr -d ' ') Lab events)"
}

scenario_credits() {
  local name="out of credits: the team's syncs are held, then resume with the credits"
  [[ -n "$METERED_ID" ]] || { fail "$name" "no metered sync"; return; }
  wait_until "metered streaming" 30 tphase_is "$METERED_ID" streaming || { fail "$name" "not streaming"; return; }
  echo false >"$WORK/can_spend"
  twait "$METERED_ID" '[.phase, .message, .running] | map(tostring) | join("/")' held/out_of_credits/false 30 \
    || { fail "$name" "status: $(tapi_body GET "/syncs/$METERED_ID/status" | jq -c '{phase, message, running}')"; return; }
  # Nothing was torn down: the slot is still there.
  [[ "$(origin_sql "SELECT count(*) FROM pg_replication_slots WHERE slot_name LIKE 'meili_connect%'")" -ge 1 ]] \
    || { fail "$name" "the replication slot was dropped"; return; }
  local resp; resp=$(tapi POST "/syncs/$METERED_ID/release")
  if [[ "$(tail -n1 <<<"$resp")" != 402 ]] || [[ "$(sed '$d' <<<"$resp" | jq -r .code)" != out_of_credits ]]; then
    fail "$name" "release answered $(tail -n1 <<<"$resp"): $(sed '$d' <<<"$resp")"; return
  fi
  # A new sync may be created; it starts held.
  local body second
  body=$(jq -nc --argjson s "$SOURCE" --arg mu "http://127.0.0.1:$MEILI_PORT" --arg mk "$MEILI_KEY" \
    '{name: "metered2", source: $s, destination: {url: $mu, api_key: $mk},
      streams: [{collection: "public.metered", index: "metered2", fields: {include: ["id", "title", "price"]}}]}')
  resp=$(tapi POST /syncs "$body")
  [[ "$(tail -n1 <<<"$resp")" == 201 ]] || { fail "$name" "create while out of credits: $(sed '$d' <<<"$resp")"; return; }
  second=$(sed '$d' <<<"$resp" | jq -r .id)
  [[ "$(tstatus "$second" '[.phase, .message] | map(tostring) | join("/")')" == "held/out_of_credits" ]] \
    || { fail "$name" "the new sync is $(tstatus "$second" '[.phase, .message] | map(tostring) | join("/")')"; tapi DELETE "/syncs/$second?force=true" >/dev/null; return; }
  # Changes made while held arrive once credits are back.
  origin_sql "INSERT INTO metered VALUES (1000, 'while held', 1)"
  echo true >"$WORK/can_spend"
  local ok=1
  for id in "$METERED_ID" "$second"; do
    wait_until "resumed" 30 tphase_is "$id" streaming || { ok=0; fail "$name" "sync $id is $(tstatus "$id" .phase): $(tstatus "$id" .message)"; }
  done
  if (( ok )); then
    converge "after the credits" metered_lines metered || { ok=0; fail "$name" "the resumed sync did not catch up"; }
    converge "new sync after the credits" metered_lines metered2 || { ok=0; fail "$name" "the new sync did not snapshot"; }
  fi
  for id in "$METERED_ID" "$second"; do
    [[ "$(tapi_code DELETE "/syncs/$id")" == 204 ]] || { ok=0; fail "$name" "delete $id failed"; }
  done
  (( ok )) && pass "$name"
}

scenario_metrics() {
  local name="/metrics: only METRICS_TOKEN, every contract name declared"
  local url="http://127.0.0.1:$API_PORT/metrics" code
  code=$(curl -s -o /dev/null -w '%{http_code}' "$url")
  [[ "$code" == 401 ]] || { fail "$name" "without a token: $code"; return; }
  code=$(curl -s -o /dev/null -w '%{http_code}' "$url" -H "Authorization: Bearer $ADMIN_KEY")
  [[ "$code" == 401 ]] || { fail "$name" "with the admin key: $code"; return; }
  local text
  text=$(curl -sS "$url" -H "Authorization: Bearer $METRICS_TOKEN" -w '\n%{http_code}')
  [[ "$(tail -n1 <<<"$text")" == 200 ]] || { fail "$name" "with the metrics token: $(tail -n1 <<<"$text")"; return; }
  text=$(sed '$d' <<<"$text")
  local m absent=""
  for m in connect_syncs connect_documents_total connect_bytes_total connect_batch_seconds \
           connect_sink_errors_total connect_source_errors_total connect_lag_seconds \
           connect_dead_letters_total connect_outbox_backlog connect_lab_events_sent_total; do
    grep -Eq "^# TYPE $m " <<<"$text" || absent="$absent $m"
  done
  [[ -z "$absent" ]] || { fail "$name" "not declared:$absent"; return; }
  grep -Eq '^connect_documents_total\{kind="snapshot"\} [1-9]' <<<"$text" || { fail "$name" "no snapshot documents counted"; return; }
  grep -Eq '^connect_lab_events_sent_total\{result="accepted"\} [1-9]' <<<"$text" || { fail "$name" "no accepted Lab event counted"; return; }
  grep -q "$TEAM" <<<"$text" && { fail "$name" "a tenant id in the metrics"; return; }
  pass "$name"
}

scenario_metering
scenario_credits
scenario_metrics

# ------------------------------------------------------------------ mongodb

mongo_eval() { docker exec -i "$PREFIX-mongo" mongosh --quiet --eval "$1"; }
mongo_lines() {
  mongo_eval 'db.getSiblingDB("shop").items.find({}, {title: 1}).forEach(d => print(d._id + "|" + d.title))' | LC_ALL=C sort
}

scenario_mongo() {
  local name="mongodb: snapshot, stream, kill -9 and restart"
  docker run -d --name "$PREFIX-mongo" -p 127.0.0.1::27017 "$MONGO_IMAGE" --replSet rs0 --bind_ip_all >/dev/null
  local port; port=$(host_port "$PREFIX-mongo" 27017)
  wait_until "mongod" 60 mongo_eval 'db.runCommand({ping: 1})' || { fail "$name" "mongod did not start"; return; }
  mongo_eval 'rs.initiate({_id: "rs0", members: [{_id: 0, host: "localhost:27017"}]})' >/dev/null
  wait_until "primary" 60 bash -c "docker exec $PREFIX-mongo mongosh --quiet --eval 'db.hello().isWritablePrimary' | grep -q true" \
    || { fail "$name" "no primary"; return; }
  mongo_eval 'const c = db.getSiblingDB("shop").items; const docs = []; for (let i = 1; i <= 2000; i++) docs.push({_id: i, title: "doc " + i}); c.insertMany(docs);' >/dev/null

  local body resp code id
  body=$(jq -nc --arg uri "mongodb://127.0.0.1:$port/?directConnection=true" \
    --arg mu "http://127.0.0.1:$MEILI_PORT" --arg mk "$MEILI_KEY" \
    '{name: "mongo", source: {kind: "mongodb", config: {uri: $uri}}, destination: {url: $mu, api_key: $mk},
      streams: [{collection: "shop.items", index: "mongo_items"}]}')
  resp=$(api POST /syncs "$body"); code=$(tail -n1 <<<"$resp")
  [[ "$code" == 201 ]] || { fail "$name" "create returned $code: $(sed '$d' <<<"$resp")"; return; }
  id=$(sed '$d' <<<"$resp" | jq -r .id)
  wait_until "mongo streaming" "$TIMEOUT" phase_is "$id" streaming \
    || { fail "$name" "phase $(status_field "$id" phase): $(status_field "$id" message)"; return; }
  converge "mongo snapshot" mongo_lines mongo_items || { fail "$name" "snapshot did not converge"; return; }

  mongo_eval 'const c = db.getSiblingDB("shop").items;
    for (let i = 2001; i <= 2100; i++) c.insertOne({_id: i, title: "new " + i});
    c.updateMany({_id: {$lte: 50}}, {$set: {title: "edited"}});
    c.deleteMany({_id: {$gt: 100, $lte: 130}});
    c.replaceOne({_id: 200}, {title: "replaced"});' >/dev/null
  converge "mongo stream" mongo_lines mongo_items || { fail "$name" "stream did not converge"; return; }

  ( for i in $(seq 1 60); do
      mongo_eval "const c = db.getSiblingDB('shop').items; c.insertOne({_id: 10000 + $i, title: 'burst $i'}); c.updateOne({_id: 300 + $i}, {\$set: {title: 'burst-upd $i'}}); c.deleteOne({_id: 500 + $i});" >/dev/null 2>&1
    done ) &
  local w=$!
  sleep 3
  kill_server
  sleep 1
  start_server || { fail "$name" "the server did not restart"; wait "$w"; return; }
  wait "$w"
  converge "mongo restart" mongo_lines mongo_items || { fail "$name" "missing or extra documents after the restart"; return; }
  [[ "$(api_code DELETE "/syncs/$id")" == 204 ]] || { fail "$name" "delete failed"; return; }
  pass "$name"
}

if [[ -z "${E2E_SKIP_MONGO:-}" ]]; then
  scenario_mongo
fi

# ------------------------------------------------------------------ host policy

PUBLIC_PORT=""
public_api() { # <method> <path> <json body>
  curl -sS -X "$1" "http://127.0.0.1:$PUBLIC_PORT$2" \
    -H "Authorization: Bearer $ADMIN_KEY" -H 'Content-Type: application/json' \
    --data-binary "$3" -w '\n%{http_code}'
}

scenario_public_policy() {
  local name="host policy public: internal origins and destinations are refused"
  PUBLIC_PORT=$(free_port)
  log "starting a second server with CONNECT_HOST_POLICY=public (log: $WORK/public.log)"
  CONNECT_HOST_POLICY=public \
  ADMIN_API_KEY="$ADMIN_KEY" \
  LISTEN_ADDR="127.0.0.1:$PUBLIC_PORT" \
  RUST_LOG="${E2E_RUST_LOG:-info}" \
    "$BIN" >>"$WORK/public.log" 2>&1 &
  PUBLIC_PID=$!
  wait_until "public server health" 60 curl -sf "http://127.0.0.1:$PUBLIC_PORT/health" \
    || { fail "$name" "the server did not start"; return; }

  local host src resp code body
  # 127.0.0.1 is the real origin: refused even though it is reachable.
  for host in 127.0.0.1 10.1.2.3 169.254.169.254 localhost; do
    src=$(jq -nc --arg url "postgres://postgres:origin@$host:$ORIGIN_PORT/app?sslmode=disable" \
      '{kind: "postgres", config: {url: $url}}')
    for path in /sources/check /sources/discover; do
      resp=$(public_api POST "$path" "$(jq -nc --argjson s "$src" '{source: $s}')")
      code=$(tail -n1 <<<"$resp"); body=$(sed '$d' <<<"$resp")
      if [[ "$code" != 422 ]] || [[ "$(jq -r .code <<<"$body")" != host_not_allowed ]]; then
        fail "$name" "$path $host answered $code: $body"; return
      fi
      if [[ "$host" == localhost ]] && grep -q '127.0.0.1\|::1' <<<"$body"; then
        fail "$name" "the refusal tells what localhost resolved to: $body"; return
      fi
    done
    resp=$(public_api POST /syncs "$(jq -nc --argjson s "$src" --arg mk "$MEILI_KEY" \
      '{name: "x", source: $s, destination: {url: "https://8.8.8.8", api_key: $mk},
        streams: [{collection: "public.items", index: "x"}]}')")
    code=$(tail -n1 <<<"$resp")
    [[ "$code" == 422 ]] || { fail "$name" "create with origin $host answered $code: $(sed '$d' <<<"$resp")"; return; }
  done
  # A Meilisearch destination on loopback, with a public origin.
  src=$(jq -nc '{kind: "postgres", config: {url: "postgres://u:p@8.8.8.8:5432/app"}}')
  for dest in "http://127.0.0.1:$MEILI_PORT" "http://localhost:$MEILI_PORT"; do
    resp=$(public_api POST /syncs "$(jq -nc --argjson s "$src" --arg mu "$dest" --arg mk "$MEILI_KEY" \
      '{name: "d", source: $s, destination: {url: $mu, api_key: $mk},
        streams: [{collection: "public.items", index: "x"}]}')")
    code=$(tail -n1 <<<"$resp"); body=$(sed '$d' <<<"$resp")
    if [[ "$code" != 422 ]] || [[ "$(jq -r .code <<<"$body")" != host_not_allowed ]]; then
      fail "$name" "destination $dest answered $code: $body"; return
    fi
  done
  # Nothing was stored.
  local n
  n=$(curl -sS "http://127.0.0.1:$PUBLIC_PORT/syncs" -H "Authorization: Bearer $ADMIN_KEY" | jq '.syncs | length')
  [[ "$n" == 0 ]] || { fail "$name" "$n sync(s) stored"; return; }
  kill -TERM "$PUBLIC_PID" 2>/dev/null; wait "$PUBLIC_PID" 2>/dev/null; PUBLIC_PID=""
  pass "$name"
}
scenario_public_policy

scenario_sigterm() {
  local name="SIGTERM stops the server gracefully"
  kill -TERM "$SERVER_PID"
  local deadline=$((SECONDS + 40))
  while (( SECONDS < deadline )) && kill -0 "$SERVER_PID" 2>/dev/null; do sleep 0.5; done
  if kill -0 "$SERVER_PID" 2>/dev/null; then fail "$name" "still running 40 s after SIGTERM"; return; fi
  wait "$SERVER_PID"; local rc=$?
  SERVER_PID=""
  [[ "$rc" == 0 ]] || { fail "$name" "exit code $rc"; return; }
  tail -n 3 "$WORK/server.log" | grep -q "stopped" || { fail "$name" "no clean stop in the log"; return; }
  pass "$name"
}
scenario_sigterm

# ------------------------------------------------------------------ report

if grep -q "$MEILI_KEY\|postgres:origin@\|$LAB_SECRET\|$METRICS_TOKEN" "$WORK/server.log" "$WORK/public.log" 2>/dev/null; then
  fail "logs" "the server log contains a secret"
else
  pass "logs: no secret in the server log"
fi

echo
log "results"
printf '  %s\n' "${RESULTS[@]}"
if (( FAILED )); then
  log "server log tail:"
  tail -n 40 "$WORK/server.log" >&2
  exit 1
fi
