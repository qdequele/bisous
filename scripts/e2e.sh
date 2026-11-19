#!/usr/bin/env bash
# End-to-end check of meili-connect against real containers, through the
# HTTP API only.
#
#   scripts/e2e.sh            # Postgres scenarios, then MongoDB
#   E2E_SKIP_MONGO=1 scripts/e2e.sh
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

SERVER_PID=""
RESULTS=()
FAILED=0

log() { printf '\033[1;34m[e2e]\033[0m %s\n' "$*"; }
pass() { RESULTS+=("PASS  $1"); log "PASS: $1"; }
fail() { RESULTS+=("FAIL  $1: $2"); FAILED=1; printf '\033[1;31m[e2e] FAIL: %s: %s\033[0m\n' "$1" "$2"; }

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill -9 "$SERVER_PID" 2>/dev/null || true
  fi
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

scenario_check_discover
scenario_snapshot
if [[ -n "$SYNC_ID" ]]; then
  scenario_stream
  scenario_crash
  scenario_resync
  scenario_truncate
  scenario_delete
fi

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

if grep -q "$MEILI_KEY\|postgres:origin@" "$WORK/server.log"; then
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
