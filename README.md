# meili-connect

Keeps Meilisearch indexes in sync with your database by following its change
stream (CDC): **Postgres, Supabase, MySQL/MariaDB, MongoDB, Kafka,
DynamoDB and Firestore**. A sync snapshots the existing rows, then streams every insert,
update and delete into Meilisearch, and survives crashes, restarts and lost
positions without missing or duplicating documents.

It runs standalone (one binary, one Postgres for its state) and as the
**Connect** data plane of the Meilisearch Lab. 100% Rust.

- [Quick start](#quick-start)
- [The API by example](#the-api-by-example)
- [Configuration](#configuration)
- [Per-source prerequisites](#per-source-prerequisites)
- [Guarantees and known limits](#guarantees-and-known-limits)
- [Development](#development)

## Quick start

```bash
cp .env.example .env
echo "CONNECT_SECRET_KEY=$(openssl rand -base64 32)" >> .env
docker compose up -d --build
curl localhost:8080/health          # {"status":"ok"}
export AUTH="Authorization: Bearer dev-admin-key-change-me"
```

`compose.yaml` starts the server, its state Postgres, a Meilisearch
(`localhost:7700`, key `dev-master-key-change-me`) and a demo origin Postgres
(`localhost:5433`, `wal_level=logical`, a `products` table with 1,000 rows).
Compose publishes every port on `127.0.0.1` only and sets a development
`ADMIN_API_KEY` (`dev-admin-key-change-me`, change it in `.env`); the
examples send it.

Sync the demo table (inside compose the services reach each other by name):

```bash
curl -s localhost:8080/syncs -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "name": "products",
  "source": {"kind": "postgres", "config": {"url": "postgres://app:app@origin:5432/app?sslmode=disable"}},
  "destination": {"url": "http://meilisearch:7700", "api_key": "dev-master-key-change-me"},
  "streams": [{"collection": "public.products", "index": "products"}]
}' | jq
```

Then change the table and search:

```bash
docker compose exec origin psql -U app -c "UPDATE products SET name = 'Blue lamp' WHERE id = 1"
curl -s 'localhost:7700/indexes/products/search?q=lamp' -H 'Authorization: Bearer dev-master-key-change-me' | jq '.hits[0]'
```

Without Docker: `LISTEN_ADDR=127.0.0.1:8080 cargo run -p meili-connect-server`
(in-memory store when `DATABASE_URL` is unset; nothing survives a restart).
With no `ADMIN_API_KEY` nor `LAB_SERVICE_TOKEN` the API is in open mode, which
the server only accepts on a loopback address or with `ALLOW_OPEN_MODE=true`.

## The API by example

The full contract is [`docs/openapi.yaml`](docs/openapi.yaml). Every error has
the body `{"code": "...", "message": "..."}`.

### Auth and tenants

| Configured | Caller sends | Scope |
|---|---|---|
| `LAB_SERVICE_TOKEN` | `Authorization: Bearer <token>` **and** `X-Connect-Tenant-Id: <tenant>` | that tenant only |
| `ADMIN_API_KEY` | `Authorization: Bearer <key>`, optionally `X-Connect-Tenant-Id` | global, or the named tenant |
| neither | nothing | global (open mode: only on a loopback `LISTEN_ADDR` or with `ALLOW_OPEN_MODE=true`, logged loudly) |

Tokens are compared in constant time. A sync of another scope does not exist
for the caller: every route answers `404`. `/health` is always open.

```bash
export API=localhost:8080
export AUTH="Authorization: Bearer $ADMIN_API_KEY"
```

### Check and discover a source (nothing is stored)

```bash
curl -s $API/sources/check -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "source": {"kind": "postgres", "config": {"url": "postgres://app:app@origin:5432/app"}},
  "streams": [{"collection": "public.products", "index": "products"}]
}' | jq
# {"passed": true, "items": [{"name": "wal_level", "status": "ok", "message": "wal_level is logical"}, ...]}

curl -s $API/sources/discover -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "source": {"kind": "postgres", "config": {"url": "postgres://app:app@origin:5432/app"}}
}' | jq '.collections[] | {name, key, estimated_rows}'
```

Without `streams`, the check tests the connection and the server settings
only, before any table is picked: the table checks are one `ok` item
(`{"name": "tables", "status": "ok", "message": "no table selected yet: table
checks run once you pick them"}`; `collections` for MongoDB, `topics` for
Kafka). Both are bounded by `SOURCE_PROBE_TIMEOUT_SECS` (`504
source_timeout`).

### Create, read, list

```bash
curl -s $API/syncs -H "$AUTH" -H 'Content-Type: application/json' -d '{
  "name": "products",
  "source": {"kind": "postgres", "config": {"url": "postgres://app:app@origin:5432/app"}},
  "destination": {"url": "http://meilisearch:7700", "api_key": "MEILI_KEY"},
  "streams": [{
    "collection": "public.products",
    "index": "products",
    "primary_key": "id",
    "fields": {"exclude": ["internal_notes"]},
    "rename": {"price_cents": "price"},
    "vector": {"field": "embedding", "embedder": "default"}
  }],
  "mass_delete_guard": 0.5
}'
# 201 {"id": "0193...", "source": {"config": {"url": "<redacted>"}}, "destination": {"api_key": "<redacted>"}, ...}

curl -s $API/syncs -H "$AUTH" | jq '.syncs[] | {id, name}'
curl -s $API/syncs/$ID -H "$AUTH"
```

The id is generated by the server (UUIDv7). **Secrets are write-only**: every
spec the API returns has the Meilisearch key and the source's secrets
(passwords, connection strings with credentials, keys, tokens) replaced by
`"<redacted>"`.

### Update

`PUT` replaces the spec. What happens next depends on what changed:

- **Another origin** (kind, host, port, URL, user, database, TLS, slot or
  publication name, `manage_publication`): the old origin is cleaned up first with the old spec
  (slot, publication…); if that fails nothing changes (`502
  teardown_failed`, or `?force=true` to go on and clean up by hand). Then the
  sync starts fresh: full snapshot.
- **Other streams** (index, primary key, fields, renames, vector, a new
  collection) **or another Meilisearch URL**: the checkpoint is cleared and
  the sync snapshots everything again, into shadow indexes where the live
  ones hold data, so new mappings are backfilled. An index the sync was
  still filling for the first time is emptied and filled again in place.
- **Anything else** (name, guard, secrets, tuning): a plain restart.

A paused sync stays paused. Send
`"<redacted>"` for any secret to keep the stored value, so a `GET` body can be
edited and sent back. A placeholder is only accepted while the connection it
belongs to is unchanged: the source's kind, hosts, ports, URLs, user,
database and TLS settings for a source secret, the URL for the Meilisearch
key. Point the sync somewhere else and the secret must be sent again
(`422 secret_required`), so a stored secret never follows a new host.

```bash
curl -s $API/syncs/$ID -H "$AUTH" | jq '.name = "products v2"' \
  | curl -s -X PUT $API/syncs/$ID -H "$AUTH" -H 'Content-Type: application/json' -d @-
```

### Status, progress, dead letters

```bash
curl -s $API/syncs/$ID/status -H "$AUTH"
# {"phase": "streaming", "message": null, "reason": null, "has_checkpoint": true, "resyncing": false,
#  "running": true, "progress": {"written": 1000, "deleted": 0, "rejected": 0},
#  "documents": {"documents_streamed": 0, "documents_snapshot": 1000, "documents_resync": 0},
#  "bytes": 98213, "sync_seconds": 42, "lag": {"seconds": 0.0, "bytes": 0},
#  "lag_warning": null, "dead_letters": 0, ...}

curl -s "$API/syncs/$ID/progress?from=2026-11-01&to=2026-11-19" -H "$AUTH"
curl -s "$API/syncs/$ID/dead-letters?limit=100" -H "$AUTH"     # page with ?after=<next_after>
curl -s -X DELETE $API/syncs/$ID/dead-letters -H "$AUTH"
```

Phases: `pending`, `snapshotting`, `streaming`, `paused` (by you),
`source_paused` (the origin is deliberately down, retried slowly), `held`
(needs you: a guard or a configuration error, see `message`; or
`out_of_credits`, which resumes by itself), `failed`.

`lag` is how far behind its origin the sync is while streaming, when the
source can tell (`null` otherwise): Postgres reports `seconds` (age of the
last commit, 0 once caught up) and `bytes` (WAL the slot retains past the
confirmed position), MongoDB and DynamoDB `seconds`, Kafka `seconds` and
`records` (behind the high watermarks); MySQL does not report it yet.

### Usage

```bash
curl -s "$API/usage?from=2026-11-01&to=2026-11-19&group_by=day" -H "$AUTH"
# {"rows": [{"day": "2026-11-18", "units": {"documents_streamed": 1200, "documents_snapshot": 0,
#   "documents_resync": 0, "bytes": 1482113, "sync_seconds": 86400}}, ...],
#  "totals": {...}, "from": "2026-11-01", "to": "2026-11-19", "group_by": "day", "source": "state"}
curl -s "$API/usage?group_by=sync" -H "$AUTH"   # rows: {"sync_id", "name", "units"}
```

The caller's metered units (see [Pricing and usage](#pricing-and-usage)) per
UTC day or per sync; at most 366 days, the last 30 by default.

### Actions

```bash
curl -s -X POST $API/syncs/$ID/pause   -H "$AUTH"   # stays paused across restarts
curl -s -X POST $API/syncs/$ID/resume  -H "$AUTH"   # also restarts a held or failed sync
curl -s -X POST $API/syncs/$ID/resync  -H "$AUTH"   # full rebuild through shadow indexes (in place while the first copy never completed)
curl -s -X POST $API/syncs/$ID/release -H "$AUTH"   # apply the TRUNCATE / mass delete that held it
```

### Delete

```bash
curl -s -X DELETE $API/syncs/$ID -H "$AUTH"
```

Stops the sync, deletes leftover shadow indexes, tears the source down on the
origin (replication slot and publication, consumer group), then deletes the
sync with its state, dead letters and progress. The Meilisearch indexes are
kept. If the origin cannot be reached the answer is `502 teardown_failed` and
the sync is kept, paused (its status says why; it stays stopped across
restarts), so the call can be retried; `?force=true` deletes
it anyway (drop the slot by hand then, or it retains WAL).

## Configuration

Every variable, with defaults, is in [`.env.example`](.env.example).

| Variable | Default | |
|---|---|---|
| `LISTEN_ADDR` | `0.0.0.0:8080` | HTTP API address. |
| `DATABASE_URL` | unset | State Postgres. **Unset: in-memory store, nothing survives a restart** (warned at startup). |
| `CONNECT_SECRET_KEY` | | Base64 32-byte key sealing specs at rest. Required with `DATABASE_URL`. `openssl rand -base64 32`. |
| `ADMIN_API_KEY` | unset | Standalone admin token. Set but blank: startup error. |
| `ALLOW_OPEN_MODE` | `false` | Without any token the server only starts on a loopback `LISTEN_ADDR`, unless this is `true`. |
| `LAB_SERVICE_TOKEN` | unset | Lab token; requires `X-Connect-Tenant-Id`. Set but blank: startup error. |
| `CONNECT_HOST_POLICY` | `public` with `LAB_SERVICE_TOKEN`, else `any` | Which hosts sources and destinations may point at: `public`, `any` or `allowlist`. See [Host policy](#host-policy). |
| `CONNECT_HOST_ALLOWLIST` | unset | With `allowlist`: comma-separated `host`, `host:port`, IP addresses and CIDRs. |
| `CONNECT_EGRESS_IPS` | unset | The public IPs or CIDRs this server's connections leave from, comma- or space-separated. Published by `GET /network` for users' database allow-lists. Informational: the server does not check it. |
| `LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET` | unset | Lab mode: usage and sync state events, credit holds. All three or none. |
| `USAGE_FLUSH_INTERVAL_SECS` | `60` | How often recorded usage with documents or bytes becomes events. |
| `USAGE_IDLE_FLUSH_SECS` | `3600` | How long a running sync's `sync_seconds` alone (an idle sync) wait before they become an event (1-86400). |
| `SYNC_EVENTS_SETTLE_SECS` | `30` | Lab mode: how long a sync's new phase must hold before `sync.state_changed` reports it (0-3600). Flapping between two phases inside it sends nothing. |
| `SYNC_HEALTH_INTERVAL_SECS` | `300` | Lab mode: how often the replication slots of tenants' Postgres and Supabase syncs are measured for `sync.health_changed`, in every phase (0-86400, `0` turns the health probe off). |
| `SYNC_LAG_WARN_SECS` | `900` | Lab mode: lag of a streaming sync that reports `sync.health_changed` `warning` (cause `lag`); `ok` again under 80 % of it. |
| `SYNC_WAL_WARN_BYTES` | `1073741824` | Lab mode: WAL a sync's replication slot keeps that reports `warning` (cause `wal_retained`); half of a positive `max_slot_wal_keep_size` warns too when lower. `ok` again under 80 % of it. |
| `LAB_SEND_INTERVAL_SECS` | `2` | How often the outbox lanes are delivered. |
| `CREDITS_CHECK_INTERVAL_SECS` | `15` | Lab mode: how often the teams' credits are checked. |
| `CREDITS_CACHE_FRESH_SECS` / `CREDITS_CACHE_STALE_SECS` | `30` / `300` | How long a credits answer is reused, and kept quietly while the Lab cannot answer. |
| `METRICS_TOKEN` | unset | Bearer token of `GET /metrics`; unset: 404. Never a management token. |
| `TINYBIRD_URL`, `TINYBIRD_TOKEN` | unset | Usage analytics in Tinybird. Both or neither. |
| `MAX_LAG_HOLD_BYTES` / `MAX_LAG_HOLD_SECS` | unset | Lag guard: a stuck sync past these gets a `lag_warning`. |
| `SOURCE_PROBE_TIMEOUT_SECS` | `30` | Limit of `check` and `discover`. |
| `SHUTDOWN_GRACE_SECS` | `30` | Time the syncs get to stop on SIGTERM/SIGINT; also the bound on stopping one sync. |
| `MAX_SYNCS_PER_TENANT` | `20` | Syncs a tenant may own (`409 sync_quota_exceeded`). The global scope is not limited. |
| `PROBE_CONCURRENCY` / `PROBE_CONCURRENCY_PER_TENANT` | `16` / `4` | `check` and `discover` running at once, in all and per tenant (`429 too_many_probes`). |
| `REQUEST_TIMEOUT_SECS` | `180` | Longest API request (`503 request_timeout`); a change to a sync still completes. |
| `DEAD_LETTERS_MAX_PER_SYNC` | `10000` | Dead letters kept per sync; past it they are counted (`dead_letters_overflow` in the status) but not stored. Payloads over 64 KiB are stored as `{"truncated": true, "bytes": n}`. |
| `LAB_OUTBOX_MAX_PENDING` | `100000` | Unsent events of an outbox lane beyond which usage stays pending in the state database instead of enqueued. |
| `LOG_FORMAT` | `pretty` | `pretty` or `json`. |
| `LOG_LEVEL` / `RUST_LOG` | `info` | `RUST_LOG` wins and takes the full filter syntax. |

Specs, checkpoints, document bodies and secrets are never logged.

### Host policy

A sync's source and its Meilisearch destination are addresses the caller
chooses, and the server connects to them from inside its own network. The
host policy decides, before any connection, which addresses are allowed:

| `CONNECT_HOST_POLICY` | Allows |
|---|---|
| `public` (default with `LAB_SERVICE_TOKEN`) | Names whose **every** resolved address is public, and public literal addresses. Loopback, RFC 1918, CGNAT `100.64/10`, link-local (`169.254.169.254` included), IPv6 ULA `fc00::/7`, the IPv4-mapped and NAT64 forms of these, multicast, broadcast, unspecified and documentation ranges are refused. |
| `any` (default standalone) | Every host. |
| `allowlist` | Only the entries of `CONNECT_HOST_ALLOWLIST`: a host name (any address it resolves to, optionally one port) or a network (every resolved address must be in one). |

In every mode the server's own state database (`DATABASE_URL`) is refused as
a source origin. The policy applies to `check`, `discover`, `POST` and `PUT`
(`422 host_not_allowed`, naming the host but not what it resolved to), and
again every time a sync starts or restarts (a refused sync is `held`). The
Meilisearch client resolves names through the policy and connects only to the
vetted addresses, uses no proxy and never follows redirects.

Errors reported to the Lab (calls with `LAB_SERVICE_TOKEN`, and the
`message` / `last_error` of a tenant's sync) are reduced to their class and a
fixed description; the server log keeps the detail. The standalone admin sees
detailed errors.

**Residual risk.** Database drivers (Postgres, MySQL, MongoDB, Kafka, the AWS
SDK) resolve names themselves: between the policy's lookup and the driver's
connect a DNS rebinding window remains for sources (not for the Meilisearch
destination, whose connection is pinned to the vetted addresses). Kafka
brokers advertised in the cluster metadata, and MongoDB replica-set members
discovered from the seed hosts, are not vetted. A hosted deployment must
therefore also restrict egress at the network level (no route to the
metadata endpoint, the state database or internal services from the
connector's network).

**The server's own identity.** On a hosted server (`LAB_SERVICE_TOKEN` set,
or `CONNECT_HOST_POLICY=public`) a tenant's source may not use what belongs
to the server (`422 ambient_credentials`, at check, discover, create and
update, and a stored one is held at its next start): DynamoDB
`credentials: default` (the host's AWS chain: give a role to assume or
static keys), MongoDB URI options reading local files (`tlsCAFile`,
`tlsCertificateKeyFile`, `tlsCRLFile`, …) or falling back to the host's
identity (`authMechanism=MONGODB-AWS`, `GSSAPI`, `MONGODB-X509`,
`MONGODB-OIDC`), and Postgres URL parameters naming local files
(`sslrootcert`, `sslcert`, `sslkey`, `passfile`, `service`, …; paste the CA
as `ca_cert`). The standalone global scope keeps them.

**DynamoDB external IDs.** On a hosted server the server assumes a tenant's
DynamoDB role with its own AWS identity, so the external ID is chosen by the
server, one per tenant, never by the tenant: leave `external_id` out (another
value answers `422 external_id_not_allowed`) and trust the role as
`GET /sources/dynamodb/trust` shows. It needs `CONNECT_SECRET_KEY`: without
it a tenant's `assume_role` answers `422 external_id_unavailable`. Changing
`CONNECT_SECRET_KEY` changes every tenant's external ID. The standalone
global scope sets its own `external_id`.

### Pricing and usage

The contract with the Lab is [`docs/LAB_INTEGRATION.md`](docs/LAB_INTEGRATION.md)
("Pricing", "Usage event"). Every applied batch records its units:

| Unit | What |
|---|---|
| `documents_streamed` | documents Meilisearch accepted from the change stream (writes and deletes) |
| `documents_snapshot` | documents written by a first sync or a resync you asked for (`POST /resync`, or a `PUT` that changed the streams) |
| `documents_resync` | documents rewritten by a resync the engine started after the origin lost its position (free) |
| `bytes` | uncompressed payload bytes Meilisearch accepted (each document's NDJSON line or id, once) |
| `sync_seconds` | seconds spent snapshotting or streaming (a paused or held sync costs nothing) |

Rejected and skipped documents are never counted. A crash between
Meilisearch accepting a batch and its checkpoint replays that batch, which is
then counted twice: at most one batch per index per crash, accepted by the
contract. `GET /syncs/{id}/status` shows a sync's totals, `GET /usage` the
tenant's per day or per sync.

### Usage events (the Lab)

With the three `LAB_*` variables set, the units are added, in the same
transaction as the progress counters, to a pending row per tenant-owned
sync; every `USAGE_FLUSH_INTERVAL_SECS` the pending rows that are due become
one `usage.recorded` event per sync, in one transaction (nothing is lost on a
crash, nothing is held in memory). A row is due when it holds documents or
bytes (it carries the seconds accumulated so far), when its sync stopped
(paused, held, failed, deleted, out of credits: the tail goes at once or at
the next flush; a graceful shutdown sends everything), or when it has held
only `sync_seconds` for `USAGE_IDLE_FLUSH_SECS`: an idle sync sends about one
event an hour, not one a minute, and no second is lost. `GET /usage` and the
status include what is still pending:

```json
{"id": "<UUIDv7>", "type": "usage.recorded", "occurred_at": "...", "account_id": "<tenant>",
 "api_key_id": null, "product": "connect",
 "data": {"operation": "sync",
          "units": {"documents_streamed": 1200, "documents_snapshot": 0, "documents_resync": 0,
                    "bytes": 1482113, "sync_seconds": 60},
          "job_id": "<sync id>", "description": "postgres → products"}}
```

A sender leases up to 500 events, posts `{"events": [...]}` to
`<LAB_URL>/internal/events` with `X-Lab-Instance-Id`, `X-Lab-Timestamp` and
`X-Lab-Signature: sha256=<hex HMAC-SHA256(LAB_INSTANCE_SECRET, "<ts>.<body>")>`,
marks the ids the Lab lists in `accepted` as sent and retries the rest. An
event that failed waits longer after each attempt (10 s doubling up to an
hour), the sender slows down while the Lab fails and logs it at most every
5 minutes, and past `LAB_OUTBOX_MAX_PENDING` unsent events usage stays
pending in the state database (one row per sync) instead of enqueued. Sent
events are pruned after 7 days. Delivery is **at-least-once** (the event id
is stable, the Lab deduplicates). Global (non-tenant) syncs are never sent to
the Lab.

**Out of credits.** In Lab mode the server asks
`GET <LAB_URL>/internal/accounts/{id}` whether each team can spend (an answer
is reused for 30 s; when the Lab cannot answer the last known answer stands,
and a team never answered for runs). A team that cannot spend has its
running syncs set to `held` with the message `out_of_credits` (nothing torn
down: the origin keeps the position); new syncs are created held; `release`,
`resume` and `resync` answer `402 out_of_credits`. They resume by themselves
once the team can spend again, also after a restart. A pause wins.

### Telemetry and analytics

- **`GET /metrics`** (Prometheus text) with `Authorization: Bearer
  <METRICS_TOKEN>` only (404 without the variable; the Lab and admin tokens
  are refused): `connect_syncs{phase}`, `connect_documents_total{kind}`,
  `connect_bytes_total`, `connect_batch_seconds`,
  `connect_sink_errors_total{class}`, `connect_source_errors_total{kind,class}`,
  `connect_lag_seconds` (rebuilt at each scrape from the running syncs' lag),
  `connect_dead_letters_total`, `connect_outbox_backlog`,
  `connect_lab_events_sent_total{result}` (`accepted`, `rejected`, `failed`),
  plus `connect_sync_seconds_total`, `connect_tinybird_*` and `process_*`
  (Linux). No tenant or sync id in any label. The names are a contract with
  the deploy box's dashboards: never rename one.
- **Tinybird** (optional, `TINYBIRD_URL` + `TINYBIRD_TOKEN`): every usage
  flush is also appended to the `connect_usage` data source, from its own
  outbox lane (deduplicated by `event_id`, at least once, its own backoff and
  backlog cap: the Lab failing never holds it back, nor the other way round);
  global syncs go with `tenant_id ""`. `GET /usage` then reads the
  `connect_tenant_usage` pipe. The data files, `tb` commands and the rule
  that column names never change are in [`tinybird/`](tinybird/README.md).
- **Lag guard**: with `MAX_LAG_HOLD_BYTES` and/or `MAX_LAG_HOLD_SECS`, a
  sync past them that saved no checkpoint for 5 minutes gets a `lag_warning`
  in its status. Nothing is dropped automatically: freeing a customer's
  replication slot or offsets loses its position, which is an operator's
  decision (pause or delete the sync, or fix the cause).

## Per-source prerequisites

`collection` names: `schema.table` (Postgres, Supabase, MySQL as
`database.table`), `database.collection` (MongoDB), the topic (Kafka), the
table name (DynamoDB), a collection, subcollection path or `**/<id>`
collection group (Firestore). Run `POST /sources/check` first: it reports exactly
what is missing.

### Postgres

Config: `{"url": "postgres://user:pass@host:5432/db?sslmode=require"}` or
`host`, `port`, `database`, `user`, `password`, `sslmode`
(`disable`/`prefer`/`require`/`verify-ca`/`verify-full`), `ca_cert`, and
optional `slot_name` / `publication_name` / `manage_publication` /
`provider`.

- `wal_level = logical` (restart required), free `max_replication_slots` and
  `max_wal_senders`.
- A role with `REPLICATION` (or `rds_replication` on RDS, `azure_pg_admin`…),
  `SELECT` on the tables, and ownership of them (to add them to a
  publication), or an existing publication.
- Every table needs a primary key (or `REPLICA IDENTITY FULL` and a key).
- One slot + one publication per sync, named `meili_connect_<sync id>`. A
  stalled sync retains WAL: set `max_slot_wal_keep_size` on busy databases.
- A table with row-level security needs a role that bypasses it
  (`BYPASSRLS`, a superuser, or the table owner without `FORCE ROW LEVEL
  SECURITY`): otherwise the snapshot copies only the rows the policies
  allow, and `check` fails the table.
- `manage_publication: false` (default `true`) uses a publication managed
  elsewhere, named by the required `publication_name`: create it yourself
  (as the tables' owner) `FOR TABLE` the mapped tables, ideally `WITH
  (publish_via_partition_root = true)`. The sync never creates, alters or
  drops it, so the role needs neither `CREATE` on the database nor
  ownership of the tables; `check` fails when it is missing or does not
  cover a mapped table, and deleting the sync drops only the slot. `slot_name`
  cannot be set in this mode: the slot is always named after the sync.
- `provider` (`neon`, `rds`, `aurora`, `cloud_sql`, `azure`; optional,
  detected from `*.neon.tech`, `*.rds.amazonaws.com` and
  `*.postgres.database.azure.com` hosts) makes failed checks name that
  service's fix: RDS `rds.logical_replication = 1` in a custom parameter
  group (Aurora: the cluster parameter group) and `GRANT rds_replication`;
  Cloud SQL the `cloudsql.logical_decoding` flag and `ALTER USER … WITH
  REPLICATION`; Azure the `wal_level` server parameter; Neon the project's
  Logical replication setting. A Neon `-pooler` host is refused: use the
  direct one. Neon removes a slot that has not advanced for about 40 hours,
  so a sync stopped longer resyncs.
- PlanetScale Postgres (`provider: planetscale`, detected from
  `*.horizon.psdb.cloud`): set `wal_level = logical` in the cluster's
  Parameters, use a role `WITH REPLICATION` (log in as `<role>.<branch id>`)
  on port 5432, not PgBouncer's 6432. Its connection string's
  `sslrootcert=system` (the system trust store, not a file) is accepted.
  The slot is created failover-enabled, as PlanetScale requires; `check`
  warns about an older slot without failover.
- Allow-list `GET /network`'s `egress_ips` in the database's security
  group, authorized networks or firewall rules.

### Supabase

`"kind": "supabase"`, same config. Use the **direct connection** string
(`db.<ref>.supabase.co:5432`), not the transaction pooler; it may be IPv6
only. A paused project shows as `source_paused`. The `supabase_realtime`
publication is never reused. Supabase limits slots per plan.

### MySQL / MariaDB

Config: `host`, `port`, `user`, `password`, `database`, `tls`
(`{"mode": "disabled|required|verify_ca|verify_identity", "ca_pem": "..."}`, default `required`),
`server_id`, `snapshot_batch_size`, `provider` (`rds`, `aurora`,
`cloud_sql`, `azure`; optional, detected from `*.rds.amazonaws.com` and
`*.mysql.database.azure.com` hosts: failed checks then name that service's
parameter group, flag or procedure).

- `binlog_format = ROW`, `binlog_row_image = FULL`; GTIDs
  (`gtid_mode = ON`, `enforce_gtid_consistency = ON`) recommended, file +
  position otherwise (always on MariaDB).
- `REPLICATION SLAVE`, `REPLICATION CLIENT`, `SELECT` on the tables.
- Binlog retention longer than any downtime, or the sync resyncs. RDS and
  Aurora: `CALL mysql.rds_set_configuration('binlog retention hours', 72)`
  (RDS keeps none by default, and writes binlogs only with automated
  backups on; Aurora needs `binlog_format` set in the cluster parameter
  group). Cloud SQL: point-in-time recovery on, `binlog_expire_logs_seconds`
  flag. Azure: `binlog_expire_logs_seconds` (0, the default, purges at once).
- PlanetScale (Vitess) exposes no MySQL binlog to external readers: use
  the [`planetscale`](#planetscale) source.

### MongoDB

Config: `{"uri": "mongodb+srv://user:pass@cluster/..."}`, optional
`database`, `full_document` (`updateLookup` default), `snapshot_batch_size`.

- A replica set or a sharded cluster (change streams). A single node works
  as a one-member replica set.
- `read` on the databases, `changeStream` / `find` privileges.
- Oplog retention longer than any downtime, or the sync resyncs.

### Kafka

Config: `bootstrap_servers`, `security` (`protocol`, `tls`, `sasl` with
`PLAIN`/`SCRAM-SHA-256`/`SCRAM-SHA-512`), `format` (`json`: the value is the
document, a null value deletes it; `debezium`: change-event envelopes),
`start_from` (`earliest`/`latest`), `key_fields` (document fields that make
the key; the record key otherwise).

- No snapshot: the topic is the history (use compacted topics).
- Offsets live in meili-connect's store; they are also committed under the
  group `meili-connect-<sync id>` for monitoring only.

### DynamoDB

Config: `region`, `endpoint_url` (DynamoDB Local), `credentials`
(`{"type": "default"}`, `{"type": "static", "access_key_id", "secret_access_key"}`,
`{"type": "assume_role", "role_arn", "external_id"}`), `scan`, `stream`.

- On a hosted server (the Lab), with `assume_role`: let this server assume
  the role. `GET /sources/dynamodb/trust` returns your team's external ID,
  the server's AWS principal and the trust policy to paste on the role; do
  not send `external_id` (it is set for you).

  ```bash
  curl -s $API/sources/dynamodb/trust -H "$AUTH" -H "X-Connect-Tenant-Id: $TEAM" | jq
  # {"external_id": "mc-3f9a1c0e5b7d2a4e8c6f1b0d9e7a5c3b",
  #  "principal": "arn:aws:iam::111122223333:role/meili-connect",
  #  "trust_policy": {"Version": "2012-10-17", "Statement": [{"Effect": "Allow",
  #    "Principal": {"AWS": "arn:aws:iam::111122223333:role/meili-connect"},
  #    "Action": "sts:AssumeRole",
  #    "Condition": {"StringEquals": {"sts:ExternalId": "mc-3f9a1c0e5b7d2a4e8c6f1b0d9e7a5c3b"}}}]},
  #  "note": null}
  ```

  `principal` is `null` (with a `note`, and a placeholder in the policy)
  when the server cannot resolve its own identity.
- DynamoDB Streams enabled on every table with `NEW_IMAGE` or
  `NEW_AND_OLD_IMAGES` (never enabled for you).
- IAM: `DescribeTable`, `Scan` on the tables; `DescribeStream`,
  `GetShardIterator`, `GetRecords` on `<table arn>/stream/*`.
- Stream records live 24 hours: a sync stopped longer resyncs.

### Firestore

Config: `service_account` (the JSON key file of a service account, as
text), optional `project_id` (the key's by default), `database_id`
(`(default)`), `snapshot.page_size` (300), `stream.max_tracked_documents`
(500000). `emulator_host` (`host:port`) targets the Firestore emulator
instead, with no key.

```json
{"kind": "firestore", "config": {"service_account": "{\"type\": \"service_account\", \"project_id\": \"my-app\", ...}"}}
```

- IAM: **Cloud Datastore Viewer** (`roles/datastore.viewer`) for the
  service account on the project. Native mode databases only.
- `collection`: `products` (a top-level collection), `users/alice/orders`
  (one subcollection), or `**/orders` (every collection named `orders`, at
  any depth). The document id is the Firestore document id for a
  collection and the document path for a collection group (where ids
  repeat across parents); every document carries `_path`, its path.
- Types: timestamps → RFC 3339 strings (UTC, microseconds); geopoints →
  `{"lat", "lng"}` (rename the field to `_geo` to filter by distance);
  references → the referenced document's path; bytes → base64; maps and
  arrays as JSON.
- Changes arrive through realtime listeners (`Listen`); the checkpoint is
  the stream's resume token. When Firestore cannot send what changed since
  the token (after a long stop) it re-sends every document: a sync that
  has not restarted since its snapshot applies it (deleting what was not
  re-sent), a restarted one resyncs into shadow indexes. The emulator
  re-sends on every resume, so there a restart always resyncs.
- The key is sent only to Google: its `token_uri` is ignored.

### PlanetScale

`"kind": "planetscale"`, for PlanetScale's MySQL databases (Vitess). They
serve no binlog to outside readers, so changes come from Vitess's
**VStream** API (gRPC over TLS), like PlanetScale's own connectors.
PlanetScale Postgres is the [Postgres](#postgres) source.

Config: `host` (the database's host, e.g. `aws.connect.psdb.cloud`),
`database`, `user`, `password` (a PlanetScale password, `pscale_pw_…`), and
optional `grpc_port` (443, VStream), `mysql_port` (3306, discovery and the
first copy), `tls` (`true`; `false` only for a local Vitess),
`tablet_type` (`primary`, `replica`, `rdonly`), `cells` (default
PlanetScale's own cell on `*.psdb.cloud`), `snapshot_batch_size`.

- Collections are `database.table`, all in the configured database; every
  table needs a primary key. Documents are the same JSON as the `mysql`
  source's.
- A password with read access is enough: nothing is created on the
  database. VStream and the MySQL protocol use the same credentials.
- The checkpoint is every shard's GTID set. A sync stopped longer than the
  binlog retention of a shard resyncs.
- Tested against a two-shard `vttestserver`
  (`crates/sources/planetscale/tests/live.rs`, `VITESS_TEST_CONFIG`).

## Guarantees and known limits

From [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md):

- **At-least-once, idempotent.** A checkpoint is saved only after
  Meilisearch reports the tasks of every change before it as succeeded. A
  crash replays from the last checkpoint; replays are harmless because the
  document id is a pure function of the source key.
- **Gapless first sync.** The stream position is captured before the
  snapshot reads anything, so changes made during the snapshot are replayed
  after it.
- **The origin is never held hostage.** Slots and offsets advance only to
  acknowledged checkpoints; a stalled sync is reported before it bloats the
  origin.
- **Position loss is a normal event.** A lost position (slot invalidated,
  binlog purged, oplog rolled over) triggers a resync into shadow indexes
  swapped in when complete; search keeps serving the old data meanwhile.
- **Guards.** A `TRUNCATE`, or a batch deleting more than
  `mass_delete_guard` (default 50%) of an index, holds the sync instead of
  emptying the index. `POST /release` applies it.
- A document Meilisearch refuses goes to the dead letters; the sync carries
  on.

Known limits:

- A snapshot interrupted by a crash restarts from scratch (no resumable
  snapshots).
- One collection per index; no joins across tables; transforms are field
  selection, renames and a vector field only.
- Usage counts not yet enqueued are lost on a crash (see above).
- No web UI: the Lab console is the UI.

## Development

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace            # live tests skip without their *_TEST_URL
scripts/e2e.sh                    # real containers: Postgres and MongoDB scenarios
```

The e2e script needs docker, curl, jq and python3. It names its containers
`mc-e2e-<pid>-*`, uses random host ports and removes everything on exit
(`E2E_KEEP_LOGS=1` keeps the server log, `E2E_SKIP_MONGO=1` skips MongoDB).

Adding a route: `crates/server/src/routes.rs` (`ROUTES`), `lib.rs`
(`router()`) and `docs/openapi.yaml`; a test fails when they disagree.
