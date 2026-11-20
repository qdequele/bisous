# Changelog

All notable changes to meili-connect are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased

### Security

- Host policy (`CONNECT_HOST_POLICY`, `CONNECT_HOST_ALLOWLIST`): `public`
  (default with `LAB_SERVICE_TOKEN`) refuses sources and Meilisearch
  destinations on loopback, private, CGNAT, link-local (cloud metadata), ULA
  and other non-public addresses, for every address a name resolves to;
  `allowlist` allows only listed hosts and networks; `any` is the standalone
  default. The state database is refused as a source in every mode. Applied
  at check, discover, create, update and every sync (re)start
  (`422 host_not_allowed`). The Meilisearch client connects only to vetted
  addresses, uses no proxy and follows no redirects.
- A `PUT` may no longer send `"<redacted>"` for a secret whose connection
  changed (new source host, port, URL, user, database or TLS settings, new
  Meilisearch URL): the stored secret would have gone to the new host.
  `422 secret_required` asks for it again.
- No more open mode by accident: without `ADMIN_API_KEY` or
  `LAB_SERVICE_TOKEN` the server refuses to start unless it listens on a
  loopback address or `ALLOW_OPEN_MODE=true`; a blank auth variable is a
  startup error. `compose.yaml` binds `127.0.0.1` and sets a development
  `ADMIN_API_KEY`.
- Errors returned to the Lab and stored for a tenant's sync are reduced to
  their class and a fixed description; non-Meilisearch response bodies are
  never echoed in errors.
- On a hosted server (`LAB_SERVICE_TOKEN`, or `CONNECT_HOST_POLICY=public`) a
  tenant's source may no longer use the server's own identity or files:
  DynamoDB `credentials: default` (the server's AWS identity), MongoDB URI
  options reading local files or falling back to the host's identity
  (`tlsCAFile`, `tlsCertificateKeyFile`, `MONGODB-AWS`, `GSSAPI`,
  `MONGODB-X509`, `MONGODB-OIDC`, …) and Postgres URL file parameters
  (`sslrootcert`, `passfile`, `service`, …) answer
  `422 ambient_credentials`; a stored one is held at its next start. The
  standalone global scope keeps them.
- On a hosted server the external ID of a tenant's DynamoDB role
  (`credentials: assume_role`) is chosen by the server, one per tenant,
  derived from `CONNECT_SECRET_KEY`: a tenant could otherwise name another
  team's role with that team's external ID and read its table through the
  server's own AWS identity. Another `external_id` answers
  `422 external_id_not_allowed`; a stored one is held at its next start
  until the role's trust policy uses the new external ID; without
  `CONNECT_SECRET_KEY`, `assume_role` answers `422 external_id_unavailable`.
  New `GET /sources/dynamodb/trust` gives a tenant its external ID, the
  server's AWS principal and the trust policy to paste. The standalone global
  scope keeps a free-form `external_id`.

### Added

- `planetscale` source: PlanetScale's MySQL (Vitess), which has no binlog
  for outside readers, through Vitess's VStream API (gRPC over TLS, the
  database's user and password). The first copy reads keyset pages over the
  MySQL protocol after capturing every shard's position; the stream emits
  inserts, updates (a key change deletes the old document), deletes and
  `TRUNCATE`, with every shard's GTID set as the checkpoint. Documents are
  the `mysql` source's JSON. New `SourceKind::Planetscale` in `core`.

- Hosted-database hints: `postgres` and `mysql` configs take an optional
  `provider` (`neon`, `rds`, `aurora`, `cloud_sql`, `azure`; MySQL without
  `neon`), also detected from RDS, Aurora, Neon and Azure host names. A
  failed check then names that provider's fix: the parameter group, database
  flag or server parameter for `wal_level` / the binlog settings, the grant
  (`rds_replication`, `WITH REPLICATION`), binlog retention
  (`mysql.rds_set_configuration`, Azure's `binlog_expire_logs_seconds = 0`
  now warns), and the network allow-list on a connection failure. A Neon
  pooled host (`-pooler`) is refused with the direct host to use instead.
- PlanetScale Postgres: `provider: planetscale` (detected from
  `*.horizon.psdb.cloud`) creates the replication slot failover-enabled
  (`FAILOVER true`, Postgres 17+), as PlanetScale requires to keep it across
  a switchover; `check` reports the slot's failover state and how to fix an
  older one, names PlanetScale's settings, and refuses PgBouncer's port 6432.
  `sslrootcert=system` in a Postgres URL (the system trust store) is no longer
  refused as a file parameter on a hosted server.
- `GET /network` lists the server's egress addresses (`CONNECT_EGRESS_IPS`)
  for database allow-lists.
- Firestore source (`"kind": "firestore"`): a service account JSON key
  (redacted like every secret; sent only to Google's token endpoint),
  `project_id` (the key's by default) and `database_id`. A mapping names a
  top-level collection, a subcollection path or a `**/<id>` collection
  group. The snapshot reads every document in pages, then realtime
  listeners (`Listen`) stream inserts, updates and deletes; the resume
  token is the checkpoint. A server reset is applied from the document set
  the source tracks, or resyncs when it cannot be (after a restart, or past
  `stream.max_tracked_documents`). Document id: the Firestore document id
  (the path for a collection group); every document carries `_path`.
  Timestamps, geopoints (`{"lat", "lng"}`), references (paths), bytes,
  maps and arrays map to JSON. `check` reports a missing role
  (`roles/datastore.viewer`) or database, `discover` lists collections with
  counts and collection groups found under sampled documents. Live tests
  run against the Firestore emulator (`FIRESTORE_TEST_EMULATOR_HOST`).
- Lab mode: a `sync.state_changed` event tells the Lab when a tenant's sync
  changes phase (`pending`, `snapshotting`, `streaming`, `paused`,
  `source_paused`, `held`, `failed`, `deleted`), including the end of a
  snapshot (`resync: true` for a resync), with a stable `reason` code
  (`source_auth`, `source_unreachable`, `source_config`,
  `insufficient_credits`, `delete_guard`, `resync_budget_spent`,
  `destination_auth`, …) next to the coarse message. A change is reported
  once its phase held for `SYNC_EVENTS_SETTLE_SECS` (default 30 s), so a
  flapping sync sends one event, not one per flap. The Lab events schema is
  vendored in `contracts/vendor/lab/` and drift-checked in CI. See
  `docs/LAB_INTEGRATION.md`, "Sync state event".
- Lab mode: a `sync.health_changed` event warns the Lab when a tenant's
  sync gets dangerous for its source, and says when it is fine again:
  `lag` (a streaming sync behind by `SYNC_LAG_WARN_SECS`, default 900 s) and
  `wal_retained` (the WAL a Postgres or Supabase replication slot keeps on
  the customer's database reaches `SYNC_WAL_WARN_BYTES`, default 1 GiB, or
  half of a positive `max_slot_wal_keep_size`). The slot is measured every
  `SYNC_HEALTH_INTERVAL_SECS` (default 300 s, `0` turns the probe off) in
  every phase where it exists, paused, held and failed included: a stopped
  sync's slot keeps WAL until the disk fills or the slot is invalidated.
  `ok` comes back only under 80 % of the threshold, and changes settle like
  state changes. See `docs/LAB_INTEGRATION.md`, "Sync health event".
- `Source::retention` (core): what a sync keeps on its origin, read on a
  short-lived connection in any phase; Postgres reads its slot's retained
  WAL, `wal_status` and `max_slot_wal_keep_size`.
- The coarse errors a Lab tenant sees name their cause more precisely
  (credentials refused, host not found, configuration, API key refused,
  index refused) without adding any detail from the driver or the server.
- Postgres and Supabase: `manage_publication: false` (default `true`) uses a
  publication managed elsewhere, named by the then required
  `publication_name`. The sync never creates, alters or drops it and needs
  no marker on it; `check` fails when it is missing or does not cover a
  mapped table (without asking for table ownership or `CREATE` on the
  database), `open` refuses it with a config error, and deleting the sync
  drops only its slot. Changing it is a new origin. The Lab uses it for
  Connect with Supabase (OAuth).
- Postgres and Supabase: `check` fails a mapped table with row-level
  security when the role neither owns it (or RLS is forced), nor is a
  superuser, nor has `BYPASSRLS`: the snapshot would silently copy only the
  rows its policies allow.
- `meili-connect` server binary: the HTTP API (`docs/openapi.yaml`) to check
  and discover sources, create, read, list, update and delete syncs, pause,
  resume, resync and release them, and read their status, daily progress and
  dead letters.
- Management auth: `LAB_SERVICE_TOKEN` with a required `X-Connect-Tenant-Id`,
  `ADMIN_API_KEY` (global, may act for a tenant), or open mode. Every route is
  tenant-scoped.
- Secrets are write-only: responses redact the Meilisearch key and the
  source's secrets as `"<redacted>"`; a `PUT` sending `"<redacted>"` keeps the
  stored value.
- Sources: Postgres, Supabase, MySQL/MariaDB, MongoDB, Kafka and DynamoDB.
- State in Postgres (`DATABASE_URL`, specs sealed with `CONNECT_SECRET_KEY`),
  or in memory for trials.
- Lab usage events: `usage.recorded` (`product: "connect"`) per tenant-owned
  sync, delivered from an outbox with signed batches (`LAB_URL`,
  `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET`).
- Metering of the contract's units (`docs/LAB_INTEGRATION.md`):
  `documents_streamed`, `documents_snapshot` (first sync or a user resync),
  `documents_resync` (a resync the engine started; free), `bytes` and
  `sync_seconds`, recorded per batch with the progress (additive migration).
  Usage events carry all five with `job_id` and a `postgres → products`
  description. Usage is durable end to end: recorded in the same transaction
  as the progress and moved into the outbox in one transaction.
- `GET /syncs/{id}/status` adds `documents` (per unit), `bytes`,
  `sync_seconds`, `lag` (`{"seconds", "bytes"}` while streaming, when the
  source can tell; `records` for Kafka), `lag_warning` and `dead_letters`.
- `GET /usage?from=&to=&group_by=day|sync`: the caller's units per day or
  per sync with totals, from Tinybird when configured, else from the state
  database.
- Out of credits (Lab mode): a team the Lab says cannot spend has its syncs
  held with `out_of_credits` (nothing torn down) and resumed when it can;
  new syncs start held; `release`, `resume` and `resync` answer `402
  out_of_credits` (`CREDITS_CHECK_INTERVAL_SECS`, `CREDITS_CACHE_FRESH_SECS`,
  `CREDITS_CACHE_STALE_SECS`).
- `GET /metrics` (Prometheus) behind `METRICS_TOKEN`: syncs per phase,
  documents per kind, bytes, batch time, sink and source errors by class,
  lag, dead letters, outbox backlog, Lab sender results.
- Tinybird analytics (`TINYBIRD_URL`, `TINYBIRD_TOKEN`): every usage flush is
  appended to `connect_usage` from its own outbox lane; `tinybird/` holds the
  data sources, a daily materialized view and the per-tenant usage pipe.
- Lag guard (`MAX_LAG_HOLD_BYTES`, `MAX_LAG_HOLD_SECS`): a sync past them
  that makes no progress gets a `lag_warning`; nothing is dropped
  automatically.
- Graceful shutdown on SIGTERM/SIGINT; syncs resume where they stopped on the
  next start, paused syncs stay paused.
- Dockerfile, `compose.yaml` with a demo origin, CI, and `scripts/e2e.sh`
  (Postgres and MongoDB scenarios against real containers).

### Fixed

- Postgres and Supabase: a table nobody has analyzed yet (created or
  filled recently, before autovacuum ran) now gets a row estimate from the
  live-row statistics, so the setup UI shows its first-sync cost instead
  of "Unknown". A partitioned table sums its partitions. A table whose
  statistics were reset still reads as unknown, never as empty.
- An idle sync no longer sends a `usage.recorded` event (and a Tinybird row)
  every flush interval carrying only its free `sync_seconds`: about 1,440
  zero-amount ledger rows a day per sync. Its seconds accumulate (none is
  lost) and go with its next billable usage, when it stops (pause, hold,
  failure, deletion, graceful shutdown), or after `USAGE_IDLE_FLUSH_SECS`
  (new, default 3600). `GET /usage` from Tinybird adds what is not sent yet.
  Migration `20261119000007` adds `usage_pending.pending_since`.
- `POST /sources/check` without `streams` is a connection-level pre-flight,
  as the Lab console's wizard sends it before the tables are picked: it no
  longer fails a healthy origin with `tables: no table is mapped`. Every
  connector reports the per-collection checks as one `ok` item (`tables`,
  `collections` or `topics`: "no table selected yet: table checks run once
  you pick them"); MongoDB and DynamoDB no longer warn either. Kafka accepts
  a check or a discovery without topics (it answered `422 source_config`).
  With streams nothing changes, and a sync still needs at least one.
- Every change to a sync (create, update, delete, pause, resume, resync,
  release) runs under the sync's lock on the row read inside it: a `PUT`
  racing a `DELETE` no longer restarts a deleted sync, and a `resume` racing a
  `PUT` no longer brings back the old spec.
- `PUT` acts on what changed: a new origin is torn down with the old spec
  first (`502 teardown_failed` and nothing changed when that fails, unless
  `?force=true`) and synced fresh; new streams or another Meilisearch URL
  clear the checkpoint so the data is backfilled; anything else restarts.
  A paused sync stays paused.
- A `DELETE` whose teardown fails now persists the sync as `paused` with
  the reason, so it stays stopped across restarts as the answer says.
- A stored sync whose spec cannot be unsealed no longer fails `GET /syncs` for
  the whole scope: it is listed (and returned by `GET /syncs/{id}`) flagged
  `unreadable`, refuses changes with `409 sync_unreadable`, and can be
  deleted with `?force=true`.
- Stopping one sync (update, pause, delete) waits at most
  `SHUTDOWN_GRACE_SECS` while holding its lock; a sync that does not stop is
  abandoned (it is already cancelled) instead of blocking every later call on
  it.
- `POST /syncs` is all or nothing: a sync that cannot be started is removed
  again and the error returned, instead of being kept and started
  unannounced at the next boot.
- Bounded growth: Lab events that fail are deferred with exponential backoff
  (additive migration: `attempts`, `next_attempt_at`), the sender backs off
  and rate-limits its logs, and past `LAB_OUTBOX_MAX_PENDING` unsent events
  usage stays counted in memory. Dead letters are capped per sync
  (`DEAD_LETTERS_MAX_PER_SYNC`, default 10,000; `dead_letters_overflow` in the
  status) and payloads over 64 KiB are truncated. Tenants own at most
  `MAX_SYNCS_PER_TENANT` syncs (default 20), `check`/`discover` run under
  global and per-tenant concurrency limits, and every request is bounded by
  `REQUEST_TIMEOUT_SECS`.
