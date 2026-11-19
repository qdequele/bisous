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

### Added

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
- Graceful shutdown on SIGTERM/SIGINT; syncs resume where they stopped on the
  next start, paused syncs stay paused.
- Dockerfile, `compose.yaml` with a demo origin, CI, and `scripts/e2e.sh`
  (Postgres and MongoDB scenarios against real containers).

### Fixed

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
