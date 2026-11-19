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
