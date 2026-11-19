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
