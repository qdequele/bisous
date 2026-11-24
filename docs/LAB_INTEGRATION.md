# Connect in the Meilisearch Lab: integration contract

This is the cross-repo contract between meili-connect (repo `qdequele/bisous`),
the Lab (`meilisearch-lab`) and the deploy box (`qdq-server`). Each repo
implements its side of this page.

## Names

| Thing | Value |
|---|---|
| Product label, nav category | **Connect** |
| Instance kind, product slug, event `product` | `connect` |
| Console route prefix | `/connect` |
| Engine repo | `qdequele/bisous` |
| Binary, image | `meili-connect`, `ghcr.io/qdequele/bisous` |
| Tenant header (Lab → engine) | `X-Connect-Tenant-Id: <account id>` |
| Lab-managed Meilisearch key name | `lab-connect` |
| Deploy folder in qdq-server | `connect/` |

Connect is hosted by Meilisearch only, like Gateway and Pipelines: every team
gets one built-in instance row served by a `hosted_engines` row.

## Auth

- Lab → engine: `Authorization: Bearer <hosted engine credential>` (the
  engine's `LAB_SERVICE_TOKEN`) plus `X-Connect-Tenant-Id`. The engine refuses
  the token without the header.
- Engine → Lab: `X-Lab-Instance-Id` + `Authorization: Bearer
  <LAB_INSTANCE_SECRET>` for `/internal/*`; event batches are HMAC-signed
  (`X-Lab-Timestamp`, `X-Lab-Signature: sha256=HMAC(secret, "<ts>.<body>")`).

## Destination

A tenant never sends a Meilisearch URL or key to the hosted engine. The
console sends `destination: {"instance": "<slug of a team Meilisearch
instance>"}` on `POST /syncs` and `PUT /syncs/{id}`; the Lab's proxy replaces
it with `{"url", "api_key"}`, where the key is a scoped `lab-connect` key the
Lab mints on that instance (documents, indexes, settings, tasks, swap actions
on all indexes; same lifecycle as `lab-pipelines`). On responses the Lab maps
the URL back to the instance slug. Standalone meili-connect keeps the
`{"url", "api_key"}` form.

When that instance has Lumen embedders (the console's embedder wizard), the
Lab adds `"embedder_keys": {"<Gateway public URL>/v1/embeddings": "<the
instance's lab-embedder key>"}` to the destination: the URL those embedders
call, exactly as the wizard writes it in the index settings, and the key it
writes there. Meilisearch never gives that key back, so without it an index
with such an embedder cannot be rebuilt in a shadow index (a resync) and the
sync is held (`destination_rejected`). The engine writes the key only into
an embedder whose `url` is exactly that one; an embedder calling anything
else with a key of its own (a team's own provider key) is still refused.
The key is redacted in every answer like `api_key`, under its URL: the Lab
reads `destination.embedder_keys` in the answer to a sync write to know the
engine took the key (an engine that predates the field ignores it and
answers without). The Lab sends the destination again when the embedder key
or the Gateway's address changes, as it does for `lab-connect`, and only to
its hosted engine.

## Supabase OAuth

"Connect with Supabase" is a Lab feature; the engine only sees a plain
Postgres spec. Design: [SUPABASE_OAUTH.md](SUPABASE_OAUTH.md).

- **Lab-only source form.** The console sends `{"kind": "supabase",
  "config": {"project": "<supabase_projects id>"}}` on `/sources/check`,
  `/sources/discover`, `POST /syncs` and `PUT /syncs/{id}`. The Lab proxy
  rewrites it into a full spec (host, provisioned role and password,
  `sslmode: require`) with `"manage_publication": false` and the
  `publication_name` it created, and maps a matching spec back to
  `{"project": id}` on answers. The engine never accepts the `project` form
  itself, and never sees the OAuth tokens.
- **The Lab owns the role and the publication.** It provisions the
  least-privilege role (`LOGIN REPLICATION`, `BYPASSRLS` when possible), and
  creates, alters and drops `meili_connect_lab_<random>` `FOR TABLE` the
  mapped tables as `postgres` through the Supabase Management API: only a
  table's owner can add it to a publication. It grants `SELECT` on the
  tables before forwarding a create or update.
- **The engine only uses the publication** (`manage_publication: false`):
  `check` fails when it is missing or does not cover a mapped table, `open`
  refuses it with a config error, and it is never created, altered or
  dropped. `DELETE /syncs/{id}` drops only the sync's slot; the Lab drops the
  publication after the `204`. Keeping the same `publication_name` on a
  `PUT` keeps the origin (no fresh snapshot); changing it or
  `manage_publication` is a new origin.
- `check` also fails a table with row-level security that the role does not
  bypass (snapshot would copy only the rows the policies allow).

## Pricing

Two meters, both reported by the engine and priced by the Lab
(`saas/config/pricing.yml`, then `price_versions`):

| Unit | Meaning | Launch price |
|---|---|---|
| `documents_streamed` | Document writes and deletes applied from the change stream | $1.00 per million |
| `documents_snapshot` | Documents written by a first sync or a resync the user asked for | $0.20 per million |
| `documents_resync` | Documents rewritten by a resync the system started (position lost on the origin) | Free, reported |
| `bytes` | Uncompressed payload bytes sent to Meilisearch | The Lab's bandwidth price, $0.10 per GB |
| `sync_seconds` | Seconds a sync spent in `snapshotting` or `streaming` | Free at launch, reported |

Rules:

- Only documents Meilisearch accepted are counted. Rejected documents, skipped
  records and replays after a crash that the engine can identify as replays
  of an already-counted batch are not billed. (A replay after a crash between
  the write and the checkpoint is counted twice at most once per crash; this
  is accepted and bounded by one batch.)
- A paused or held sync costs nothing.
- Prices are sub-credit per document (1 credit = $0.01), so the Lab prices
  per unit in micro-USD and carries fractions per account, as it already does
  for pass-through provider cost.
- A team out of credits: the Lab answers `can_spend: false` on the account
  lookup; the engine sets that team's syncs to `Held` with the message
  `out_of_credits` and resumes them when credits are back. Nothing is torn
  down, so the origin keeps the position (bounded by the lag guard).

Worked examples: a first sync of 1 M rows of 1 KB costs $0.20 + $0.10 = $0.30.
A table taking 100 k changes a day at 1 KB costs about $3.00 + $0.30 a month.

## Usage event

One `usage.recorded` event per sync, enqueued at a flush
(`USAGE_FLUSH_INTERVAL_SECS`, default 60 s) when its pending usage is due:

- **activity**: a billable unit (`documents_streamed`, `documents_snapshot`,
  `documents_resync`, `bytes`) is pending. The event carries the
  `sync_seconds` accumulated so far too.
- **idle**: only `sync_seconds` (free) is pending, and the first of them was
  recorded at least `USAGE_IDLE_FLUSH_SECS` (default 3600 s) ago, so after
  the sync's previous event. An idle sync sends about one event an hour
  instead of one a minute (one ledger row each). The age is stored with the
  pending row and survives restarts.
- **stop**: the sync was paused, held (by a guard or out of credits),
  failed or deleted: its tail goes at once or at the next flush. A graceful
  shutdown sends everything pending.

No second is lost: they accumulate in the state database until they go.
`GET /usage` and `GET /syncs/{id}/status` count them as soon as they are
recorded, so at any time they equal the events sent plus what is pending.

```json
{
  "id": "<UUIDv7>",
  "type": "usage.recorded",
  "occurred_at": "2026-11-19T10:00:00Z",
  "account_id": "<tenant>",
  "api_key_id": null,
  "product": "connect",
  "data": {
    "operation": "sync",
    "units": {
      "documents_streamed": 1200,
      "documents_snapshot": 0,
      "documents_resync": 0,
      "bytes": 1482113,
      "sync_seconds": 60
    },
    "job_id": "<sync id>",
    "description": "postgres → products"
  }
}
```

The Lab owns `lab-events.schema.json`: it adds `connect` to `product` and the
`sync` operation with those unit names first; the engine vendors the schema
(`contracts/vendor/lab/`, byte copy, drift-checked in CI) and its tests
validate every event it builds against it.

## Sync state event

One `sync.state_changed` event when a tenant's sync changes phase, through
the same signed outbox as usage (at least once, deduplicated by id). Global
syncs and a server without the `LAB_*` event variables send none. The Lab
turns them into the team's emails; the Lab must accept the type before an
engine sends it (it does since the schema has `syncStateData`).

- **When**: every change of phase the engine or the manager persists, and
  the deletion of the sync (`to: "deleted"`). A save of the same phase
  (checkpoints, a retried error's message) is not a change. The end of a
  snapshot (`snapshotting` → `streaming`) is always one, `resync: true` when
  it was a resync.
- **No floods**: a change waits until its phase has held for
  `SYNC_EVENTS_SETTLE_SECS` (default 30 s). A newer change replaces a waiting
  one, and a change back to the phase last reported cancels it: a sync
  flapping between `streaming` and `source_paused` every few seconds sends
  nothing until it stays in one, then one event. At most one event per sync
  per window; a deletion goes at the next tick (5 s) and drops a change
  still waiting. A graceful shutdown sends what waits; a crash loses it (the
  Lab learns the phase at the next change).
- **Id**: the outbox row id, a UUIDv7 made when the change is enqueued and
  kept across retries. `occurred_at` is when the change happened.
- **Phases**: `pending`, `snapshotting`, `streaming`, `paused` (by the
  user), `source_paused`, `held`, `failed`, `deleted`. `from` is the phase
  right before `to`.
- **reason** (with `message`, for `paused`, `source_paused`, `held`,
  `failed`): a stable code from one mapping (`crates/server/src/reason.rs`).
  `message` is what `GET /syncs/{id}/status` shows the tenant: the coarse,
  fixed description of the error class, never a host, driver text or what a
  remote server sent (cut at 1000 bytes). The status carries the same
  `reason` next to it while the sync is stopped, so the Lab can act on a
  sync it reads (a `destination_auth` hold gets a fresh `lab-connect` key)
  without waiting for the event. What the remote server really answered is
  in the engine's log only: Meilisearch's refusals at `warn`, the rest at
  `debug`.

| reason | When |
|---|---|
| `insufficient_credits` | The team is out of credits (message `out_of_credits`) |
| `source_auth` | The source refused the credentials |
| `source_unreachable` | The source host does not resolve, refuses or does not answer; the source is paused |
| `source_config` | Publication, replication slot, privileges, tables or prerequisites; a host-policy refusal |
| `invalid_config` | The source configuration or the sync spec is invalid |
| `resync_budget_spent` | The position was lost again after the allowed resyncs in a row |
| `destination_auth` | Meilisearch refused the API key |
| `destination_rejected` | Meilisearch refused the index (primary key) or the documents, or an index that holds documents cannot be rebuilt aside because its embedder has an API key the destination's `embedder_keys` does not name (its own message says to empty the index or remove the key) |
| `destination_unreachable` | Meilisearch does not answer (also a batch held after failing 10 times) |
| `delete_guard` | A truncate or a mass delete was held back |
| `internal` | Anything else |

```json
{
  "id": "<UUIDv7>",
  "type": "sync.state_changed",
  "occurred_at": "2026-11-20T10:00:00.000Z",
  "account_id": "<tenant>",
  "api_key_id": null,
  "product": "connect",
  "data": {
    "sync_id": "<sync id>",
    "sync_name": "postgres → products",
    "from": "streaming",
    "to": "held",
    "reason": "source_auth",
    "message": "source configuration error: the source refused the credentials: check the user and the password or key"
  }
}
```

## Sync health event

One `sync.health_changed` event when a tenant's sync gets dangerous for its
source, per cause, and one when it is fine again. Same outbox, signing,
ids, Lab-tenants-only rule and settle window as the state event.

- **`lag`**: a *streaming* sync is behind its source by
  `SYNC_LAG_WARN_SECS` (default 900 s), from the lag the source already
  reports (no query; looked at every 30 s). Not measured in other phases:
  the state event covers a stopped sync, and a lag warning stands until the
  sync streams under the threshold again.
- **`wal_retained`**: the WAL the replication slot of a Postgres or Supabase
  sync keeps on the customer's database
  (`pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)`) reaches
  `SYNC_WAL_WARN_BYTES` (default 1 GiB), or half of the source's positive
  `max_slot_wal_keep_size` when that is lower (`source_limit_bytes`; past it
  Postgres invalidates the slot and the sync resyncs). Measured every
  `SYNC_HEALTH_INTERVAL_SECS` (default 300 s; `0` turns the probe off) in
  **every** phase where the slot exists: streaming, snapshotting, paused,
  held, failed, source paused. A stopped sync is the dangerous case: its
  slot keeps WAL until the disk fills. Each measure is one query on its own
  short-lived connection with the sync's credentials, behind the host
  policy, at most 4 at a time and 20 s each; it never touches the sync's
  own sessions. A measure that fails is logged and skipped (an unreachable
  source is the state event's business). A slot that is gone counts as
  0 bytes. Other source kinds are not measured (MySQL binlog retention is
  out of scope).
- **Status**: `warning` from the threshold, `ok` again only under 80 % of
  it (no flapping around the threshold). `threshold` is the one that
  applied: seconds for `lag`, bytes for `wal_retained`. `state` is the
  sync's phase when it was measured.
- **When**: on a change of status only, once it has held for
  `SYNC_EVENTS_SETTLE_SECS`; a change back to what was reported last
  cancels it. A deleted sync drops what waits.
- **Restarts**: the status is kept in memory and starts `ok`. A sync still
  over its threshold after a restart reports `warning` once more (the Lab
  dedupes repeated warnings); one that recovered while the server was down
  sends no `ok`.
- **Privacy**: numbers only. No host, slot name or credentials.

```json
{
  "id": "<UUIDv7>",
  "type": "sync.health_changed",
  "occurred_at": "2026-11-20T10:00:00.000Z",
  "account_id": "<tenant>",
  "api_key_id": null,
  "product": "connect",
  "data": {
    "sync_id": "<sync id>",
    "sync_name": "postgres → products",
    "status": "warning",
    "cause": "wal_retained",
    "state": "paused",
    "retained_wal_bytes": 3221225472,
    "threshold": 536870912,
    "source_limit_bytes": 1073741824
  }
}
```

## Engine API the Lab relies on

Everything in `docs/openapi.yaml`, plus:

- `GET /usage?from=&to=&group_by=day|sync`: the units above per day and per
  sync for the calling tenant (from Tinybird when configured, else from the
  state database). Drives the console's Usage page.
- `GET /syncs/{id}/status` includes `lag` (`{"seconds", "bytes"}` when the
  source can tell), `documents` totals per unit, `bytes`, and
  `dead_letters`.
- `GET /sources/dynamodb/trust`: for the calling tenant, `{"external_id",
  "principal" (string or null), "trust_policy" (IAM JSON), "note" (string or
  null)}`. The console shows it in the DynamoDB `assume_role` form and never
  sends `external_id` (the engine sets it; another value is
  `422 external_id_not_allowed`).
- `GET /network`: `{"egress_ips": [...]}`, the addresses the database
  allow-lists must admit (`CONNECT_EGRESS_IPS`; empty when unset). The
  console shows them in the hosted-database setup guides.
- `provider` in a `postgres` or `mysql` config: the console's hosted-database
  tiles (Neon, RDS, Aurora, Cloud SQL, Azure) set it so failed checks name
  the provider's fix. Engine first: an engine without it refuses the field.
- Firestore (`"kind": "firestore"`): the console sends the service account
  key file as text in `config.service_account` (the engine redacts it on
  every answer; a `PUT` may send `"<redacted>"` back while `project_id`,
  `database_id` and `emulator_host` are unchanged), an optional
  `project_id`, and a `database_id`. It never offers `emulator_host` (a
  hosted engine's host policy refuses it anyway). `discover` names
  top-level collections and `**/<id>` collection groups, with
  `estimated_rows` when Firestore counts them within 5 s; a subcollection
  path (`users/alice/orders`) can be typed. The role to grant is
  `roles/datastore.viewer`.
- `GET /metrics`: Prometheus text, authenticated with `METRICS_TOKEN` (never
  the tenant tokens), no tenant labels beyond counts.
- `GET /health`.

## Telemetry and analytics

- **Monitoring** (operators): Prometheus `/metrics` scraped by the box, JSON
  logs to Loki with alert rules (sync failed, held, lag above threshold,
  outbox backlog, sink error rate, process restarts).
- **Analytics and billing reconciliation**: every usage flush is also sent to
  Tinybird (`tinybird/` in the engine repo: one append-only datasource with
  stable column names, a daily materialised view, a per-tenant pipe),
  deduplicated by event id, at-least-once. The Lab's credits ledger stays the
  billing source of truth; Tinybird is for dashboards and for checking the
  ledger against what the engine metered.
- **Product analytics** (console): the Lab's existing analytics events for
  the Connect pages (sync created, first sync completed, sync held, resync).
