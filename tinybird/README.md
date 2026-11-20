# Tinybird resources for meili-connect usage analytics

Optional. With `TINYBIRD_URL` and `TINYBIRD_TOKEN` set, meili-connect sends
every usage flush (the same units the Lab is billed from,
`docs/LAB_INTEGRATION.md`) to Tinybird as well, from its own outbox lane:
at least once, deduplicated by `event_id`, with its own backoff and backlog
cap, so the Lab failing never holds Tinybird back (and the other way round).
The Lab's credits ledger stays the billing source of truth; Tinybird is for
dashboards and for checking the ledger against what the engine metered.

```
tinybird/
├── datasources/
│   ├── connect_usage.datasource          # raw rows, one per sync per flush (ReplacingMergeTree)
│   └── connect_usage_daily.datasource    # daily rollup per tenant / sync (materialized)
├── pipes/
│   ├── connect_usage_daily_mv.pipe       # MATERIALIZED: connect_usage → connect_usage_daily
│   └── connect_tenant_usage.pipe         # ENDPOINT: one tenant's usage per day and sync
└── README.md
```

## The rule: column names never change

The columns of `connect_usage` are the keys the engine writes
(`crates/server/src/usage.rs`, `tinybird_row`) and, for the units, the unit
names of the Lab event:

| column | type | |
|---|---|---|
| `event_id` | String | the outbox row id (UUIDv7), stable across retries |
| `occurred_at` | DateTime64(3) | when the usage was enqueued (RFC 3339, `Z`) |
| `tenant_id` | String | the Lab account id; `""` for the global scope |
| `sync_id` | String | |
| `source_kind` | LowCardinality(String) | `postgres`, `supabase`, `mysql`, `mongodb`, `kafka`, `dynamodb`, `firestore` |
| `operation` | LowCardinality(String) | `sync` |
| `documents_streamed` | UInt64 | writes and deletes applied from the change stream |
| `documents_snapshot` | UInt64 | written by a first sync or a resync the user asked for |
| `documents_resync` | UInt64 | rewritten by a resync the engine started (free) |
| `bytes` | UInt64 | uncompressed payload bytes sent to Meilisearch |
| `sync_seconds` | UInt64 | seconds spent snapshotting or streaming |

**Never rename, retype or reorder a column.** Dashboards, the
`connect_tenant_usage` endpoint and `GET /usage` depend on them, and a row
whose shape the data source does not expect lands in
`connect_usage_quarantine` (the engine logs it as an error and counts it in
`connect_tinybird_events_sent_total{result="rejected"}`; it does not retry
it). A real change is a new data source plus a backfill, deployed before the
engine writes the new shape. Adding a column is the only in-place change,
and it must have a default so older engines keep working.

## Deploy

With the Tinybird Forward CLI (`tb`):

```bash
curl -LsSf https://tbrd.co/fwd | sh     # or: uv tool install tinybird
cd tinybird
tb --local build                         # validate against Tinybird Local
tb --local deploy
tb login && tb --cloud deploy            # the real workspace
```

`tb build` writes a `fixtures/` folder and `tb login` a `.tinyb` file (it
holds a token): both are gitignored.

## Token

Every deployment mints `connect_engine`: APPEND on `connect_usage` and READ
on `connect_tenant_usage`. It is the engine's `TINYBIRD_TOKEN` (one token for
both: the engine appends its rows and serves `GET /usage` from the pipe).

```bash
tb --cloud token ls
tb --cloud token copy connect_engine
```

`TINYBIRD_URL` is the workspace's **regional** API host (`tb --cloud info`
prints it), e.g. `https://api.eu-central-1.aws.tinybird.co`; the wrong host
answers 401/403.

## Check rows are landing

```bash
tb --cloud sql "SELECT count() FROM connect_usage"
tb --cloud sql "SELECT tenant_id, sum(documents_streamed), sum(bytes) FROM connect_usage FINAL GROUP BY tenant_id"
tb --cloud sql "SELECT count() FROM connect_usage_quarantine"    # schema drift alarm
tb --cloud endpoint data connect_tenant_usage --tenant_id <account> --date_from 2026-11-01 --date_to 2026-11-30
```

A manual row, the exact request the engine makes:

```bash
TS=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
printf '{"event_id":"smoke-1","occurred_at":"%s","tenant_id":"smoke","sync_id":"s","source_kind":"postgres","operation":"sync","documents_streamed":1,"documents_snapshot":0,"documents_resync":0,"bytes":100,"sync_seconds":1}\n' "$TS" |
  curl -X POST "$TINYBIRD_URL/v0/events?name=connect_usage&wait=true" \
       -H "Authorization: Bearer $TINYBIRD_TOKEN" -H "Content-Type: application/x-ndjson" --data-binary @-
```

It answers `{"successful_rows":1,"quarantined_rows":0}`.

## Duplicates

The engine sends a row again when Tinybird's acknowledgement is lost; the
copy is byte-identical (same `event_id`, same `occurred_at`). `connect_usage`
is a `ReplacingMergeTree` sorted on `tenant_id, occurred_at, event_id`, so
copies collapse on merge: read it with `FINAL` (the endpoint does) for exact
sums. `connect_usage_daily` is fed by a materialized view, a trigger on
insert that does not see that collapse: it is for dashboards, not for
reconciliation.

## Which table do I read?

| | Fresh | Deduplicated | Use it for |
|---|---|---|---|
| `connect_usage` | real time | with `FINAL` | reconciliation, audits, `GET /usage` |
| `connect_usage_daily` | real time | no | dashboards |
