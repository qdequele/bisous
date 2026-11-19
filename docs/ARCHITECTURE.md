# Architecture

meili-connect keeps Meilisearch indexes in sync with a database by following
its change stream (CDC). It runs standalone and as the **Connect** data plane
of the Meilisearch Lab, next to Gateway (Lumen), Pipelines (glutony) and
Crawler (scrapix).

## Guarantees

- **At-least-once, idempotent.** A checkpoint is saved only after Meilisearch
  reports the tasks of every change before it as succeeded. A crash replays
  from the last checkpoint; replays are harmless because the document id is a
  pure function of the source key (`core::document_id`).
- **Gapless first sync.** The stream position is captured before the snapshot
  reads anything (`Source::open` → `Opened::Fresh { start }`), so changes made
  during the snapshot are replayed after it.
- **The origin is never held hostage.** Replication slots and consumer offsets
  advance only to acknowledged checkpoints, and a stalled sync is detected and
  reported before it bloats the origin (slot lag guard).
- **Position loss is a normal event.** `SourceError::PositionLost` triggers a
  resync into shadow indexes that are swapped in when complete; search keeps
  serving the old data meanwhile.

## Crates

| Crate | Role |
|---|---|
| `core` | Contracts only: `Change`, `Checkpoint`, `Source`, `SourceFactory` (with `endpoints`), `Sink`, `StateStore`, `SyncSpec`, errors, `Secret`, `document_id`, and `HostPolicy`. No I/O but the DNS lookup of `HostPolicy`. |
| `sink` | `MeiliSink`: NDJSON batches, task polling, poison-document bisection, swap, settings copy. |
| `engine` | `run_sync`: drives one sync through its lifecycle; `Supervisor`: runs many, restarts with backoff. Depends on the three traits only. |
| `store` | Postgres (`sqlx`): sync specs with sealed secrets, `SyncState`, dead letters, progress counters, Lab usage outbox. Also an in-memory store for tests and single-node trials. |
| `sources/*` | One crate per origin, each exporting a `SourceFactory`. |
| `server` | Binary `meili-connect`: axum API, auth, supervisor wiring, Lab seams. |

Dependency rule: `sources/*`, `sink`, `engine` and `store` depend on `core`
and never on each other. Only `server` sees everything.

## The sync loop (`engine`)

```
load SyncState
open(resume = state.checkpoint)
  Resumed            -> stream
  Fresh{start}       -> snapshot (if capable) into live or shadow indexes
                        save checkpoint = start, phase = Streaming -> stream
  Err(PositionLost)  -> resync: shadow indexes, open(None), snapshot, swap
stream: read events into a batch until max_docs / max_bytes / max_delay
        or a Checkpoint event when the batch is non-empty and old enough
  map     Change -> DocOp per StreamMapping (select, rename, vector, id)
  coalesce by (index, id): last write wins, Patch folded into prior Upsert
  apply   sink.apply per index, in order
  dead-letter rejected docs
  save    state.checkpoint = last Checkpoint event fully covered by the batch
  ack     publish it on the ack channel
```

Rules the engine enforces:

- A `Checkpoint` event is only eligible once every change received before it
  has been applied. Changes after the last checkpoint in a batch are applied
  but not acknowledged until a later checkpoint covers them.
- `Truncate`, or a batch whose deletes exceed `mass_delete_guard` of the
  index's document count, sets the sync to `Held` with a message instead of
  applying. A user action releases it.
- Errors map to phases by variant only: `Transient` → retry with jittered
  exponential backoff (cap 60 s); `Paused` → `SourcePaused`, retry every
  60 s; `Config` → `Held`; `Fatal` → `Failed`; `PositionLost` → resync.
- Shadow index name: `{index}__connect_{sync_id_short}`. On completion:
  `copy_settings(live → shadow)` happens before the snapshot,
  `swap(live, shadow)` after, then `delete_index(shadow)`.

## Metering

The units of `docs/LAB_INTEGRATION.md` ("Pricing") are measured in the
engine and recorded with each batch's progress (`core::Progress`, carrying a
`core::Usage`):

- `documents_streamed` / `documents_snapshot` / `documents_resync`: the
  documents Meilisearch accepted (`ApplyReport.written + deleted`, never the
  rejected or skipped ones), by why they were written: the change stream, a
  first sync or a resync the user asked for, a resync the engine started
  after `PositionLost`. Why a resync runs is persisted with it
  (`SyncState::resync_reason`), so a crash in the middle of it does not
  change how the rest is metered.
- `bytes`: `ApplyReport.bytes`, the uncompressed wire form of each accepted
  document plus a separator, counted once whatever the retries.
- `sync_seconds`: time in `snapshotting` or `streaming`, accumulated by the
  run monitor and recorded by the supervisor every `seconds_interval` and at
  the end of each run. A paused or held sync accrues nothing.

## Sources

| Kind | Mechanism | Checkpoint | Snapshot |
|---|---|---|---|
| `postgres`, `supabase` | Logical replication, `pgoutput`, one slot + publication per sync | LSN | `COPY`/keyset reads in a `REPEATABLE READ` snapshot exported at slot creation |
| `mysql` | Row binlog (`binlog_row_image=FULL`), GTID | GTID set (file+pos fallback) | Consistent read with the GTID captured first |
| `mongodb` | Change streams, `fullDocument: updateLookup` | Resume token | `find` per collection after capturing the cluster time |
| `kafka` | Consumer, manual offset commit | `{topic: {partition: offset}}` | None |
| `dynamodb` | DynamoDB Streams, shards read in lineage order | `{shard_id: sequence_number}` | `Scan` |

Supabase is the Postgres connector plus setup rules: direct connection (not
the transaction pooler), IPv6, a paused project maps to `SourceError::Paused`,
an invalidated slot maps to `PositionLost`, and the `supabase_realtime`
publication is never reused.

## Meilisearch writes (`sink`)

- `Replace` → `POST /indexes/{uid}/documents`, `Merge` → `PUT`, `Delete` →
  `POST /indexes/{uid}/documents/delete-batch`. Consecutive ops of one kind
  form one request; order across kinds is preserved.
- NDJSON, gzip, payload split under the instance's payload limit.
- Every task is polled to a terminal state. A failed document task is
  bisected until the offending documents are isolated and returned as
  `rejected`.
- `ensure_index` refuses an existing index with another primary key.

## Tenancy and the Lab

Standalone works with none of the `LAB_*` variables set. With them:

- **Management auth** follows glutony: `Authorization: Bearer
  <LAB_SERVICE_TOKEN>` plus a required `X-Connect-Tenant-Id`, or
  `ADMIN_API_KEY`, or open mode for local development. Tokens are compared in
  constant time.
- **Every sync belongs to a tenant** (or the global scope standalone). All
  store queries are tenant-scoped.
- **Usage events** go to the Lab through an outbox: `usage.recorded` with
  `product: "connect"`, `operation: "sync"`, the five units of
  `docs/LAB_INTEGRATION.md`, `job_id` (the sync) and a description, signed
  `X-Lab-Signature: sha256=HMAC(LAB_INSTANCE_SECRET, "<ts>.<body>")`, batches
  of at most 500. The Lab must add `connect` to `lab-events.schema.json`
  first (Lab-owned contract). Usage is durable end to end
  (`store::UsageLedger`): `record_progress` adds a batch's units to a
  `usage_pending` row per lane (Lab, Tinybird) in the same transaction as
  the progress counters, and the flush moves pending rows into the lane's
  outbox in one transaction. The one double count left: a crash after
  Meilisearch accepted a batch and before its progress and checkpoint were
  saved replays (and records) that batch once more.
- **Secrets at rest** are sealed with ChaCha20-Poly1305
  (`version(1)‖nonce(12)‖ciphertext`, key from `CONNECT_SECRET_KEY`), same
  layout as glutony's connections.

## Host policy

Sources and destinations are tenant-supplied addresses that the server
connects to from inside its network. `core::HostPolicy`
(`CONNECT_HOST_POLICY`: `public` by default with `LAB_SERVICE_TOKEN`, `any`
standalone, `allowlist`) decides which addresses may be used; every resolved
address of a name must pass. The state database is refused as a source origin
in every mode.

- Each connector declares where it connects: `SourceFactory::endpoints`
  (Postgres/MySQL host, every MongoDB seed host and the hosts of a
  `mongodb+srv` SRV record, every Kafka bootstrap server, DynamoDB's
  `endpoint_url`).
- The server vets them at `check`, `discover`, `POST` and `PUT`, and wraps
  the registry (`server::guard::GuardedSources`) so that every source the
  engine builds, one per attempt, vets them again before its first call
  that may connect. A refusal is a `Config` error: the sync is held.
- The sink resolves through the policy (a custom reqwest resolver), so its
  connections go to the vetted addresses only; literal IPs are vetted when
  the sink is built; no proxy, no redirects.
- Errors of a tenant's source and sink, and of Lab calls, are reduced to
  their class and a fixed description (`server::guard`).

Residual risk: database drivers resolve DNS themselves, so between vetting
and the driver's connect a rebinding window remains for sources (not for the
sink); Kafka brokers advertised in metadata and MongoDB members discovered
from the seeds are not vetted. A hosted deployment must also restrict egress
at the network level.

## Changes to a sync (`server::manager`)

Every mutation (create, update, delete, pause, resume, resync, release) is
one `SyncManager` entry point: it takes the sync's lock, reads the stored row
again inside it, writes, then starts or stops the supervisor. The HTTP
handler runs it detached from the request, so a client that hangs up (or the
request timeout) never leaves a change half done. Stopping a sync inside the
lock is bounded by `SHUTDOWN_GRACE_SECS`.

`PUT` diffs the old and the new spec inside the lock: a new origin is torn
down with the old spec first (nothing changes when that fails, unless
`force`), then the checkpoint is cleared; new streams or another Meilisearch
URL clear the checkpoint (full snapshot, into shadow indexes where the live
ones hold data); anything else restarts. A paused sync stays paused.

Bounds: `MAX_SYNCS_PER_TENANT`, probe slots (global and per tenant), a
request timeout, dead letters per sync (enforced by the store, overflow
counted in `sync_progress.dead_letters_dropped`), and the Lab outbox
(per-event backoff, sender backoff, backlog cap on the enqueue side).

## Not in the first version

Resumable snapshots, joins across tables, transforms beyond field selection
and renames, a web UI (the Lab console owns it), Supabase OAuth setup.
