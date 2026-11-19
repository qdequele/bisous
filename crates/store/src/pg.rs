//! Postgres store over `sqlx`. Runtime-checked queries only, so the crate
//! builds without a database. Migrations in `crates/store/migrations` are
//! applied at startup; they are forward-only and additive.

use std::str::FromStr;
use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use meili_connect_core::{
    Checkpoint, DeadLetter, Secret, StateStore, StoreError, SyncPhase, SyncSpec, SyncState,
};
use sqlx::Row;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions, PgRow};
use sqlx::types::Json;
use uuid::Uuid;

use crate::error::from_sqlx;
use crate::{
    BootSync, DailyProgress, DeadLetterLimits, DeadLetterPage, LabOutbox, OutboxEvent, OutboxLease,
    RepoError, RetryBackoff, SecretKey, StoredDeadLetter, StoredSync, SyncRepository, SyncRow,
    UnreadableSync, bound_letter, to_i64, to_u64, validate_spec,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Rows deleted per statement by `prune_sent_events`.
const PRUNE_BATCH: i64 = 1_000;

/// Pick then lease. The `MATERIALIZED` CTE runs once: an `IN (SELECT …
/// LIMIT n FOR UPDATE SKIP LOCKED)` subquery may be re-run per outer row
/// under a nested-loop plan and lease more than `n` rows.
const LEASE_SQL: &str = "WITH picked AS MATERIALIZED (
        SELECT id FROM lab_events_outbox
        WHERE sent_at IS NULL AND (leased_until IS NULL OR leased_until <= now())
          AND (next_attempt_at IS NULL OR next_attempt_at <= now())
        ORDER BY created_at, id
        LIMIT $1
        FOR UPDATE SKIP LOCKED
     )
     UPDATE lab_events_outbox o
     SET leased_until = now() + make_interval(secs => $2), lease_token = $3
     FROM picked WHERE o.id = picked.id
     RETURNING o.id, o.tenant_id, o.payload, o.created_at";

pub struct PgStore {
    pool: PgPool,
    key: SecretKey,
    limits: DeadLetterLimits,
}

impl std::fmt::Debug for PgStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgStore")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl PgStore {
    /// Connect to `database_url`, apply pending migrations and return the
    /// store. `key` seals sync specs at rest.
    pub async fn connect(database_url: &Secret, key: SecretKey) -> Result<Self, StoreError> {
        // The parse error can echo the URL, password included: drop it.
        let options = PgConnectOptions::from_str(database_url.expose())
            .map_err(|_| StoreError::Fatal("DATABASE_URL is not a valid Postgres URL".into()))?;
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(Duration::from_secs(10))
            .connect_with(options)
            .await
            .map_err(|e| StoreError::from(from_sqlx(e)))?;
        Self::from_pool(pool, key).await
    }

    /// Wrap an existing pool, applying pending migrations first.
    pub async fn from_pool(pool: PgPool, key: SecretKey) -> Result<Self, StoreError> {
        MIGRATOR.run(&pool).await.map_err(|e| match e {
            // A connection lost while migrating is worth a retry.
            sqlx::migrate::MigrateError::Execute(e) => match from_sqlx(e) {
                RepoError::Transient(m) => StoreError::Transient(format!("migrations: {m}")),
                other => StoreError::Fatal(format!("migrations failed: {other}")),
            },
            other => StoreError::Fatal(format!("migrations failed: {other}")),
        })?;
        Ok(Self {
            pool,
            key,
            limits: DeadLetterLimits::default(),
        })
    }

    /// Bound what one sync may keep in the dead-letter table.
    pub fn with_dead_letter_limits(mut self, limits: DeadLetterLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    fn row_to_sync(&self, row: &PgRow) -> Result<SyncRow, RepoError> {
        let id: String = row.try_get("id").map_err(from_sqlx)?;
        let tenant: Option<String> = row.try_get("tenant_id").map_err(from_sqlx)?;
        let sealed: Vec<u8> = row.try_get("spec_sealed").map_err(from_sqlx)?;
        let created_at = row.try_get("created_at").map_err(from_sqlx)?;
        let updated_at = row.try_get("updated_at").map_err(from_sqlx)?;
        Ok(match self.unseal(&id, tenant.as_deref(), &sealed) {
            Ok(spec) => SyncRow::Readable(StoredSync {
                spec,
                created_at,
                updated_at,
            }),
            Err(e) => {
                tracing::warn!(sync_id = %id, error = %e, "a stored sync cannot be read");
                SyncRow::Unreadable(UnreadableSync {
                    name: row.try_get("name").map_err(from_sqlx)?,
                    kind: row.try_get("kind").map_err(from_sqlx)?,
                    id,
                    tenant,
                    error: e.to_string(),
                    created_at,
                    updated_at,
                })
            }
        })
    }

    /// Unseal and check the spec belongs to its row, so a sealed blob
    /// copied onto another row (another tenant) is refused.
    fn unseal(&self, id: &str, tenant: Option<&str>, sealed: &[u8]) -> Result<SyncSpec, RepoError> {
        let spec = self.key.open_spec(sealed)?;
        if spec.id != id || spec.tenant.as_deref() != tenant {
            return Err(RepoError::Fatal(format!(
                "sealed spec of sync {id} does not match its row"
            )));
        }
        Ok(spec)
    }

    async fn ensure_owned(&self, tenant: Option<&str>, id: &str) -> Result<(), RepoError> {
        let found =
            sqlx::query("SELECT 1 FROM syncs WHERE id = $1 AND tenant_id IS NOT DISTINCT FROM $2")
                .bind(id)
                .bind(tenant)
                .fetch_optional(&self.pool)
                .await
                .map_err(from_sqlx)?;
        match found {
            Some(_) => Ok(()),
            None => Err(RepoError::NotFound(format!("sync {id}"))),
        }
    }
}

fn phase_str(p: SyncPhase) -> &'static str {
    match p {
        SyncPhase::Pending => "pending",
        SyncPhase::Snapshotting => "snapshotting",
        SyncPhase::Streaming => "streaming",
        SyncPhase::Paused => "paused",
        SyncPhase::SourcePaused => "source_paused",
        SyncPhase::Held => "held",
        SyncPhase::Failed => "failed",
    }
}

fn parse_phase(s: &str) -> Result<SyncPhase, StoreError> {
    Ok(match s {
        "pending" => SyncPhase::Pending,
        "snapshotting" => SyncPhase::Snapshotting,
        "streaming" => SyncPhase::Streaming,
        "paused" => SyncPhase::Paused,
        "source_paused" => SyncPhase::SourcePaused,
        "held" => SyncPhase::Held,
        "failed" => SyncPhase::Failed,
        other => return Err(StoreError::Fatal(format!("unknown sync phase {other:?}"))),
    })
}

fn store_err(e: sqlx::Error) -> StoreError {
    from_sqlx(e).into()
}

#[async_trait]
impl StateStore for PgStore {
    async fn load(&self, sync_id: &str) -> Result<Option<SyncState>, StoreError> {
        let row = sqlx::query(
            "SELECT sync_id, phase, checkpoint, message, resyncing, updated_at
             FROM sync_state WHERE sync_id = $1",
        )
        .bind(sync_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(store_err)?;
        let Some(row) = row else { return Ok(None) };
        let phase: String = row.try_get("phase").map_err(store_err)?;
        let checkpoint: Option<Json<serde_json::Value>> =
            row.try_get("checkpoint").map_err(store_err)?;
        Ok(Some(SyncState {
            sync_id: row.try_get("sync_id").map_err(store_err)?,
            phase: parse_phase(&phase)?,
            checkpoint: checkpoint.map(|Json(v)| Checkpoint(v)),
            message: row.try_get("message").map_err(store_err)?,
            resyncing: row.try_get("resyncing").map_err(store_err)?,
            updated_at: row.try_get("updated_at").map_err(store_err)?,
        }))
    }

    async fn save(&self, state: &SyncState) -> Result<(), StoreError> {
        // One autocommitted statement: durable once it returns.
        sqlx::query(
            "INSERT INTO sync_state (sync_id, phase, checkpoint, message, resyncing, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (sync_id) DO UPDATE SET
                phase = EXCLUDED.phase,
                checkpoint = EXCLUDED.checkpoint,
                message = EXCLUDED.message,
                resyncing = EXCLUDED.resyncing,
                updated_at = EXCLUDED.updated_at",
        )
        .bind(&state.sync_id)
        .bind(phase_str(state.phase))
        .bind(state.checkpoint.as_ref().map(|c| Json(&c.0)))
        .bind(state.message.as_deref())
        .bind(state.resyncing)
        .bind(state.updated_at)
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }

    async fn push_dead_letters(&self, letters: &[DeadLetter]) -> Result<(), StoreError> {
        if letters.is_empty() {
            return Ok(());
        }
        let mut by_sync: std::collections::BTreeMap<&str, Vec<DeadLetter>> =
            std::collections::BTreeMap::new();
        for l in letters {
            by_sync
                .entry(l.sync_id.as_str())
                .or_default()
                .push(bound_letter(l, &self.limits));
        }
        // All or nothing.
        let mut tx = self.pool.begin().await.map_err(store_err)?;
        for (sync_id, group) in by_sync {
            let stored: i64 =
                sqlx::query_scalar("SELECT count(*) FROM dead_letters WHERE sync_id = $1")
                    .bind(sync_id)
                    .fetch_one(&mut *tx)
                    .await
                    .map_err(store_err)?;
            let room = self.limits.max_per_sync.saturating_sub(to_u64(stored));
            let keep = group.len().min(usize::try_from(room).unwrap_or(usize::MAX));
            let dropped = group.len() - keep;
            if keep > 0 {
                let kept = &group[..keep];
                sqlx::query(
                    "INSERT INTO dead_letters
                        (sync_id, index_uid, document_id, payload, reason, created_at)
                     SELECT * FROM UNNEST($1::text[], $2::text[], $3::text[], $4::jsonb[],
                                          $5::text[], $6::timestamptz[])",
                )
                .bind(kept.iter().map(|l| l.sync_id.clone()).collect::<Vec<_>>())
                .bind(kept.iter().map(|l| l.index.clone()).collect::<Vec<_>>())
                .bind(
                    kept.iter()
                        .map(|l| l.document_id.clone())
                        .collect::<Vec<_>>(),
                )
                .bind(kept.iter().map(|l| l.payload.clone()).collect::<Vec<_>>())
                .bind(kept.iter().map(|l| l.reason.clone()).collect::<Vec<_>>())
                .bind(kept.iter().map(|l| l.created_at).collect::<Vec<_>>())
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
            }
            if dropped > 0 {
                sqlx::query(
                    "INSERT INTO sync_progress (sync_id, day, dead_letters_dropped)
                     VALUES ($1, $2, $3)
                     ON CONFLICT (sync_id, day) DO UPDATE SET
                        dead_letters_dropped =
                            sync_progress.dead_letters_dropped + EXCLUDED.dead_letters_dropped",
                )
                .bind(sync_id)
                .bind(Utc::now().date_naive())
                .bind(to_i64(dropped as u64))
                .execute(&mut *tx)
                .await
                .map_err(store_err)?;
            }
        }
        tx.commit().await.map_err(store_err)?;
        Ok(())
    }

    async fn record_progress(
        &self,
        sync_id: &str,
        written: u64,
        deleted: u64,
        rejected: u64,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO sync_progress (sync_id, day, written, deleted, rejected)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (sync_id, day) DO UPDATE SET
                written = sync_progress.written + EXCLUDED.written,
                deleted = sync_progress.deleted + EXCLUDED.deleted,
                rejected = sync_progress.rejected + EXCLUDED.rejected",
        )
        .bind(sync_id)
        .bind(Utc::now().date_naive())
        .bind(to_i64(written))
        .bind(to_i64(deleted))
        .bind(to_i64(rejected))
        .execute(&self.pool)
        .await
        .map_err(store_err)?;
        Ok(())
    }
}

#[async_trait]
impl SyncRepository for PgStore {
    async fn create_sync(
        &self,
        tenant: Option<&str>,
        spec: &SyncSpec,
    ) -> Result<StoredSync, RepoError> {
        validate_spec(tenant, spec)?;
        let sealed = self.key.seal_spec(spec)?;
        let row = sqlx::query(
            "INSERT INTO syncs (id, tenant_id, name, kind, spec_sealed)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING created_at, updated_at",
        )
        .bind(&spec.id)
        .bind(tenant)
        .bind(&spec.name)
        .bind(spec.source.kind.as_str())
        .bind(sealed)
        .fetch_one(&self.pool)
        .await
        .map_err(from_sqlx)?;
        Ok(StoredSync {
            spec: spec.clone(),
            created_at: row.try_get("created_at").map_err(from_sqlx)?,
            updated_at: row.try_get("updated_at").map_err(from_sqlx)?,
        })
    }

    async fn get_sync(&self, tenant: Option<&str>, id: &str) -> Result<Option<SyncRow>, RepoError> {
        let row = sqlx::query(
            "SELECT id, tenant_id, name, kind, spec_sealed, created_at, updated_at FROM syncs
             WHERE id = $1 AND tenant_id IS NOT DISTINCT FROM $2",
        )
        .bind(id)
        .bind(tenant)
        .fetch_optional(&self.pool)
        .await
        .map_err(from_sqlx)?;
        row.map(|r| self.row_to_sync(&r)).transpose()
    }

    async fn list_syncs(&self, tenant: Option<&str>) -> Result<Vec<SyncRow>, RepoError> {
        let rows = sqlx::query(
            "SELECT id, tenant_id, name, kind, spec_sealed, created_at, updated_at FROM syncs
             WHERE tenant_id IS NOT DISTINCT FROM $1 ORDER BY name",
        )
        .bind(tenant)
        .fetch_all(&self.pool)
        .await
        .map_err(from_sqlx)?;
        rows.iter().map(|r| self.row_to_sync(r)).collect()
    }

    async fn update_sync(
        &self,
        tenant: Option<&str>,
        spec: &SyncSpec,
    ) -> Result<StoredSync, RepoError> {
        validate_spec(tenant, spec)?;
        let sealed = self.key.seal_spec(spec)?;
        let row = sqlx::query(
            "UPDATE syncs SET name = $3, kind = $4, spec_sealed = $5, updated_at = now()
             WHERE id = $1 AND tenant_id IS NOT DISTINCT FROM $2
             RETURNING created_at, updated_at",
        )
        .bind(&spec.id)
        .bind(tenant)
        .bind(&spec.name)
        .bind(spec.source.kind.as_str())
        .bind(sealed)
        .fetch_optional(&self.pool)
        .await
        .map_err(from_sqlx)?
        .ok_or_else(|| RepoError::NotFound(format!("sync {}", spec.id)))?;
        Ok(StoredSync {
            spec: spec.clone(),
            created_at: row.try_get("created_at").map_err(from_sqlx)?,
            updated_at: row.try_get("updated_at").map_err(from_sqlx)?,
        })
    }

    async fn delete_sync(&self, tenant: Option<&str>, id: &str) -> Result<(), RepoError> {
        // State, dead letters and progress go with it (ON DELETE CASCADE).
        let done =
            sqlx::query("DELETE FROM syncs WHERE id = $1 AND tenant_id IS NOT DISTINCT FROM $2")
                .bind(id)
                .bind(tenant)
                .execute(&self.pool)
                .await
                .map_err(from_sqlx)?;
        if done.rows_affected() == 0 {
            return Err(RepoError::NotFound(format!("sync {id}")));
        }
        Ok(())
    }

    async fn list_all_for_boot(&self) -> Result<Vec<BootSync>, RepoError> {
        let rows = sqlx::query("SELECT id, tenant_id, spec_sealed FROM syncs ORDER BY id")
            .fetch_all(&self.pool)
            .await
            .map_err(from_sqlx)?;
        rows.iter()
            .map(|row| {
                let id: String = row.try_get("id").map_err(from_sqlx)?;
                let tenant: Option<String> = row.try_get("tenant_id").map_err(from_sqlx)?;
                let sealed: Vec<u8> = row.try_get("spec_sealed").map_err(from_sqlx)?;
                let spec = self.unseal(&id, tenant.as_deref(), &sealed);
                Ok(BootSync { id, tenant, spec })
            })
            .collect()
    }

    async fn list_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        page: DeadLetterPage,
    ) -> Result<Vec<StoredDeadLetter>, RepoError> {
        self.ensure_owned(tenant, sync_id).await?;
        let rows = sqlx::query(
            "SELECT id, sync_id, index_uid, document_id, payload, reason, created_at
             FROM dead_letters WHERE sync_id = $1 AND id > $2 ORDER BY id LIMIT $3",
        )
        .bind(sync_id)
        .bind(page.after.unwrap_or(0))
        .bind(i64::from(page.clamped_limit()))
        .fetch_all(&self.pool)
        .await
        .map_err(from_sqlx)?;
        rows.iter()
            .map(|r| {
                let Json(payload): Json<serde_json::Value> =
                    r.try_get("payload").map_err(from_sqlx)?;
                Ok(StoredDeadLetter {
                    id: r.try_get("id").map_err(from_sqlx)?,
                    letter: DeadLetter {
                        sync_id: r.try_get("sync_id").map_err(from_sqlx)?,
                        index: r.try_get("index_uid").map_err(from_sqlx)?,
                        document_id: r.try_get("document_id").map_err(from_sqlx)?,
                        payload,
                        reason: r.try_get("reason").map_err(from_sqlx)?,
                        created_at: r.try_get("created_at").map_err(from_sqlx)?,
                    },
                })
            })
            .collect()
    }

    async fn delete_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        ids: &[i64],
    ) -> Result<u64, RepoError> {
        self.ensure_owned(tenant, sync_id).await?;
        if ids.is_empty() {
            return Ok(0);
        }
        let done = sqlx::query("DELETE FROM dead_letters WHERE sync_id = $1 AND id = ANY($2)")
            .bind(sync_id)
            .bind(ids)
            .execute(&self.pool)
            .await
            .map_err(from_sqlx)?;
        Ok(done.rows_affected())
    }

    async fn clear_dead_letters(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
    ) -> Result<u64, RepoError> {
        self.ensure_owned(tenant, sync_id).await?;
        let done = sqlx::query("DELETE FROM dead_letters WHERE sync_id = $1")
            .bind(sync_id)
            .execute(&self.pool)
            .await
            .map_err(from_sqlx)?;
        Ok(done.rows_affected())
    }

    async fn progress(
        &self,
        tenant: Option<&str>,
        sync_id: &str,
        from: NaiveDate,
        to: NaiveDate,
    ) -> Result<Vec<DailyProgress>, RepoError> {
        self.ensure_owned(tenant, sync_id).await?;
        let rows = sqlx::query(
            "SELECT day, written, deleted, rejected, dead_letters_dropped FROM sync_progress
             WHERE sync_id = $1 AND day BETWEEN $2 AND $3 ORDER BY day",
        )
        .bind(sync_id)
        .bind(from)
        .bind(to)
        .fetch_all(&self.pool)
        .await
        .map_err(from_sqlx)?;
        rows.iter()
            .map(|r| {
                Ok(DailyProgress {
                    day: r.try_get("day").map_err(from_sqlx)?,
                    written: to_u64(r.try_get("written").map_err(from_sqlx)?),
                    deleted: to_u64(r.try_get("deleted").map_err(from_sqlx)?),
                    rejected: to_u64(r.try_get("rejected").map_err(from_sqlx)?),
                    dead_letters_dropped: to_u64(
                        r.try_get("dead_letters_dropped").map_err(from_sqlx)?,
                    ),
                })
            })
            .collect()
    }
}

#[async_trait]
impl LabOutbox for PgStore {
    async fn enqueue_event(
        &self,
        tenant: Option<&str>,
        payload: &serde_json::Value,
    ) -> Result<Uuid, RepoError> {
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO lab_events_outbox (id, tenant_id, payload) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(tenant)
            .bind(Json(payload))
            .execute(&self.pool)
            .await
            .map_err(from_sqlx)?;
        Ok(id)
    }

    async fn lease_events(&self, limit: u32, lease: Duration) -> Result<OutboxLease, RepoError> {
        let token = Uuid::new_v4();
        if limit == 0 {
            return Ok(OutboxLease {
                token,
                events: Vec::new(),
            });
        }
        let rows = sqlx::query(LEASE_SQL)
            .bind(i64::from(limit))
            .bind(lease.as_secs_f64())
            .bind(token)
            .fetch_all(&self.pool)
            .await
            .map_err(from_sqlx)?;
        let mut events = rows
            .iter()
            .map(|r| {
                let Json(payload): Json<serde_json::Value> =
                    r.try_get("payload").map_err(from_sqlx)?;
                let created_at: DateTime<Utc> = r.try_get("created_at").map_err(from_sqlx)?;
                Ok(OutboxEvent {
                    id: r.try_get("id").map_err(from_sqlx)?,
                    tenant: r.try_get("tenant_id").map_err(from_sqlx)?,
                    payload,
                    created_at,
                })
            })
            .collect::<Result<Vec<_>, RepoError>>()?;
        events.sort_by_key(|e| (e.created_at, e.id));
        Ok(OutboxLease { token, events })
    }

    async fn mark_events_sent(&self, lease: Uuid, ids: &[Uuid]) -> Result<u64, RepoError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let done = sqlx::query(
            "UPDATE lab_events_outbox SET sent_at = now(), leased_until = NULL
             WHERE id = ANY($2) AND lease_token = $1 AND sent_at IS NULL",
        )
        .bind(lease)
        .bind(ids)
        .execute(&self.pool)
        .await
        .map_err(from_sqlx)?;
        Ok(done.rows_affected())
    }

    async fn prune_sent_events(&self, older_than: Duration) -> Result<u64, RepoError> {
        // One cutoff for every batch, so the loop ends even while events
        // keep being delivered.
        let cutoff: DateTime<Utc> = sqlx::query_scalar("SELECT now() - make_interval(secs => $1)")
            .bind(older_than.as_secs_f64())
            .fetch_one(&self.pool)
            .await
            .map_err(from_sqlx)?;
        let mut total = 0;
        loop {
            let done = sqlx::query(
                "WITH old AS MATERIALIZED (
                    SELECT id FROM lab_events_outbox
                    WHERE sent_at IS NOT NULL AND sent_at < $1
                    LIMIT $2
                 )
                 DELETE FROM lab_events_outbox o USING old WHERE o.id = old.id",
            )
            .bind(cutoff)
            .bind(PRUNE_BATCH)
            .execute(&self.pool)
            .await
            .map_err(from_sqlx)?;
            total += done.rows_affected();
            if done.rows_affected() < PRUNE_BATCH as u64 {
                return Ok(total);
            }
        }
    }

    async fn defer_events(
        &self,
        lease: Uuid,
        ids: &[Uuid],
        backoff: RetryBackoff,
    ) -> Result<u64, RepoError> {
        if ids.is_empty() {
            return Ok(0);
        }
        let done = sqlx::query(
            "UPDATE lab_events_outbox
             SET attempts = attempts + 1,
                 leased_until = NULL,
                 next_attempt_at = now() + make_interval(secs =>
                    LEAST($4::float8, $3::float8 * power(2, LEAST(attempts, 20))))
             WHERE id = ANY($2) AND lease_token = $1 AND sent_at IS NULL",
        )
        .bind(lease)
        .bind(ids)
        .bind(backoff.base.as_secs_f64())
        .bind(backoff.max.as_secs_f64())
        .execute(&self.pool)
        .await
        .map_err(from_sqlx)?;
        Ok(done.rows_affected())
    }

    async fn count_unsent(&self) -> Result<u64, RepoError> {
        let n: i64 =
            sqlx::query_scalar("SELECT count(*) FROM lab_events_outbox WHERE sent_at IS NULL")
                .fetch_one(&self.pool)
                .await
                .map_err(from_sqlx)?;
        Ok(to_u64(n))
    }
}

/// Live tests: set `DATABASE_URL` to run them.
#[cfg(test)]
mod tests;
