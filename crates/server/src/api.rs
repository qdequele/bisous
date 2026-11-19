//! HTTP handlers. Every route but `/health` takes a [`Scope`] and only ever
//! sees the syncs of that scope: a sync of another tenant is a 404, on
//! every route.

use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Days, NaiveDate, Utc};
use meili_connect_core::{
    MeiliDestination, Source, SourceSpec, StreamMapping, SyncPhase, SyncSpec,
};
use meili_connect_store::{DeadLetterPage, StoredSync};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::auth::{Principal, Scope};
use crate::error::{ApiError, source_error};
use crate::guard::{VetError, coarse_source_error, vet_destination, vet_source};
use crate::redact::{
    REDACTED, check_placeholders, contains_redacted, redact_spec, restore_redacted,
};
use crate::state::AppState;

type ApiResult<T> = Result<T, ApiError>;

/// Id used to build a source for `check` and `discover`; nothing is
/// created on the origin under it.
const PROBE_ID: &str = "preflight";

const MAX_NAME_LEN: usize = 200;
const MAX_PROGRESS_DAYS: u64 = 366;

fn body(input: Result<Json<Value>, JsonRejection>) -> ApiResult<Value> {
    input.map(|Json(v)| v).map_err(ApiError::from)
}

// ---------------------------------------------------------------- health

pub async fn health() -> Json<Value> {
    Json(json!({ "status": "ok" }))
}

// ---------------------------------------------------------------- sources

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProbeRequest {
    pub source: SourceSpec,
    #[serde(default)]
    pub streams: Vec<StreamMapping>,
}

fn probe_request(input: Result<Json<Value>, JsonRejection>) -> ApiResult<ProbeRequest> {
    let value = body(input)?;
    if let Some(path) = contains_redacted(&value) {
        return Err(ApiError::invalid(format!(
            "`{path}` is \"{REDACTED}\": send the real value"
        )));
    }
    serde_json::from_value(value).map_err(|e| ApiError::invalid(e.to_string()))
}

/// Builds the source (its config errors are the connector's own words),
/// then vets its endpoints against the host policy.
async fn probe_source(
    state: &AppState,
    req: &ProbeRequest,
    coarse: bool,
) -> ApiResult<Box<dyn Source>> {
    let sources = state.sources(coarse);
    state
        .registry
        .build(PROBE_ID, &req.source, &req.streams)
        .map_err(|e| source_error(&e))?;
    vet(state, &req.source, coarse).await?;
    sources
        .build_vetted(PROBE_ID, &req.source, &req.streams)
        .map_err(|e| source_error(&e))
}

/// Every endpoint of the source passes the host policy.
async fn vet(state: &AppState, source: &SourceSpec, coarse: bool) -> ApiResult<()> {
    match vet_source(&*state.registry, &state.policy, source).await {
        Ok(_) => Ok(()),
        Err(VetError::Policy(e)) => Err(ApiError::host_not_allowed(e.to_string())),
        Err(VetError::Source(e)) if coarse => Err(source_error(&coarse_source_error(e))),
        Err(VetError::Source(e)) => Err(source_error(&e)),
    }
}

/// Lab callers get coarse source and sink errors.
fn coarse(scope: &Scope) -> bool {
    scope.principal == Principal::Lab
}

fn probe_timeout(state: &AppState) -> ApiError {
    ApiError::new(
        StatusCode::GATEWAY_TIMEOUT,
        "source_timeout",
        format!(
            "the source did not answer within {} s",
            state.probe_timeout.as_secs()
        ),
    )
}

pub async fn check_source(
    State(state): State<AppState>,
    scope: Scope,
    input: Result<Json<Value>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let req = probe_request(input)?;
    let report = tokio::time::timeout(state.probe_timeout, async {
        let mut source = probe_source(&state, &req, coarse(&scope)).await?;
        source.check().await.map_err(|e| source_error(&e))
    })
    .await
    .map_err(|_| probe_timeout(&state))??;
    Ok(Json(json!({
        "passed": report.passed(),
        "items": report.items,
    })))
}

pub async fn discover_source(
    State(state): State<AppState>,
    scope: Scope,
    input: Result<Json<Value>, JsonRejection>,
) -> ApiResult<Json<Value>> {
    let req = probe_request(input)?;
    let collections = tokio::time::timeout(state.probe_timeout, async {
        let mut source = probe_source(&state, &req, coarse(&scope)).await?;
        source.discover().await.map_err(|e| source_error(&e))
    })
    .await
    .map_err(|_| probe_timeout(&state))??;
    Ok(Json(json!({ "collections": collections })))
}

// ---------------------------------------------------------------- syncs

/// The writable part of a sync. `id`, `tenant`, `created_at` and
/// `updated_at` are accepted (so a `GET` body can be sent back as is) and
/// ignored.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncInput {
    name: String,
    source: SourceSpec,
    destination: MeiliDestination,
    streams: Vec<StreamMapping>,
    #[serde(default = "default_guard")]
    mass_delete_guard: Option<f64>,
    #[serde(default, rename = "id")]
    _id: Option<Value>,
    #[serde(default, rename = "tenant")]
    _tenant: Option<Value>,
    #[serde(default, rename = "created_at")]
    _created_at: Option<Value>,
    #[serde(default, rename = "updated_at")]
    _updated_at: Option<Value>,
}

fn default_guard() -> Option<f64> {
    Some(0.5)
}

impl SyncInput {
    fn into_spec(self, id: String, tenant: Option<String>) -> SyncSpec {
        SyncSpec {
            id,
            tenant,
            name: self.name.trim().to_owned(),
            source: self.source,
            destination: self.destination,
            streams: self.streams,
            mass_delete_guard: self.mass_delete_guard,
        }
    }
}

/// `SyncSpec::validate` plus what the server needs: a name, a
/// Meilisearch URL, a source config its connector accepts, and a source and
/// a destination the host policy allows (resolved now, so a refusal is
/// immediate; vetted again on every start).
async fn validate(state: &AppState, spec: &SyncSpec, coarse: bool) -> ApiResult<()> {
    validate_shape(state, spec)?;
    vet_destination(&state.policy, spec.destination.url.trim())
        .await
        .map_err(ApiError::host_not_allowed)?;
    vet(state, &spec.source, coarse).await
}

fn validate_shape(state: &AppState, spec: &SyncSpec) -> ApiResult<()> {
    if spec.name.is_empty() || spec.name.len() > MAX_NAME_LEN {
        return Err(ApiError::invalid(format!(
            "name must be 1 to {MAX_NAME_LEN} characters"
        )));
    }
    spec.validate().map_err(ApiError::invalid)?;
    let url = spec.destination.url.trim();
    if !(url.starts_with("http://") || url.starts_with("https://")) {
        return Err(ApiError::invalid("destination.url must be an http(s) URL"));
    }
    (state.sinks)(&spec.destination).map_err(|e| ApiError::invalid(e.to_string()))?;
    // Builds the source without connecting: the connector checks its
    // config shape and the mappings.
    state
        .registry
        .build(&spec.id, &spec.source, &spec.streams)
        .map_err(|e| source_error(&e))?;
    Ok(())
}

#[derive(Debug, Serialize)]
pub struct SyncView {
    #[serde(flatten)]
    spec: Value,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// The only way a spec leaves the server: with every secret redacted.
pub fn view(stored: &StoredSync) -> ApiResult<SyncView> {
    let mut spec = serde_json::to_value(&stored.spec)
        .map_err(|_| ApiError::internal("cannot serialize the sync"))?;
    redact_spec(&mut spec);
    Ok(SyncView {
        spec,
        created_at: stored.created_at,
        updated_at: stored.updated_at,
    })
}

async fn owned(state: &AppState, scope: &Scope, id: &str) -> ApiResult<StoredSync> {
    state
        .repo()
        .get_sync(scope.tenant(), id)
        .await?
        .ok_or_else(|| ApiError::sync_not_found(id))
}

pub async fn create_sync(
    State(state): State<AppState>,
    scope: Scope,
    input: Result<Json<Value>, JsonRejection>,
) -> ApiResult<Response> {
    let value = body(input)?;
    if let Some(path) = contains_redacted(&value) {
        return Err(ApiError::invalid(format!(
            "`{path}` is \"{REDACTED}\": send the real value"
        )));
    }
    let input: SyncInput =
        serde_json::from_value(value).map_err(|e| ApiError::invalid(e.to_string()))?;
    let id = uuid::Uuid::now_v7().to_string();
    let spec = input.into_spec(id, scope.tenant.clone());
    validate(&state, &spec, coarse(&scope)).await?;
    let stored = state.manager.create(spec).await?;
    tracing::info!(sync_id = %stored.spec.id, tenant = ?scope.tenant(), "sync created");
    Ok((StatusCode::CREATED, Json(view(&stored)?)).into_response())
}

pub async fn list_syncs(State(state): State<AppState>, scope: Scope) -> ApiResult<Json<Value>> {
    let syncs = state.repo().list_syncs(scope.tenant()).await?;
    let views = syncs.iter().map(view).collect::<ApiResult<Vec<_>>>()?;
    Ok(Json(json!({ "syncs": views })))
}

pub async fn get_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<SyncView>> {
    Ok(Json(view(&owned(&state, &scope, &id).await?)?))
}

pub async fn update_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
    query: Result<Query<ForceQuery>, QueryRejection>,
    input: Result<Json<Value>, JsonRejection>,
) -> ApiResult<Json<SyncView>> {
    let Query(query) = query?;
    let mut value = body(input)?;
    let coarse = coarse(&scope);
    // Everything that depends on the stored spec runs inside the sync's
    // lock, on the row read there.
    let prepare = async |current: &StoredSync| -> ApiResult<SyncSpec> {
        let stored_json = serde_json::to_value(&current.spec)
            .map_err(|_| ApiError::internal("cannot serialize the sync"))?;
        check_placeholders(&value, &stored_json).map_err(ApiError::secret_required)?;
        restore_redacted(&mut value, &stored_json).map_err(ApiError::invalid)?;
        let input: SyncInput = serde_json::from_value(std::mem::take(&mut value))
            .map_err(|e| ApiError::invalid(e.to_string()))?;
        let spec = input.into_spec(current.spec.id.clone(), current.spec.tenant.clone());
        validate(&state, &spec, coarse).await?;
        Ok(spec)
    };
    let stored = state
        .manager
        .update(scope.tenant(), &id, query.force, prepare)
        .await?;
    Ok(Json(view(&stored)?))
}

/// `?force=true`: go on when the origin cannot be cleaned up.
#[derive(Debug, Deserialize)]
pub struct ForceQuery {
    #[serde(default)]
    force: bool,
}

pub async fn delete_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
    query: Result<Query<ForceQuery>, QueryRejection>,
) -> ApiResult<StatusCode> {
    let Query(query) = query?;
    state
        .manager
        .delete(scope.tenant(), &id, query.force)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

// ---------------------------------------------------------------- actions

pub async fn pause_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    state.manager.pause(scope.tenant(), &id).await?;
    status_json(&state, &scope, &id).await.map(Json)
}

pub async fn resume_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    state.manager.resume(scope.tenant(), &id).await?;
    status_json(&state, &scope, &id).await.map(Json)
}

pub async fn resync_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    state.manager.resync(scope.tenant(), &id).await?;
    status_json(&state, &scope, &id).await.map(Json)
}

pub async fn release_sync(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    state.manager.release(scope.tenant(), &id).await?;
    status_json(&state, &scope, &id).await.map(Json)
}

// ---------------------------------------------------------------- status

async fn status_json(state: &AppState, scope: &Scope, id: &str) -> ApiResult<Value> {
    let saved = state.state_store().load(id).await?;
    let live = state.manager.status(id).await?;
    let first_day = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap_or_default();
    let today = Utc::now().date_naive();
    let days = state
        .repo()
        .progress(scope.tenant(), id, first_day, today)
        .await?;
    let (mut written, mut deleted, mut rejected) = (0u64, 0u64, 0u64);
    for d in &days {
        written = written.saturating_add(d.written);
        deleted = deleted.saturating_add(d.deleted);
        rejected = rejected.saturating_add(d.rejected);
    }
    let phase = saved.as_ref().map_or(SyncPhase::Pending, |s| s.phase);
    Ok(json!({
        "sync_id": id,
        "phase": phase,
        "message": saved.as_ref().and_then(|s| s.message.clone()),
        "has_checkpoint": saved.as_ref().is_some_and(|s| s.checkpoint.is_some()),
        "resyncing": saved.as_ref().is_some_and(|s| s.resyncing),
        "updated_at": saved.as_ref().map(|s| s.updated_at),
        "running": live.as_ref().is_some_and(|s| s.running),
        "consecutive_failures": live.as_ref().map_or(0, |s| s.consecutive_failures),
        "last_error": live.as_ref().and_then(|s| s.last_error.clone()),
        "progress": { "written": written, "deleted": deleted, "rejected": rejected },
    }))
}

pub async fn sync_status(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    owned(&state, &scope, &id).await?;
    status_json(&state, &scope, &id).await.map(Json)
}

#[derive(Debug, Deserialize)]
pub struct ProgressQuery {
    from: Option<NaiveDate>,
    to: Option<NaiveDate>,
}

pub async fn sync_progress(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
    query: Result<Query<ProgressQuery>, QueryRejection>,
) -> ApiResult<Json<Value>> {
    let Query(q) = query?;
    owned(&state, &scope, &id).await?;
    let to = q.to.unwrap_or_else(|| Utc::now().date_naive());
    let from = match q.from {
        Some(f) => f,
        None => to.checked_sub_days(Days::new(29)).unwrap_or(to),
    };
    if from > to {
        return Err(ApiError::invalid("`from` is after `to`"));
    }
    if (to - from).num_days() >= MAX_PROGRESS_DAYS as i64 {
        return Err(ApiError::invalid(format!(
            "the range spans more than {MAX_PROGRESS_DAYS} days"
        )));
    }
    let days = state.repo().progress(scope.tenant(), &id, from, to).await?;
    Ok(Json(json!({ "from": from, "to": to, "days": days })))
}

#[derive(Debug, Deserialize)]
pub struct DeadLetterQuery {
    after: Option<i64>,
    limit: Option<u32>,
}

pub async fn list_dead_letters(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
    query: Result<Query<DeadLetterQuery>, QueryRejection>,
) -> ApiResult<Json<Value>> {
    let Query(q) = query?;
    owned(&state, &scope, &id).await?;
    let limit = q.limit.unwrap_or(100).clamp(1, 1000);
    let letters = state
        .repo()
        .list_dead_letters(
            scope.tenant(),
            &id,
            DeadLetterPage {
                after: q.after,
                limit,
            },
        )
        .await?;
    let next_after = (letters.len() as u32 == limit)
        .then(|| letters.last().map(|l| l.id))
        .flatten();
    let items: Vec<Value> = letters
        .into_iter()
        .map(|l| {
            json!({
                "id": l.id,
                "index": l.letter.index,
                "document_id": l.letter.document_id,
                "payload": l.letter.payload,
                "reason": l.letter.reason,
                "created_at": l.letter.created_at,
            })
        })
        .collect();
    Ok(Json(json!({ "items": items, "next_after": next_after })))
}

pub async fn clear_dead_letters(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> ApiResult<Json<Value>> {
    owned(&state, &scope, &id).await?;
    let deleted = state.repo().clear_dead_letters(scope.tenant(), &id).await?;
    Ok(Json(json!({ "deleted": deleted })))
}

/// Unknown routes answer with the standard error body.
pub async fn fallback() -> ApiError {
    ApiError::not_found("no such route")
}
