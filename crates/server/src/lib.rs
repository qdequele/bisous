//! meili-connect server: the HTTP API, auth, the supervisor wiring and the
//! Lab seams. See `docs/ARCHITECTURE.md` and the README.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod ambient;
pub mod api;
pub mod auth;
pub mod config;
pub mod credits;
pub mod error;
pub mod guard;
pub mod lab;
pub mod limits;
pub mod manager;
pub mod metrics;
pub mod redact;
pub mod registry;
pub mod routes;
pub mod state;
pub mod tinybird;
pub mod usage;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use meili_connect_core::{HostPolicy, Secret, SourceFactory, StateStore, SyncPhase};
use meili_connect_engine::SupervisorConfig;
use meili_connect_store::{DeadLetterLimits, Lane, MemStore, PgStore, SecretKey, UsageLedger};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::auth::AuthConfig;
use crate::config::{Config, LogFormat};
use crate::credits::CreditCache;
use crate::limits::{Limits, ProbeSlots};
use crate::manager::{ManagerDeps, SinkFactory, SyncManager, meili_sinks};
use crate::metrics::Metrics;
use crate::registry::SourceRegistry;
use crate::state::{AppState, Backend};
use crate::tinybird::{TinybirdConfig, TinybirdReader, TinybirdSender};
use crate::usage::{MeteredStore, UsageEvents};

/// The API. Every path is listed in [`routes::ROUTES`].
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(api::health))
        .route("/metrics", get(metrics::metrics))
        .route("/sources/check", post(api::check_source))
        .route("/sources/discover", post(api::discover_source))
        .route("/usage", get(api::usage))
        .route("/syncs", post(api::create_sync).get(api::list_syncs))
        .route(
            "/syncs/{id}",
            get(api::get_sync)
                .put(api::update_sync)
                .delete(api::delete_sync),
        )
        .route("/syncs/{id}/pause", post(api::pause_sync))
        .route("/syncs/{id}/resume", post(api::resume_sync))
        .route("/syncs/{id}/resync", post(api::resync_sync))
        .route("/syncs/{id}/release", post(api::release_sync))
        .route("/syncs/{id}/status", get(api::sync_status))
        .route("/syncs/{id}/progress", get(api::sync_progress))
        .route(
            "/syncs/{id}/dead-letters",
            get(api::list_dead_letters).delete(api::clear_dead_letters),
        )
        .fallback(api::fallback)
        .layer(axum::middleware::from_fn_with_state(
            state.limits.request_timeout,
            limits::request_timeout,
        ))
        .with_state(state)
}

/// What [`build_state`] needs besides the backend.
pub struct Wiring {
    pub sources: Arc<dyn SourceFactory>,
    pub sinks: SinkFactory,
    pub policy: Arc<HostPolicy>,
    pub supervisor: SupervisorConfig,
    pub auth: AuthConfig,
    pub probe_timeout: std::time::Duration,
    pub lab_events: bool,
    pub limits: Limits,
    /// Lab mode: hold the syncs of a team out of credits.
    pub credits: Option<Arc<CreditCache>>,
    /// `GET /metrics` answers 404 without it.
    pub metrics_token: Option<Secret>,
    pub lag_guard: crate::config::LagGuard,
    /// Usage analytics: its own outbox lane, `GET /usage` from its pipe.
    pub tinybird: Option<TinybirdConfig>,
    /// Lab mode or `CONNECT_HOST_POLICY=public`.
    pub hosted: bool,
}

/// Assemble the shared state around a backend. The engine gets the
/// backend wrapped in a [`MeteredStore`].
pub fn build_state(
    backend: Arc<dyn Backend>,
    wiring: Wiring,
    cancel: CancellationToken,
) -> AppState {
    let mut lanes = Vec::new();
    if wiring.lab_events {
        lanes.push(Lane::Lab);
    }
    if wiring.tinybird.is_some() {
        lanes.push(Lane::Tinybird);
    }
    let usage =
        Arc::new(UsageEvents::new(lanes).with_backlog_cap(wiring.limits.outbox_max_pending));
    backend.set_usage_lanes(usage.lanes());
    let metrics = Arc::new(Metrics::new());
    let engine_store: Arc<dyn StateStore> = Arc::new(MeteredStore::new(
        Arc::clone(&backend) as Arc<dyn StateStore>,
        Arc::clone(&metrics),
    ));
    let manager = Arc::new(SyncManager::new(ManagerDeps {
        store: engine_store,
        backend: Arc::clone(&backend),
        sources: Arc::clone(&wiring.sources),
        sinks: Arc::clone(&wiring.sinks),
        policy: Arc::clone(&wiring.policy),
        config: wiring.supervisor,
        usage,
        max_syncs_per_tenant: wiring.limits.max_syncs_per_tenant,
        stop_grace: wiring.limits.stop_grace,
        credits: wiring.credits,
        metrics: Arc::clone(&metrics),
        hosted: wiring.hosted,
        cancel,
    }));
    AppState {
        backend,
        manager,
        registry: wiring.sources,
        sinks: wiring.sinks,
        policy: wiring.policy,
        auth: wiring.auth,
        probe_timeout: wiring.probe_timeout,
        lab_events: wiring.lab_events,
        metrics,
        metrics_token: wiring.metrics_token,
        lag_guard: wiring.lag_guard,
        hosted: wiring.hosted,
        usage_reader: match &wiring.tinybird {
            Some(tb) => match TinybirdReader::new(tb.clone()) {
                Ok(reader) => Some(Arc::new(reader)),
                Err(e) => {
                    warn!(error = %e, "cannot build the Tinybird client; GET /usage reads the state database");
                    None
                }
            },
            None => None,
        },
        probes: Arc::new(ProbeSlots::new(
            wiring.limits.probe_concurrency,
            wiring.limits.probe_concurrency_per_tenant,
        )),
        limits: wiring.limits,
    }
}

/// Start every stored sync whose persisted phase is not `Paused`.
pub async fn boot_syncs(state: &AppState) -> Result<usize, meili_connect_store::RepoError> {
    let mut started = 0;
    for row in state.repo().list_all_for_boot().await? {
        let spec = match row.spec {
            Ok(spec) => spec,
            Err(e) => {
                error!(sync_id = %row.id, error = %e, "cannot read a stored sync; it is not started");
                continue;
            }
        };
        let saved = match state.state_store().load(&spec.id).await {
            Ok(s) => s,
            Err(e) => {
                warn!(sync_id = %spec.id, error = %e, "cannot read the sync state; starting it");
                None
            }
        };
        let phase = saved.as_ref().map(|s| s.phase);
        let credit_hold = saved.as_ref().is_some_and(|s| {
            s.phase == SyncPhase::Held
                && s.message.as_deref() == Some(crate::credits::OUT_OF_CREDITS)
        });
        if phase == Some(SyncPhase::Paused) || credit_hold {
            // A sync held for credits resumes by itself with them.
            state.manager.register_idle(&spec, saved.as_ref());
            continue;
        }
        let id = spec.id.clone();
        match state.manager.run(spec).await {
            Ok(()) => started += 1,
            Err(e) => error!(sync_id = %id, error = %e, "cannot start a stored sync"),
        }
    }
    Ok(started)
}

fn init_tracing(config: &Config) {
    let filter = tracing_subscriber::EnvFilter::try_new(&config.log_filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let ansi = std::io::IsTerminal::is_terminal(&std::io::stdout());
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_ansi(ansi);
    match config.log_format {
        LogFormat::Json => builder.json().init(),
        LogFormat::Pretty => builder.init(),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            warn!(error = %e, "cannot listen for SIGINT");
            std::future::pending::<()>().await;
        }
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                warn!(error = %e, "cannot listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => info!("SIGINT received, shutting down"),
        _ = term => info!("SIGTERM received, shutting down"),
    }
}

/// The process: read the config, open the store, start the syncs, serve.
pub async fn run() -> Result<(), String> {
    let config = Config::from_env().map_err(|e| e.0)?;
    init_tracing(&config);

    let dead_letter_limits = DeadLetterLimits {
        max_per_sync: config.limits.dead_letters_max_per_sync,
        ..DeadLetterLimits::default()
    };
    let backend: Arc<dyn Backend> = match (&config.database_url, &config.secret_key) {
        (Some(url), Some(key)) => {
            let key = SecretKey::from_base64(key).map_err(|e| e.to_string())?;
            let store = PgStore::connect(url, key)
                .await
                .map_err(|e| format!("cannot open the state database: {e}"))?
                .with_dead_letter_limits(dead_letter_limits);
            info!("state store: Postgres");
            Arc::new(store)
        }
        _ => {
            warn!(
                "DATABASE_URL is not set: using the IN-MEMORY store. Syncs, checkpoints and \
                 usage are LOST on restart, and every sync snapshots again. Do not run this in \
                 production."
            );
            Arc::new(MemStore::new().with_dead_letter_limits(dead_letter_limits))
        }
    };

    let auth = AuthConfig {
        lab_service_token: config.lab_service_token.clone(),
        admin_api_key: config.admin_api_key.clone(),
        open_mode: config.open_mode,
    };
    if auth.is_open() {
        warn!(
            "neither ADMIN_API_KEY nor LAB_SERVICE_TOKEN is set: the API is in OPEN mode, anyone \
             who reaches it can read and change every sync"
        );
    }

    let policy = Arc::new(config.host_policy.clone());
    if policy.is_any() && config.lab_service_token.is_some() {
        warn!(
            "CONNECT_HOST_POLICY=any with LAB_SERVICE_TOKEN set: tenants can point syncs at \
             internal addresses (metadata endpoint, private networks). Use `public` or \
             `allowlist` for a hosted deployment"
        );
    }
    info!(host_policy = policy.mode_name(), "host policy");

    // Lab mode: a team out of credits has its syncs held.
    let credits = match &config.lab_events {
        Some(lab) => Some(Arc::new(
            CreditCache::new(lab.clone())
                .map_err(|e| format!("cannot build the Lab HTTP client: {e}"))?
                .with_windows(config.credits_fresh, config.credits_stale),
        )),
        None => None,
    };
    let cancel = CancellationToken::new();
    let state = build_state(
        Arc::clone(&backend),
        Wiring {
            sources: Arc::new(SourceRegistry::new()),
            sinks: meili_sinks(Arc::clone(&policy)),
            policy,
            supervisor: SupervisorConfig::default(),
            auth,
            probe_timeout: config.source_probe_timeout,
            lab_events: config.lab_events.is_some(),
            limits: config.limits.clone(),
            credits: credits.clone(),
            metrics_token: config.metrics_token.clone(),
            lag_guard: config.lag_guard,
            tinybird: config.tinybird.clone(),
            hosted: config.hosted(),
        },
        cancel.child_token(),
    );

    let started = boot_syncs(&state)
        .await
        .map_err(|e| format!("cannot list the stored syncs: {e}"))?;
    info!(started, "syncs started");

    let background = CancellationToken::new();
    let mut tasks = Vec::new();
    if !state.manager.usage().lanes().is_empty() {
        tasks.push(tokio::spawn(lab::run_usage_flush(
            Arc::clone(state.manager.usage()),
            Arc::clone(&backend) as Arc<dyn UsageLedger>,
            config.usage_flush_interval,
            background.child_token(),
        )));
    }
    if credits.is_some() {
        tasks.push(tokio::spawn(Arc::clone(&state.manager).run_credit_checks(
            config.credits_check_interval,
            background.child_token(),
        )));
    }
    if let Some(tb) = &config.tinybird {
        info!(tinybird_url = %tb.url, "usage rows go to Tinybird");
        let sender = TinybirdSender::new(backend.outbox(Lane::Tinybird), tb.clone())
            .map_err(|e| format!("cannot build the Tinybird HTTP client: {e}"))?
            .with_metrics(Arc::clone(&state.metrics));
        tasks.push(tokio::spawn(
            sender.run(config.lab_send_interval, background.child_token()),
        ));
    }
    if let Some(lab) = &config.lab_events {
        info!(lab_url = %lab.url, "usage events go to the Lab");
        let sender = lab::LabSender::new(backend.outbox(Lane::Lab), lab.clone())
            .map_err(|e| format!("cannot build the Lab HTTP client: {e}"))?
            .with_metrics(Arc::clone(&state.metrics));
        tasks.push(tokio::spawn(
            sender.run(config.lab_send_interval, background.child_token()),
        ));
    } else {
        info!("LAB_URL is not set: no usage events are sent");
    }

    let listener = tokio::net::TcpListener::bind(config.listen_addr)
        .await
        .map_err(|e| format!("cannot listen on {}: {e}", config.listen_addr))?;
    info!(addr = %config.listen_addr, "listening");
    axum::serve(listener, router(state.clone()))
        .with_graceful_shutdown(shutdown_signal())
        .await
        .map_err(|e| format!("server error: {e}"))?;

    info!("stopping the syncs");
    state.manager.shutdown(config.shutdown_grace).await;
    // Pending usage is durable; enqueue it now so it goes out sooner.
    if let Err(e) = state.manager.usage().flush(&*backend).await {
        warn!(error = %e, "cannot enqueue the last usage; it stays pending");
    }
    background.cancel();
    for t in tasks {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), t).await;
    }
    cancel.cancel();
    info!("stopped");
    Ok(())
}
