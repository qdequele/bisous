//! meili-connect server: the HTTP API, auth, the supervisor wiring and the
//! Lab seams. See `docs/ARCHITECTURE.md` and the README.

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

pub mod api;
pub mod auth;
pub mod config;
pub mod error;
pub mod lab;
pub mod manager;
pub mod redact;
pub mod registry;
pub mod routes;
pub mod state;
pub mod usage;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use axum::Router;
use axum::routing::{get, post};
use meili_connect_core::{SourceFactory, StateStore, SyncPhase};
use meili_connect_engine::SupervisorConfig;
use meili_connect_store::{MemStore, PgStore, SecretKey};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::auth::AuthConfig;
use crate::config::{Config, LogFormat};
use crate::manager::{SinkFactory, SyncManager, meili_sinks};
use crate::registry::SourceRegistry;
use crate::state::{AppState, Backend};
use crate::usage::{MeteredStore, UsageMeter};

/// The API. Every path is listed in [`routes::ROUTES`].
pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(api::health))
        .route("/sources/check", post(api::check_source))
        .route("/sources/discover", post(api::discover_source))
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
        .with_state(state)
}

/// What [`build_state`] needs besides the backend.
pub struct Wiring {
    pub sources: Arc<dyn SourceFactory>,
    pub sinks: SinkFactory,
    pub supervisor: SupervisorConfig,
    pub auth: AuthConfig,
    pub probe_timeout: std::time::Duration,
    pub lab_events: bool,
}

/// Assemble the shared state around a backend. The engine gets the
/// backend wrapped in a [`MeteredStore`].
pub fn build_state(
    backend: Arc<dyn Backend>,
    wiring: Wiring,
    cancel: CancellationToken,
) -> AppState {
    let meter = Arc::new(UsageMeter::new());
    let engine_store: Arc<dyn StateStore> = Arc::new(MeteredStore::new(
        Arc::clone(&backend) as Arc<dyn StateStore>,
        Arc::clone(&meter),
    ));
    let manager = Arc::new(SyncManager::new(
        engine_store,
        Arc::clone(&wiring.sources),
        Arc::clone(&wiring.sinks),
        wiring.supervisor,
        meter,
        cancel,
    ));
    AppState {
        backend,
        manager,
        sources: wiring.sources,
        sinks: wiring.sinks,
        auth: wiring.auth,
        probe_timeout: wiring.probe_timeout,
        lab_events: wiring.lab_events,
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
        let phase = match state.state_store().load(&spec.id).await {
            Ok(s) => s.map(|s| s.phase),
            Err(e) => {
                warn!(sync_id = %spec.id, error = %e, "cannot read the sync state; starting it");
                None
            }
        };
        if phase == Some(SyncPhase::Paused) {
            state.manager.register_idle(&spec);
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

    let backend: Arc<dyn Backend> = match (&config.database_url, &config.secret_key) {
        (Some(url), Some(key)) => {
            let key = SecretKey::from_base64(key).map_err(|e| e.to_string())?;
            let store = PgStore::connect(url, key)
                .await
                .map_err(|e| format!("cannot open the state database: {e}"))?;
            info!("state store: Postgres");
            Arc::new(store)
        }
        _ => {
            warn!(
                "DATABASE_URL is not set: using the IN-MEMORY store. Syncs, checkpoints and \
                 usage are LOST on restart, and every sync snapshots again. Do not run this in \
                 production."
            );
            Arc::new(MemStore::new())
        }
    };

    let auth = AuthConfig {
        lab_service_token: config.lab_service_token.clone(),
        admin_api_key: config.admin_api_key.clone(),
    };
    if auth.is_open() {
        warn!(
            "neither ADMIN_API_KEY nor LAB_SERVICE_TOKEN is set: the API is in OPEN mode, anyone \
             who reaches it can read and change every sync"
        );
    }

    let cancel = CancellationToken::new();
    let state = build_state(
        Arc::clone(&backend),
        Wiring {
            sources: Arc::new(SourceRegistry::new()),
            sinks: meili_sinks(),
            supervisor: SupervisorConfig::default(),
            auth,
            probe_timeout: config.source_probe_timeout,
            lab_events: config.lab_events.is_some(),
        },
        cancel.child_token(),
    );

    let started = boot_syncs(&state)
        .await
        .map_err(|e| format!("cannot list the stored syncs: {e}"))?;
    info!(started, "syncs started");

    let background = CancellationToken::new();
    let mut tasks = Vec::new();
    if let Some(lab) = &config.lab_events {
        info!(lab_url = %lab.url, "usage events go to the Lab");
        let outbox: Arc<dyn meili_connect_store::LabOutbox> = Arc::clone(&backend) as _;
        tasks.push(tokio::spawn(lab::run_usage_flush(
            Arc::clone(state.manager.meter()),
            Arc::clone(&outbox),
            config.usage_flush_interval,
            background.child_token(),
        )));
        let sender = lab::LabSender::new(outbox, lab.clone())
            .map_err(|e| format!("cannot build the Lab HTTP client: {e}"))?;
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
    if config.lab_events.is_some() {
        // Counts not enqueued yet would be lost: enqueue them now.
        if let Err(e) = state.manager.meter().flush(state.outbox()).await {
            warn!(error = %e, "cannot enqueue the last usage counts");
        }
    }
    background.cancel();
    for t in tasks {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), t).await;
    }
    cancel.cancel();
    info!("stopped");
    Ok(())
}
