//! The API error body: `{"code": "...", "message": "..."}` on every failure.

use axum::Json;
use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use meili_connect_core::SourceError;
use meili_connect_engine::{EngineError, SupervisorError};
use meili_connect_store::RepoError;
use serde_json::json;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self {
            status,
            code,
            message: message.into(),
        }
    }

    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message)
    }

    pub fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }

    pub fn not_found(what: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", what)
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(StatusCode::CONFLICT, "conflict", message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }

    /// A `"<redacted>"` placeholder for a secret whose connection changed.
    pub fn secret_required(message: impl Into<String>) -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, "secret_required", message)
    }

    /// A source or destination the host policy refuses.
    pub fn host_not_allowed(message: impl Into<String>) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "host_not_allowed",
            message,
        )
    }

    pub fn sync_not_found(id: &str) -> Self {
        Self::not_found(format!("sync {id} not found"))
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({ "code": self.code, "message": self.message })),
        )
            .into_response()
    }
}

impl From<RepoError> for ApiError {
    fn from(e: RepoError) -> Self {
        match e {
            RepoError::NotFound(m) => Self::not_found(m),
            RepoError::Conflict(_) => {
                Self::conflict("a sync with this id or name already exists in this scope")
            }
            RepoError::Invalid(m) => Self::invalid(m),
            RepoError::Transient(m) => {
                tracing::warn!(error = %m, "state store unavailable");
                Self::new(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "store_unavailable",
                    "the state store is unavailable, retry later",
                )
            }
            RepoError::Fatal(m) => {
                tracing::error!(error = %m, "state store error");
                Self::internal("state store error")
            }
        }
    }
}

impl From<meili_connect_core::StoreError> for ApiError {
    fn from(e: meili_connect_core::StoreError) -> Self {
        RepoError::from(e).into()
    }
}

/// Errors of `check`, `discover` and `teardown`, by reaction.
pub fn source_error(e: &SourceError) -> ApiError {
    match e {
        SourceError::Config(m) => {
            ApiError::new(StatusCode::UNPROCESSABLE_ENTITY, "source_config", m.clone())
        }
        SourceError::Paused(m) => {
            ApiError::new(StatusCode::SERVICE_UNAVAILABLE, "source_paused", m.clone())
        }
        SourceError::Transient(m) | SourceError::PositionLost(m) => {
            ApiError::new(StatusCode::BAD_GATEWAY, "source_unavailable", m.clone())
        }
        SourceError::Fatal(m) => ApiError::new(StatusCode::BAD_GATEWAY, "source_error", m.clone()),
    }
}

impl From<SupervisorError> for ApiError {
    fn from(e: SupervisorError) -> Self {
        match e {
            SupervisorError::AlreadyRunning(id) => Self::conflict(format!("sync {id} is running")),
            SupervisorError::NotFound(id) => Self::conflict(format!("sync {id} is not running")),
            SupervisorError::ShuttingDown => Self::new(
                StatusCode::SERVICE_UNAVAILABLE,
                "shutting_down",
                "the server is shutting down",
            ),
            SupervisorError::Store(e) => e.into(),
            SupervisorError::Engine(EngineError::Spec(m)) => Self::invalid(m),
            SupervisorError::Engine(EngineError::Source(e)) => source_error(&e),
            SupervisorError::Engine(e) => {
                Self::new(StatusCode::BAD_GATEWAY, "engine", e.to_string())
            }
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(e: JsonRejection) -> Self {
        // The body text names the field and the expected type, never values.
        Self::new(e.status(), "invalid_request", e.body_text())
    }
}

impl From<QueryRejection> for ApiError {
    fn from(e: QueryRejection) -> Self {
        Self::invalid(e.body_text())
    }
}
