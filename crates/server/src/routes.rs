//! Every route the server mounts. The tests hold `router()` (in `lib.rs`)
//! and `docs/openapi.yaml` against this table: adding a route means adding
//! it here, in `lib.rs` and in the OpenAPI spec, or the build fails.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteSpec {
    pub method: &'static str,
    pub path: &'static str,
    /// Goes through management auth (everything but `/health`).
    pub authenticated: bool,
}

const fn open(method: &'static str, path: &'static str) -> RouteSpec {
    RouteSpec {
        method,
        path,
        authenticated: false,
    }
}

const fn auth(method: &'static str, path: &'static str) -> RouteSpec {
    RouteSpec {
        method,
        path,
        authenticated: true,
    }
}

pub const ROUTES: &[RouteSpec] = &[
    open("GET", "/health"),
    auth("POST", "/sources/check"),
    auth("POST", "/sources/discover"),
    auth("POST", "/syncs"),
    auth("GET", "/syncs"),
    auth("GET", "/syncs/{id}"),
    auth("PUT", "/syncs/{id}"),
    auth("DELETE", "/syncs/{id}"),
    auth("POST", "/syncs/{id}/pause"),
    auth("POST", "/syncs/{id}/resume"),
    auth("POST", "/syncs/{id}/resync"),
    auth("POST", "/syncs/{id}/release"),
    auth("GET", "/syncs/{id}/status"),
    auth("GET", "/syncs/{id}/progress"),
    auth("GET", "/syncs/{id}/dead-letters"),
    auth("DELETE", "/syncs/{id}/dead-letters"),
];
