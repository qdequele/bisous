//! Management authentication, same model as glutony.
//!
//! | `LAB_SERVICE_TOKEN` | `ADMIN_API_KEY` | caller sends | scope |
//! |---|---|---|---|
//! | set | any | `Bearer <token>` + **required** `X-Connect-Tenant-Id` | that tenant |
//! | any | set | `Bearer <key>`, `X-Connect-Tenant-Id` optional | global, or the named tenant |
//! | unset | unset | nothing | open mode: global scope, only when the config allowed it (loopback or `ALLOW_OPEN_MODE=true`); otherwise every call is refused |
//!
//! Tokens are compared in constant time. `/health` never goes through here.

use axum::extract::FromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;
use meili_connect_core::Secret;
use subtle::ConstantTimeEq;

use crate::error::ApiError;
use crate::state::AppState;

/// Header naming the tenant a call acts for.
pub const H_TENANT_ID: &str = "x-connect-tenant-id";

/// Longest accepted tenant id.
pub const MAX_TENANT_LEN: usize = 128;

/// The default has no secret and no open mode: it refuses every call.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    pub lab_service_token: Option<Secret>,
    pub admin_api_key: Option<Secret>,
    /// No secret is configured and that was allowed (loopback listener or
    /// `ALLOW_OPEN_MODE=true`). Ignored when a secret is set.
    pub open_mode: bool,
}

impl AuthConfig {
    /// Open mode, explicitly.
    pub fn open() -> Self {
        Self {
            open_mode: true,
            ..Self::default()
        }
    }

    fn no_secret(&self) -> bool {
        self.lab_service_token.is_none() && self.admin_api_key.is_none()
    }

    /// Open mode: no management secret configured, deliberately.
    pub fn is_open(&self) -> bool {
        self.no_secret() && self.open_mode
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    /// The Meilisearch Lab, always acting for one tenant.
    Lab,
    /// An operator; global unless it names a tenant.
    Admin,
    /// No management auth configured.
    Open,
}

/// Who calls and which tenant the call is scoped to (`None`: global).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    pub principal: Principal,
    pub tenant: Option<String>,
}

impl Scope {
    pub fn tenant(&self) -> Option<&str> {
        self.tenant.as_deref()
    }
}

/// Constant-time equality. Lengths leak, contents do not.
pub fn secret_eq(expected: &str, got: &str) -> bool {
    bool::from(expected.as_bytes().ct_eq(got.as_bytes()))
}

fn token_matches(expected: Option<&Secret>, got: &str) -> bool {
    expected.is_some_and(|e| secret_eq(e.expose(), got))
}

fn bearer(headers: &HeaderMap) -> Option<&str> {
    let value = headers
        .get(axum::http::header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let (scheme, token) = value.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

pub fn validate_tenant(t: &str) -> Result<(), ApiError> {
    let ok = !t.is_empty()
        && t.len() <= MAX_TENANT_LEN
        && t.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b':'));
    if ok { Ok(()) } else { Err(invalid_tenant()) }
}

fn invalid_tenant() -> ApiError {
    ApiError::new(
        axum::http::StatusCode::BAD_REQUEST,
        "invalid_tenant",
        format!(
            "{H_TENANT_ID} must be 1 to {MAX_TENANT_LEN} characters of letters, digits, '-', \
             '_', '.' or ':'"
        ),
    )
}

/// Authenticate a management request.
pub fn authorize(headers: &HeaderMap, config: &AuthConfig) -> Result<Scope, ApiError> {
    if config.is_open() {
        return Ok(Scope {
            principal: Principal::Open,
            tenant: None,
        });
    }
    if config.no_secret() {
        // Fail closed: no secret and open mode not allowed.
        return Err(ApiError::unauthorized(
            "no management auth is configured on this server",
        ));
    }
    // Check the token before looking at the tenant: an unauthenticated
    // caller learns nothing about tenants.
    let token =
        bearer(headers).ok_or_else(|| ApiError::unauthorized("missing or invalid bearer token"))?;
    // Evaluate both comparisons so the timing does not tell which matched.
    let is_lab = token_matches(config.lab_service_token.as_ref(), token);
    let is_admin = token_matches(config.admin_api_key.as_ref(), token);
    let principal = match (is_lab, is_admin) {
        (true, _) => Principal::Lab,
        (false, true) => Principal::Admin,
        (false, false) => return Err(ApiError::unauthorized("missing or invalid bearer token")),
    };
    let tenant = match headers.get(H_TENANT_ID) {
        Some(v) => {
            let t = v.to_str().map_err(|_| invalid_tenant())?.trim();
            validate_tenant(t)?;
            Some(t.to_owned())
        }
        None if principal == Principal::Lab => {
            return Err(ApiError::new(
                axum::http::StatusCode::BAD_REQUEST,
                "invalid_tenant",
                format!("{H_TENANT_ID} is required with the Lab service token"),
            ));
        }
        None => None,
    };
    Ok(Scope { principal, tenant })
}

impl FromRequestParts<AppState> for Scope {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&parts.headers, &state.auth)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]
    use super::*;
    use axum::http::{HeaderName, HeaderValue};

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn cfg(lab: Option<&str>, admin: Option<&str>) -> AuthConfig {
        AuthConfig {
            lab_service_token: lab.map(Secret::new),
            admin_api_key: admin.map(Secret::new),
            open_mode: lab.is_none() && admin.is_none(),
        }
    }

    #[test]
    fn no_secret_without_open_mode_refuses_everything() {
        let err = authorize(&HeaderMap::new(), &AuthConfig::default()).unwrap_err();
        assert_eq!(err.code, "unauthorized");
        let err = authorize(
            &headers(&[("authorization", "Bearer anything")]),
            &AuthConfig::default(),
        )
        .unwrap_err();
        assert_eq!(err.code, "unauthorized");
    }

    #[test]
    fn open_mode_is_global() {
        let s = authorize(&HeaderMap::new(), &cfg(None, None)).unwrap();
        assert_eq!(s.principal, Principal::Open);
        assert_eq!(s.tenant, None);
        // A tenant header is ignored in open mode: there is no one to trust.
        let s = authorize(&headers(&[(H_TENANT_ID, "t1")]), &cfg(None, None)).unwrap();
        assert_eq!(s.tenant, None);
    }

    #[test]
    fn lab_token_requires_a_tenant() {
        let c = cfg(Some("lab-secret"), Some("admin-secret"));
        let err = authorize(&headers(&[("authorization", "Bearer lab-secret")]), &c).unwrap_err();
        assert_eq!(err.code, "invalid_tenant");
        let s = authorize(
            &headers(&[
                ("authorization", "Bearer lab-secret"),
                (H_TENANT_ID, "team-1"),
            ]),
            &c,
        )
        .unwrap();
        assert_eq!(s.principal, Principal::Lab);
        assert_eq!(s.tenant(), Some("team-1"));
    }

    #[test]
    fn admin_is_global_or_acts_for_a_tenant() {
        let c = cfg(Some("lab-secret"), Some("admin-secret"));
        let s = authorize(&headers(&[("authorization", "Bearer admin-secret")]), &c).unwrap();
        assert_eq!((s.principal, s.tenant), (Principal::Admin, None));
        let s = authorize(
            &headers(&[
                ("authorization", "bearer admin-secret"),
                (H_TENANT_ID, "team-2"),
            ]),
            &c,
        )
        .unwrap();
        assert_eq!(s.tenant(), Some("team-2"));
    }

    #[test]
    fn wrong_or_missing_tokens_are_401() {
        for c in [
            cfg(Some("lab-secret"), None),
            cfg(None, Some("admin-secret")),
            cfg(Some("lab-secret"), Some("admin-secret")),
        ] {
            for h in [
                HeaderMap::new(),
                headers(&[("authorization", "Bearer nope")]),
                headers(&[("authorization", "Basic lab-secret")]),
                headers(&[("authorization", "Bearer lab-secretX")]),
                headers(&[("authorization", "Bearer admin-secre")]),
                headers(&[("authorization", "Bearer ")]),
                // A bad tenant does not matter before the token is valid.
                headers(&[
                    ("authorization", "Bearer nope"),
                    (H_TENANT_ID, "bad tenant"),
                ]),
            ] {
                let err = authorize(&h, &c).unwrap_err();
                assert_eq!(err.code, "unauthorized", "{h:?}");
            }
        }
    }

    #[test]
    fn bad_tenant_ids_are_refused() {
        let c = cfg(Some("lab-secret"), None);
        for t in ["", "a b", "x/y", &"a".repeat(MAX_TENANT_LEN + 1)] {
            let err = authorize(
                &headers(&[("authorization", "Bearer lab-secret"), (H_TENANT_ID, t)]),
                &c,
            )
            .unwrap_err();
            assert_eq!(err.code, "invalid_tenant", "{t:?}");
        }
    }

    #[test]
    fn constant_time_compare() {
        assert!(secret_eq("abc", "abc"));
        assert!(!secret_eq("abc", "abd"));
        assert!(!secret_eq("abc", "ab"));
        assert!(!secret_eq("", "a"));
    }
}
