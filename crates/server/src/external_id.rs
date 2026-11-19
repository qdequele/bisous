//! The external ID a hosted server assumes a tenant's DynamoDB role with.
//!
//! A DynamoDB source with `credentials: assume_role` makes the server call
//! `sts:AssumeRole` with **its own** AWS identity. Every role that trusts
//! that identity is reachable by every tenant who names it, unless the
//! trust policy also demands an external ID that only its owner's syncs
//! can present. So on a hosted server (Lab mode or a public host policy)
//! the server chooses the external ID of a tenant's sync, never the
//! tenant:
//!
//! `external_id = "mc-" + hex(HMAC-SHA256(k, "dynamodb-external-id:v1:" + tenant))[..32]`
//!
//! where `k` is derived from `CONNECT_SECRET_KEY` for this use only
//! ([`SecretKey::derive_subkey`]). It is stable for a tenant, differs
//! between tenants, and no tenant can compute another's.
//!
//! - **Requests** (check, discover, create, update): an `external_id` other
//!   than the tenant's is refused (`external_id_not_allowed`); an equal one
//!   is accepted and dropped. A stored spec never holds one.
//! - **Builds** (every probe and every engine start, in
//!   [`crate::guard::GuardedSources`]): the source assumes the role with
//!   the tenant's external ID. A spec stored before this rule with another
//!   external ID is held, before any connection, until its owner moves the
//!   role's trust policy to the new one.
//! - Without `CONNECT_SECRET_KEY` (the in-memory store) there is no key:
//!   assume-role is refused for tenants.
//! - The global scope (standalone admin) keeps a free-form `external_id`.

use std::sync::Arc;

use hmac::{Hmac, Mac};
use meili_connect_core::{SourceKind, SourceSpec};
use meili_connect_store::SecretKey;
use serde_json::Value;
use sha2::Sha256;
use subtle::ConstantTimeEq;

const SUBKEY_LABEL: &str = "dynamodb-external-id";
const MESSAGE_PREFIX: &str = "dynamodb-external-id:v1:";
/// Hex characters kept from the MAC (128 bits).
const ID_HEX_LEN: usize = 32;

/// Derives the external ID of each tenant. `Debug` never shows the key.
#[derive(Clone)]
pub struct ExternalIds {
    key: [u8; 32],
}

impl std::fmt::Debug for ExternalIds {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExternalIds(<redacted>)")
    }
}

impl ExternalIds {
    pub fn new(secret_key: &SecretKey) -> Self {
        Self {
            key: secret_key.derive_subkey(SUBKEY_LABEL),
        }
    }

    /// The external ID of `tenant`'s DynamoDB roles.
    pub fn for_tenant(&self, tenant: &str) -> String {
        let mut mac = match <Hmac<Sha256> as Mac>::new_from_slice(&self.key) {
            Ok(m) => m,
            Err(_) => unreachable!("HMAC accepts keys of any length"),
        };
        mac.update(MESSAGE_PREFIX.as_bytes());
        mac.update(tenant.as_bytes());
        let hex = hex::encode(mac.finalize().into_bytes());
        format!("mc-{}", &hex[..ID_HEX_LEN])
    }
}

/// The rule for one tenant-owned source on a hosted server.
#[derive(Debug, Clone)]
pub struct Binding {
    /// `None`: no `CONNECT_SECRET_KEY`, assume-role is refused.
    pub ids: Option<Arc<ExternalIds>>,
    pub tenant: String,
}

/// The rule for a source owned by `tenant`: only a tenant's, on a hosted
/// server.
pub fn external_id_binding(
    hosted: bool,
    ids: &Option<Arc<ExternalIds>>,
    tenant: Option<&str>,
) -> Option<Binding> {
    match tenant {
        Some(tenant) if hosted => Some(Binding {
            ids: ids.clone(),
            tenant: tenant.to_owned(),
        }),
        _ => None,
    }
}

/// Why a tenant's source cannot assume its role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// It carries an external ID other than the tenant's.
    Mismatch {
        /// The tenant's external ID.
        expected: String,
    },
    /// The server has no key to derive external IDs with.
    Unavailable,
}

/// Message of [`Refusal::Mismatch`] on a request.
pub const NOT_ALLOWED: &str = "`external_id` is chosen by this server for your team: leave it \
     out, or send the value of GET /sources/dynamodb/trust, and use that value in the role's \
     trust policy (`sts:ExternalId` condition)";

/// Message of [`Refusal::Unavailable`].
pub const UNAVAILABLE: &str = "`credentials: assume_role` is not available on this server: it \
     needs CONNECT_SECRET_KEY (with DATABASE_URL) to derive your team's external ID; use static \
     keys instead";

impl Refusal {
    /// What the owner of a stored sync is told when it is held.
    pub fn held_message(&self) -> String {
        match self {
            Refusal::Mismatch { expected } => format!(
                "this sync assumes its DynamoDB role with an external ID that is no longer \
                 accepted: the external ID is now chosen by this server for your team. Update \
                 the role's trust policy to the new external ID `{expected}` (GET \
                 /sources/dynamodb/trust gives the whole policy), then save the sync again \
                 without `external_id`"
            ),
            Refusal::Unavailable => UNAVAILABLE.to_owned(),
        }
    }
}

/// The `credentials` object of a DynamoDB assume-role source.
fn assume_role(spec: &SourceSpec) -> Option<&serde_json::Map<String, Value>> {
    if spec.kind != SourceKind::Dynamodb {
        return None;
    }
    let credentials = spec.config.get("credentials")?.as_object()?;
    (credentials.get("type").and_then(Value::as_str) == Some("assume_role")).then_some(credentials)
}

/// Check the external ID `spec` carries against the tenant's, and return
/// the tenant's (`None`: the source assumes no role).
fn expected(spec: &SourceSpec, binding: &Binding) -> Result<Option<String>, Refusal> {
    let Some(credentials) = assume_role(spec) else {
        return Ok(None);
    };
    let ids = binding.ids.as_ref().ok_or(Refusal::Unavailable)?;
    let expected = ids.for_tenant(&binding.tenant);
    match credentials.get("external_id") {
        None | Some(Value::Null) => Ok(Some(expected)),
        Some(Value::String(given)) if bool::from(given.as_bytes().ct_eq(expected.as_bytes())) => {
            Ok(Some(expected))
        }
        Some(_) => Err(Refusal::Mismatch { expected }),
    }
}

/// A request's source: refuse a foreign external ID, drop the tenant's own
/// (the stored spec never holds one).
pub fn take_requested(spec: &mut SourceSpec, binding: &Binding) -> Result<(), Refusal> {
    if expected(spec, binding)?.is_some()
        && let Some(credentials) = spec
            .config
            .get_mut("credentials")
            .and_then(Value::as_object_mut)
    {
        credentials.remove("external_id");
    }
    Ok(())
}

/// The spec a source is built from: the tenant's external ID in place.
pub fn bind(spec: &SourceSpec, binding: &Binding) -> Result<SourceSpec, Refusal> {
    let mut bound = spec.clone();
    if let Some(id) = expected(spec, binding)?
        && let Some(credentials) = bound
            .config
            .get_mut("credentials")
            .and_then(Value::as_object_mut)
    {
        credentials.insert("external_id".into(), Value::String(id));
    }
    Ok(bound)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn ids(byte: u8) -> Arc<ExternalIds> {
        Arc::new(ExternalIds::new(&SecretKey::from_bytes([byte; 32])))
    }

    fn binding(tenant: &str) -> Binding {
        Binding {
            ids: Some(ids(1)),
            tenant: tenant.into(),
        }
    }

    fn role(external_id: Option<&str>) -> SourceSpec {
        let mut credentials = json!({"type": "assume_role", "role_arn": "arn:aws:iam::1:role/r"});
        if let Some(id) = external_id {
            credentials["external_id"] = json!(id);
        }
        SourceSpec {
            kind: SourceKind::Dynamodb,
            config: json!({"region": "eu-west-1", "credentials": credentials}),
        }
    }

    fn external_id(spec: &SourceSpec) -> Option<&str> {
        spec.config["credentials"]
            .get("external_id")
            .and_then(Value::as_str)
    }

    #[test]
    fn the_derivation_is_stable_per_tenant_and_differs_between_tenants() {
        let a = ids(1).for_tenant("team-a");
        assert_eq!(a, ids(1).for_tenant("team-a"));
        assert_eq!(a.len(), 3 + ID_HEX_LEN);
        assert!(a.starts_with("mc-"));
        assert!(a[3..].chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(a, ids(1).for_tenant("team-b"));
        assert_ne!(a, ids(1).for_tenant("team-a "));
        // Another CONNECT_SECRET_KEY, other IDs.
        assert_ne!(a, ids(2).for_tenant("team-a"));
        assert!(!format!("{:?}", ids(1)).contains("1, 1"));
    }

    #[test]
    fn a_tenant_cannot_present_another_tenants_external_id() {
        let a_id = ids(1).for_tenant("team-a");
        // Tenant B sends A's role and A's external ID: refused.
        let mut spec = role(Some(&a_id));
        assert_eq!(
            take_requested(&mut spec, &binding("team-b")),
            Err(Refusal::Mismatch {
                expected: ids(1).for_tenant("team-b")
            })
        );
        // Whatever B builds, it assumes the role with its own ID.
        let bound = bind(&role(None), &binding("team-b")).unwrap();
        assert_eq!(
            external_id(&bound),
            Some(ids(1).for_tenant("team-b").as_str())
        );
        assert!(bind(&role(Some(&a_id)), &binding("team-b")).is_err());
    }

    #[test]
    fn requests_keep_no_external_id() {
        let own = ids(1).for_tenant("team-a");
        for given in [None, Some(own.as_str())] {
            let mut spec = role(given);
            take_requested(&mut spec, &binding("team-a")).unwrap();
            assert_eq!(external_id(&spec), None);
            assert_eq!(
                external_id(&bind(&spec, &binding("team-a")).unwrap()),
                Some(own.as_str())
            );
        }
        let mut spec = role(None);
        spec.config["credentials"]["external_id"] = json!(42);
        assert!(take_requested(&mut spec, &binding("team-a")).is_err());
    }

    #[test]
    fn only_dynamodb_assume_role_is_concerned() {
        let no_key = Binding {
            ids: None,
            tenant: "t".into(),
        };
        let others = [
            SourceSpec {
                kind: SourceKind::Dynamodb,
                config: json!({"region": "x", "credentials": {"type": "static", "access_key_id": "A", "secret_access_key": "S"}}),
            },
            SourceSpec {
                kind: SourceKind::Postgres,
                config: json!({"credentials": {"type": "assume_role", "external_id": "x"}}),
            },
        ];
        for spec in others {
            assert_eq!(bind(&spec, &no_key).unwrap(), spec);
            let mut copy = spec.clone();
            take_requested(&mut copy, &no_key).unwrap();
            assert_eq!(copy, spec);
        }
        // Without a key, assume-role is refused.
        assert_eq!(bind(&role(None), &no_key), Err(Refusal::Unavailable));
        let held = Refusal::Mismatch {
            expected: "mc-x".into(),
        }
        .held_message();
        assert!(
            held.contains("trust policy") && held.contains("mc-x"),
            "{held}"
        );
    }
}
