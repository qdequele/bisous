//! gRPC statuses → the engine's reaction.

use std::error::Error as StdError;

use gcloud_sdk::error::ErrorKind;
use meili_connect_core::SourceError;
use tonic::{Code, Status};

/// The IAM role that grants every read the connector makes.
pub const VIEWER_ROLE: &str = "Cloud Datastore Viewer (roles/datastore.viewer)";

/// Map a failed call. `who` names the service account in permission
/// messages (its email, never the key).
pub fn status_error(op: &str, status: &Status, who: Option<&str>) -> SourceError {
    if let Some(e) = credentials_error(status) {
        return e;
    }
    let msg = format!("{op}: {}", short(status.message()));
    match status.code() {
        Code::Unauthenticated => SourceError::Config(format!(
            "{msg} (the service account key was refused: is it revoked or disabled?)"
        )),
        Code::PermissionDenied => {
            let grant = match who {
                Some(email) => format!("grant {VIEWER_ROLE} to {email}"),
                None => format!("grant {VIEWER_ROLE} to the service account"),
            };
            if status.message().contains("has not been used")
                || status.message().contains("is disabled")
            {
                SourceError::Config(format!(
                    "{msg} (enable the Cloud Firestore API in the project)"
                ))
            } else {
                SourceError::Config(format!("{msg} ({grant})"))
            }
        }
        Code::NotFound => SourceError::Config(format!(
            "{msg} (check `project_id` and `database_id`: the database must exist and be in Firestore Native mode)"
        )),
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => {
            SourceError::Config(msg)
        }
        Code::Unimplemented => SourceError::Fatal(msg),
        // Quota, server errors, network, deadlines, aborted streams.
        _ => SourceError::Transient(msg),
    }
}

/// A token could not be minted: the key is malformed or refused (the user
/// must fix it), or the token endpoint failed (retry).
fn credentials_error(status: &Status) -> Option<SourceError> {
    let mut cur: Option<&(dyn StdError + 'static)> = status.source();
    while let Some(e) = cur {
        if let Some(err) = e.downcast_ref::<gcloud_sdk::error::Error>() {
            return Some(match err.kind() {
                ErrorKind::Credentials(c) if c.is_transient() => SourceError::Transient(format!(
                    "minting an access token: {}",
                    short(&c.to_string())
                )),
                ErrorKind::Credentials(c) => SourceError::Config(format!(
                    "the service account key was refused by Google: {} (is the key revoked, or the account disabled?)",
                    short(&c.to_string())
                )),
                ErrorKind::CredentialsBuild(_) => SourceError::Config(
                    "the service account key cannot be used (paste the whole JSON key file)".into(),
                ),
                _ => SourceError::Transient(short(&err.to_string())),
            });
        }
        cur = e.source();
    }
    None
}

/// The first line, at most 300 characters.
pub fn short(s: &str) -> String {
    s.lines()
        .next()
        .unwrap_or_default()
        .chars()
        .take(300)
        .collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn codes_map_to_reactions() {
        let e = |code| {
            status_error(
                "RunQuery",
                &Status::new(code, "boom"),
                Some("a@b.iam.gserviceaccount.com"),
            )
        };
        assert!(matches!(e(Code::Unavailable), SourceError::Transient(_)));
        assert!(matches!(
            e(Code::ResourceExhausted),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            e(Code::DeadlineExceeded),
            SourceError::Transient(_)
        ));
        assert!(matches!(e(Code::Internal), SourceError::Transient(_)));
        assert!(matches!(e(Code::NotFound), SourceError::Config(_)));
        assert!(matches!(e(Code::Unauthenticated), SourceError::Config(_)));
        assert!(matches!(e(Code::Unimplemented), SourceError::Fatal(_)));
        match e(Code::PermissionDenied) {
            SourceError::Config(m) => {
                assert!(
                    m.contains("roles/datastore.viewer") && m.contains("a@b.iam"),
                    "{m}"
                )
            }
            other => panic!("{other:?}"),
        }
        let disabled = status_error(
            "ListCollectionIds",
            &Status::new(
                Code::PermissionDenied,
                "Cloud Firestore API has not been used in project 1 before or it is disabled",
            ),
            None,
        );
        assert!(
            matches!(disabled, SourceError::Config(m) if m.contains("enable the Cloud Firestore API"))
        );
    }
}
