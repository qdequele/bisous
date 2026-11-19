//! AWS SDK errors → the engine's reaction.

use std::error::Error as StdError;

use aws_credential_types::provider::error::CredentialsError;
use aws_sdk_dynamodb::config::http::HttpResponse;
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use meili_connect_core::SourceError;

/// What an AWS error means for us.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrClass {
    /// Throttled: back off and retry in place.
    Throttle,
    /// Network, 5xx, timeouts: the engine retries.
    Transient,
    /// Credentials, permissions, validation: needs the user.
    Config,
    /// Stream data is gone (trimmed).
    Lost,
    /// Shard iterator older than 15 minutes.
    ExpiredIterator,
    /// The table, stream or shard does not exist.
    NotFound,
    /// A bug on our side.
    Fatal,
}

/// Classify a service error from its code and HTTP status.
pub fn classify_code(code: Option<&str>, status: Option<u16>) -> ErrClass {
    match code.unwrap_or_default() {
        "ThrottlingException"
        | "ProvisionedThroughputExceededException"
        | "RequestLimitExceeded"
        | "LimitExceededException"
        | "Throttling"
        | "TooManyRequestsException" => ErrClass::Throttle,
        "TrimmedDataAccessException" => ErrClass::Lost,
        "ExpiredIteratorException" => ErrClass::ExpiredIterator,
        "ResourceNotFoundException" => ErrClass::NotFound,
        "AccessDeniedException"
        | "AccessDenied"
        | "UnrecognizedClientException"
        | "InvalidClientTokenId"
        | "InvalidSignatureException"
        | "SignatureDoesNotMatch"
        | "IncompleteSignature"
        | "MissingAuthenticationToken"
        | "MissingAuthenticationTokenException"
        | "ExpiredToken"
        | "ExpiredTokenException"
        | "UnauthorizedOperation"
        | "ValidationException"
        | "InvalidEndpointException" => ErrClass::Config,
        "InternalServerError" | "InternalFailure" | "ServiceUnavailable" => ErrClass::Transient,
        _ => match status {
            Some(429) => ErrClass::Throttle,
            Some(s) if s >= 500 => ErrClass::Transient,
            Some(401 | 403) => ErrClass::Config,
            Some(s) if s >= 400 => ErrClass::Config,
            _ => ErrClass::Transient,
        },
    }
}

/// Classify any SDK error and render a message that names the operation
/// and AWS's error code. AWS messages never contain secret keys.
pub fn classify<E>(op: &str, err: &SdkError<E, HttpResponse>) -> (ErrClass, String)
where
    E: ProvideErrorMetadata + StdError + 'static,
{
    match err {
        SdkError::ServiceError(_) => {
            let code = err.code();
            let status = err.raw_response().map(|r| r.status().as_u16());
            let class = classify_code(code, status);
            let msg = format!(
                "{op}: {}: {}",
                code.unwrap_or("error"),
                err.message().unwrap_or("no message")
            );
            (class, msg)
        }
        SdkError::TimeoutError(_) => (ErrClass::Transient, format!("{op}: request timed out")),
        SdkError::ConstructionFailure(_) => (
            ErrClass::Fatal,
            format!("{op}: could not build the request: {}", chain(err)),
        ),
        SdkError::DispatchFailure(d) if d.is_io() || d.is_timeout() => (
            ErrClass::Transient,
            format!("{op}: network error: {}", chain(err)),
        ),
        _ => {
            let class = credentials_class(err).unwrap_or(ErrClass::Transient);
            (class, format!("{op}: {}", chain(err)))
        }
    }
}

/// Identity resolution failures surface as dispatch failures: find the
/// `CredentialsError` in the chain and decide whether the user must act.
fn credentials_class(err: &(dyn StdError + 'static)) -> Option<ErrClass> {
    let mut cur: Option<&(dyn StdError + 'static)> = Some(err);
    while let Some(e) = cur {
        if let Some(c) = e.downcast_ref::<CredentialsError>() {
            return Some(match c {
                CredentialsError::CredentialsNotLoaded(_)
                | CredentialsError::InvalidConfiguration(_) => ErrClass::Config,
                CredentialsError::ProviderError(_) => {
                    // AssumeRole refused (trust policy, external id) is the
                    // user's to fix; anything else may pass.
                    let text = chain(c);
                    if [
                        "AccessDenied",
                        "InvalidClientTokenId",
                        "ExpiredToken",
                        "not authorized",
                    ]
                    .iter()
                    .any(|needle| text.contains(needle))
                    {
                        ErrClass::Config
                    } else {
                        ErrClass::Transient
                    }
                }
                _ => ErrClass::Transient,
            });
        }
        cur = e.source();
    }
    None
}

/// The error and its sources on one line.
fn chain(err: &(dyn StdError + 'static)) -> String {
    let mut out = err.to_string();
    let mut cur = err.source();
    let mut depth = 0;
    while let Some(e) = cur {
        let s = e.to_string();
        if !out.contains(&s) {
            out.push_str(": ");
            out.push_str(&s);
        }
        cur = e.source();
        depth += 1;
        if depth > 8 {
            break;
        }
    }
    out
}

/// Map a classified error to a `SourceError` for callers that have no
/// special handling. `NotFound` and `ExpiredIterator` become `Config` and
/// `Transient`; callers that care match on the class first.
pub fn to_source_error(class: ErrClass, msg: String) -> SourceError {
    match class {
        ErrClass::Throttle | ErrClass::Transient | ErrClass::ExpiredIterator => {
            SourceError::Transient(msg)
        }
        ErrClass::Config | ErrClass::NotFound => SourceError::Config(msg),
        ErrClass::Lost => SourceError::PositionLost(msg),
        ErrClass::Fatal => SourceError::Fatal(msg),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn codes_map_to_reactions() {
        let c = |code: &str| classify_code(Some(code), Some(400));
        assert_eq!(
            c("ProvisionedThroughputExceededException"),
            ErrClass::Throttle
        );
        assert_eq!(c("ThrottlingException"), ErrClass::Throttle);
        assert_eq!(c("LimitExceededException"), ErrClass::Throttle);
        assert_eq!(c("TrimmedDataAccessException"), ErrClass::Lost);
        assert_eq!(c("ExpiredIteratorException"), ErrClass::ExpiredIterator);
        assert_eq!(c("ResourceNotFoundException"), ErrClass::NotFound);
        assert_eq!(c("AccessDeniedException"), ErrClass::Config);
        assert_eq!(c("UnrecognizedClientException"), ErrClass::Config);
        assert_eq!(c("ExpiredTokenException"), ErrClass::Config);
        assert_eq!(
            classify_code(Some("InternalServerError"), Some(500)),
            ErrClass::Transient
        );
        assert_eq!(classify_code(None, Some(503)), ErrClass::Transient);
        assert_eq!(classify_code(None, Some(429)), ErrClass::Throttle);
        assert_eq!(classify_code(Some("Weird"), Some(403)), ErrClass::Config);
        assert_eq!(classify_code(None, None), ErrClass::Transient);
    }

    #[test]
    fn classes_map_to_source_errors() {
        let m = || "m".to_owned();
        assert!(matches!(
            to_source_error(ErrClass::Throttle, m()),
            SourceError::Transient(_)
        ));
        assert!(matches!(
            to_source_error(ErrClass::Lost, m()),
            SourceError::PositionLost(_)
        ));
        assert!(matches!(
            to_source_error(ErrClass::Config, m()),
            SourceError::Config(_)
        ));
        assert!(matches!(
            to_source_error(ErrClass::Fatal, m()),
            SourceError::Fatal(_)
        ));
    }

    #[test]
    fn missing_credentials_need_the_user() {
        let e = CredentialsError::not_loaded("no providers in chain provided credentials");
        assert_eq!(credentials_class(&e), Some(ErrClass::Config));
        let e = CredentialsError::provider_error(
            "AccessDenied: not authorized to perform sts:AssumeRole",
        );
        assert_eq!(credentials_class(&e), Some(ErrClass::Config));
        let e = CredentialsError::provider_timed_out(std::time::Duration::from_secs(5));
        assert_eq!(credentials_class(&e), Some(ErrClass::Transient));
    }
}
