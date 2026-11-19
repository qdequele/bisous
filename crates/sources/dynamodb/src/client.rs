//! AWS client construction.

use std::time::Duration;

use aws_config::timeout::TimeoutConfig;
use aws_config::{BehaviorVersion, Region, SdkConfig};
use aws_credential_types::Credentials;

use crate::config::{CredentialsConfig, DynamoConfig};

/// The three clients a source needs, sharing one credentials provider.
#[derive(Clone, Debug)]
pub struct Clients {
    pub dynamo: aws_sdk_dynamodb::Client,
    pub streams: aws_sdk_dynamodbstreams::Client,
    pub sts: aws_sdk_sts::Client,
    /// A custom endpoint is in use (DynamoDB Local): STS is not available.
    pub custom_endpoint: bool,
}

impl Clients {
    pub async fn build(config: &DynamoConfig, sync_id: &str) -> Self {
        let timeouts = TimeoutConfig::builder()
            .connect_timeout(Duration::from_secs(10))
            .operation_attempt_timeout(Duration::from_secs(30))
            .operation_timeout(Duration::from_secs(90))
            .build();
        let base = || {
            aws_config::defaults(BehaviorVersion::latest())
                .region(Region::new(config.region.clone()))
                .timeout_config(timeouts.clone())
        };
        let sdk: SdkConfig = match &config.credentials {
            CredentialsConfig::Default => base().load().await,
            CredentialsConfig::Static {
                access_key_id,
                secret_access_key,
                session_token,
            } => {
                let creds = Credentials::new(
                    access_key_id.clone(),
                    secret_access_key.expose().to_owned(),
                    session_token.as_ref().map(|t| t.expose().to_owned()),
                    None,
                    "meili-connect-static",
                );
                base().credentials_provider(creds).load().await
            }
            CredentialsConfig::AssumeRole {
                role_arn,
                external_id,
                session_name,
            } => {
                // Base credentials: the host's default chain. STS keeps its
                // regular endpoint even when DynamoDB's is overridden.
                let host = base().load().await;
                let mut builder = aws_config::sts::AssumeRoleProvider::builder(role_arn.clone())
                    .session_name(
                        session_name
                            .clone()
                            .unwrap_or_else(|| session_name_for(sync_id)),
                    )
                    .configure(&host);
                if let Some(ext) = external_id {
                    builder = builder.external_id(ext.expose().to_owned());
                }
                let provider = builder.build().await;
                base().credentials_provider(provider).load().await
            }
        };

        let mut dynamo = aws_sdk_dynamodb::config::Builder::from(&sdk);
        let mut streams = aws_sdk_dynamodbstreams::config::Builder::from(&sdk);
        if let Some(url) = &config.endpoint_url {
            dynamo = dynamo.endpoint_url(url.clone());
            streams = streams.endpoint_url(url.clone());
        }
        Self {
            dynamo: aws_sdk_dynamodb::Client::from_conf(dynamo.build()),
            streams: aws_sdk_dynamodbstreams::Client::from_conf(streams.build()),
            sts: aws_sdk_sts::Client::new(&sdk),
            custom_endpoint: config.endpoint_url.is_some(),
        }
    }
}

/// The ARN of this host's own AWS identity (the default provider chain), from
/// `sts:GetCallerIdentity`: the principal that assumes customers' roles.
/// The error is a short description, without the SDK's cause chain.
pub async fn host_caller_arn(timeout: Duration) -> Result<String, String> {
    let lookup = async {
        let sdk = aws_config::defaults(BehaviorVersion::latest())
            .timeout_config(
                TimeoutConfig::builder()
                    .connect_timeout(Duration::from_secs(3))
                    .operation_timeout(timeout)
                    .build(),
            )
            .load()
            .await;
        let mut sts = aws_sdk_sts::config::Builder::from(&sdk);
        if sdk.region().is_none() {
            // GetCallerIdentity answers in every region.
            sts = sts.region(Region::new("us-east-1"));
        }
        let out = aws_sdk_sts::Client::from_conf(sts.build())
            .get_caller_identity()
            .send()
            .await
            .map_err(|e| {
                tracing::warn!(error = %aws_smithy_types::error::display::DisplayErrorContext(&e), "sts:GetCallerIdentity failed");
                "sts:GetCallerIdentity failed (no AWS credentials on this server?)".to_owned()
            })?;
        out.arn
            .ok_or_else(|| "sts:GetCallerIdentity returned no ARN".to_owned())
    };
    tokio::time::timeout(timeout, lookup)
        .await
        .map_err(|_| "sts:GetCallerIdentity did not answer in time".to_owned())?
}

/// STS session names: 2–64 chars of `[\w+=,.@-]`.
fn session_name_for(sync_id: &str) -> String {
    let clean: String = sync_id
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || "+=,.@-_".contains(*c))
        .take(50)
        .collect();
    format!("meili-connect-{clean}")
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn session_names_are_valid() {
        assert_eq!(session_name_for("abc-123"), "meili-connect-abc-123");
        let long = session_name_for(&"x/".repeat(100));
        assert!(long.len() <= 64);
        assert!(!long.contains('/'));
    }
}
