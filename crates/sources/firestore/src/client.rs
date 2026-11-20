//! gRPC client construction: Google's endpoint with a service account key,
//! or the emulator with its stub token.

use std::time::Duration;

use gcloud_sdk::HeaderMap;
use gcloud_sdk::google::firestore::v1::firestore_client::FirestoreClient;
use gcloud_sdk::google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag,
};
use gcloud_sdk::google_cloud_auth::errors::CredentialsError;
use gcloud_sdk::{GoogleApi, GoogleAuthHeaders, GoogleAuthMiddleware, GoogleAuthMiddlewareLayer};
use meili_connect_core::SourceError;

use crate::config::FirestoreConfig;

/// Google's Firestore endpoint. Never configurable apart from the emulator:
/// a service account key only ever goes to Google.
pub const FIRESTORE_API_URL: &str = "https://firestore.googleapis.com";

/// The scope the key's access tokens are minted for.
const DATASTORE_SCOPE: &str = "https://www.googleapis.com/auth/datastore";

/// How long one unary call (or one `RunQuery` page) may take. `Listen`
/// has no deadline: it streams until cancelled.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(60);

/// How long connecting to the endpoint may take.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

pub type Client = FirestoreClient<GoogleAuthMiddleware>;

/// One authenticated channel per source; cheap to clone.
#[derive(Clone)]
pub struct Firestore {
    api: GoogleApi<Client>,
    /// `projects/{project}/databases/{database}`.
    pub database: String,
}

impl std::fmt::Debug for Firestore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Firestore")
            .field("database", &self.database)
            .finish_non_exhaustive()
    }
}

impl Firestore {
    /// Connect. The token is minted on the first call, so a wrong key shows
    /// up there (as `Config`), not here.
    pub async fn connect(config: &FirestoreConfig) -> Result<Self, SourceError> {
        let database = config.database_path()?;
        let (url, headers) = match &config.emulator_host {
            Some(host) => (
                format!("http://{host}"),
                GoogleAuthHeaders::from(Credentials::from(EmulatorCredentials::new())),
            ),
            None => {
                let key = config.service_account_key()?;
                let headers = GoogleAuthHeaders::from_service_account_key(
                    key,
                    vec![DATASTORE_SCOPE.to_owned()],
                )
                .await
                .map_err(|e| {
                    SourceError::Config(format!(
                        "the service account key cannot be used: {}",
                        short(&e.to_string())
                    ))
                })?;
                (FIRESTORE_API_URL.to_owned(), headers)
            }
        };
        let middleware = GoogleAuthMiddlewareLayer::new(headers, Some(database.clone()))
            .map_err(|e| SourceError::Fatal(format!("Firestore client setup: {e}")))?;
        let api = tokio::time::timeout(
            CONNECT_TIMEOUT,
            GoogleApi::from_function_with_middleware(FirestoreClient::new, &url, middleware),
        )
        .await
        .map_err(|_| SourceError::Transient(format!("connecting to {url} timed out")))?
        .map_err(|e| {
            SourceError::Transient(format!("connecting to {url}: {}", short(&e.to_string())))
        })?;
        Ok(Self { api, database })
    }

    /// A client for one call (clones share the channel).
    pub fn get(&self) -> Client {
        // Messages are up to 1 MiB per document; RunQuery batches several.
        self.api.get().max_decoding_message_size(64 * 1024 * 1024)
    }

    /// `projects/{project}/databases/{database}/documents`.
    pub fn documents_root(&self) -> String {
        format!("{}/documents", self.database)
    }
}

/// Error texts from the auth library can be long; keep the first line.
fn short(s: &str) -> String {
    let line = s.lines().next().unwrap_or_default();
    line.chars().take(300).collect()
}

/// Credentials for the emulator: its conventional stub token, so that a
/// client of the emulator looks up no Google credentials.
#[derive(Debug)]
struct EmulatorCredentials {
    /// The tag of the one header these credentials ever serve, so that the
    /// middleware keeps it cached.
    entity_tag: EntityTag,
}

impl EmulatorCredentials {
    fn new() -> Self {
        Self {
            entity_tag: EntityTag::new(),
        }
    }
}

impl CredentialsProvider for EmulatorCredentials {
    async fn headers(
        &self,
        extensions: tonic::codegen::http::Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        if extensions.get::<EntityTag>() == Some(&self.entity_tag) {
            return Ok(CacheableResource::NotModified);
        }
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            tonic::codegen::http::HeaderValue::from_static("Bearer owner"),
        );
        Ok(CacheableResource::New {
            entity_tag: self.entity_tag.clone(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}
