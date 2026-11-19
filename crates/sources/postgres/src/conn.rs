//! Connections: `tokio-postgres` for SQL, `pgwire-replication` for the
//! replication protocol. Both use the same TLS policy and session settings.

use std::io::Write as _;
use std::sync::Arc;
use std::time::Duration;

use meili_connect_core::SourceError;
use pgwire_replication::{Lsn, ReplicationConfig, TlsConfig};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tempfile::NamedTempFile;
use tokio_util::sync::CancellationToken;

use crate::config::{ConnParams, SslMode};
use crate::error::{self, Context};
use crate::ident;

/// Pinned so text output is the same everywhere (see `convert`).
pub const SESSION_OPTIONS: &str = "-c datestyle=ISO,YMD -c timezone=UTC -c intervalstyle=iso_8601 -c bytea_output=hex -c extra_float_digits=3";

pub const APPLICATION_NAME: &str = "meili-connect";

const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// The CA certificate written to a private temp file, because
/// `pgwire-replication` reads it from a path. Removed on drop.
#[derive(Debug, Clone, Default)]
pub struct CaFile(Option<Arc<NamedTempFile>>);

impl CaFile {
    pub fn new(pem: Option<&str>) -> Result<Self, SourceError> {
        let Some(pem) = pem else {
            return Ok(Self(None));
        };
        let mut file = tempfile::Builder::new()
            .prefix("meili-connect-ca-")
            .suffix(".pem")
            .tempfile()
            .map_err(|e| SourceError::Fatal(format!("cannot write the CA certificate: {e}")))?;
        file.write_all(pem.as_bytes())
            .and_then(|()| file.flush())
            .map_err(|e| SourceError::Fatal(format!("cannot write the CA certificate: {e}")))?;
        Ok(Self(Some(Arc::new(file))))
    }

    fn path(&self) -> Option<std::path::PathBuf> {
        self.0.as_ref().map(|f| f.path().to_path_buf())
    }
}

/// One SQL connection. The driver task stops when the client is dropped.
pub struct Sql {
    pub client: tokio_postgres::Client,
    supabase: bool,
}

impl Sql {
    pub fn err(&self, e: &tokio_postgres::Error) -> SourceError {
        error::from_pg(e, Context::Query, self.supabase)
    }

    pub async fn simple(
        &self,
        sql: &str,
    ) -> Result<Vec<tokio_postgres::SimpleQueryMessage>, SourceError> {
        self.client
            .simple_query(sql)
            .await
            .map_err(|e| self.err(&e))
    }

    pub async fn query(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<Vec<tokio_postgres::Row>, SourceError> {
        self.client
            .query(sql, params)
            .await
            .map_err(|e| self.err(&e))
    }

    pub async fn execute(
        &self,
        sql: &str,
        params: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<u64, SourceError> {
        self.client
            .execute(sql, params)
            .await
            .map_err(|e| self.err(&e))
    }
}

pub async fn connect_sql(p: &ConnParams, cancel: &CancellationToken) -> Result<Sql, SourceError> {
    let mut cfg = tokio_postgres::Config::new();
    cfg.host(&p.host)
        .port(p.port)
        .user(&p.user)
        .dbname(&p.database)
        .application_name(APPLICATION_NAME)
        .options(SESSION_OPTIONS)
        .connect_timeout(CONNECT_TIMEOUT)
        .keepalives(true)
        .keepalives_idle(Duration::from_secs(30));
    if !p.password.is_empty() {
        cfg.password(p.password.expose());
    }
    let supabase = p.supabase;
    let map = |e: tokio_postgres::Error| error::from_pg(&e, Context::Query, supabase);

    let client = if p.sslmode == SslMode::Disable {
        cfg.ssl_mode(tokio_postgres::config::SslMode::Disable);
        let fut = cfg.connect(tokio_postgres::NoTls);
        let (client, conn) = tokio::select! {
            r = fut => r.map_err(map)?,
            () = cancel.cancelled() => return Err(SourceError::Transient("cancelled".into())),
        };
        spawn_driver(conn);
        client
    } else {
        cfg.ssl_mode(tokio_postgres::config::SslMode::Require);
        let tls = tokio_postgres_rustls::MakeRustlsConnect::new(rustls_config(p)?);
        let fut = cfg.connect(tls);
        let (client, conn) = tokio::select! {
            r = fut => r.map_err(map)?,
            () = cancel.cancelled() => return Err(SourceError::Transient("cancelled".into())),
        };
        spawn_driver(conn);
        client
    };
    Ok(Sql { client, supabase })
}

fn spawn_driver<F>(conn: F)
where
    F: std::future::Future<Output = Result<(), tokio_postgres::Error>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            tracing::debug!(error = %e, "postgres connection closed");
        }
    });
}

/// The replication session for `slot`/`publication`, starting at `start`.
pub fn replication_config(
    p: &ConnParams,
    ca: &CaFile,
    slot: &str,
    publication: &str,
    start: Lsn,
) -> ReplicationConfig {
    let tls = match p.sslmode {
        SslMode::Disable => TlsConfig::disabled(),
        SslMode::Require => TlsConfig::require(),
        SslMode::VerifyCa => TlsConfig::verify_ca(ca.path()),
        SslMode::VerifyFull => TlsConfig::verify_full(ca.path()),
    };
    // `publication_names` is split like an identifier list: quote it so
    // mixed-case names survive.
    ReplicationConfig::new(
        p.host.clone(),
        p.user.clone(),
        p.password.expose().to_owned(),
        p.database.clone(),
        slot.to_owned(),
        ident::quote_ident(publication),
    )
    .with_port(p.port)
    .with_tls(tls)
    .with_start_lsn(start)
    .with_options(SESSION_OPTIONS)
    .with_application_name(APPLICATION_NAME)
    .with_status_interval(Duration::from_secs(2))
    .with_wakeup_interval(Duration::from_secs(5))
    .with_buffer_size(256)
}

fn rustls_config(p: &ConnParams) -> Result<rustls::ClientConfig, SourceError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| SourceError::Fatal(format!("TLS setup: {e}")))?;

    let mut roots = rustls::RootCertStore::empty();
    match &p.ca_cert {
        Some(pem) => {
            use rustls::pki_types::pem::PemObject;
            let certs = CertificateDer::pem_slice_iter(pem.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .map_err(|_| {
                    SourceError::Config("ca_cert is not a valid PEM certificate".into())
                })?;
            let (added, _) = roots.add_parsable_certificates(certs);
            if added == 0 {
                return Err(SourceError::Config(
                    "ca_cert holds no usable certificate".into(),
                ));
            }
        }
        None => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
    }

    let config = match p.sslmode {
        SslMode::Require | SslMode::Disable => builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
            .with_no_client_auth(),
        SslMode::VerifyCa => {
            let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(
                Arc::new(roots),
                provider,
            )
            .build()
            .map_err(|e| SourceError::Config(format!("TLS setup: {e}")))?;
            builder
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(ChainOnly(inner)))
                .with_no_client_auth()
        }
        SslMode::VerifyFull => builder.with_root_certificates(roots).with_no_client_auth(),
    };
    Ok(config)
}

/// `sslmode=require`: encrypted, not authenticated (libpq semantics).
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoVerify {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// `sslmode=verify-ca`: the chain must be trusted, the name is not checked.
#[derive(Debug)]
struct ChainOnly(Arc<rustls::client::WebPkiServerVerifier>);

impl ServerCertVerifier for ChainOnly {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        match self
            .0
            .verify_server_cert(end_entity, intermediates, server_name, ocsp, now)
        {
            Err(rustls::Error::InvalidCertificate(rustls::CertificateError::NotValidForName))
            | Err(rustls::Error::InvalidCertificate(
                rustls::CertificateError::NotValidForNameContext { .. },
            )) => Ok(ServerCertVerified::assertion()),
            other => other,
        }
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.0.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.supported_verify_schemes()
    }
}
