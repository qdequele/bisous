//! Connector options that make the server act with its **own** identity,
//! files or environment on a tenant's behalf.
//!
//! On a hosted server (Lab mode, or `CONNECT_HOST_POLICY=public`) a tenant
//! must never borrow what belongs to the server: its cloud credentials, the
//! files on its disk, its Kerberos ticket. Standalone, the operator owns
//! both sides, and the global scope keeps them.
//!
//! What is refused, per connector:
//!
//! - **DynamoDB** `credentials: default` (or no `credentials`): the AWS
//!   default provider chain of the host (env, `~/.aws`, IMDS, ECS/EKS web
//!   identity). A tenant gives a role to assume or static keys.
//! - **MongoDB** URI options that read local files (`tlsCAFile`,
//!   `tlsCertificateKeyFile`, `tlsCertificateKeyFilePassword`, `tlsCRLFile`,
//!   the legacy `sslCAFile`, `sslPEMKeyFile`, `sslPEMKeyPassword`) and the
//!   auth mechanisms that fall back to the host's identity or files
//!   (`MONGODB-AWS`: env and IMDS; `GSSAPI`: the host's Kerberos;
//!   `MONGODB-X509`: a local client certificate; `MONGODB-OIDC`: the host's
//!   cloud token).
//! - **Postgres / Supabase** URL parameters naming local files or services
//!   (`sslrootcert`, `sslcert`, `sslkey`, `sslcrl`, `sslcrldir`, `passfile`,
//!   `service`, `servicefile`). The connector does not read them (the CA
//!   goes inline in `ca_cert`, Unix sockets are refused by `endpoints`);
//!   they are refused so nobody believes they take effect.
//!
//! Nothing to refuse: **Kafka** takes PEM inline (`ca_pem`, `cert_pem`,
//! `key_pem`) and only `PLAIN` / `SCRAM-*` SASL, and its config refuses
//! unknown keys (no raw librdkafka property such as `ssl.ca.location`);
//! **MySQL** takes the CA inline (`tls.ca_pem`) and never uses a socket;
//! **Firestore** requires an inline service account key (never the host's
//! Application Default Credentials) and sends it only to Google's token
//! endpoint, whatever the key's `token_uri` says.

use meili_connect_core::{SourceKind, SourceSpec};
use serde_json::Value;

const MONGO_FILE_OPTIONS: &[&str] = &[
    "tlscafile",
    "tlscertificatekeyfile",
    "tlscertificatekeyfilepassword",
    "tlscrlfile",
    "sslcafile",
    "sslpemkeyfile",
    "sslpemkeypassword",
];
const MONGO_AMBIENT_MECHANISMS: &[&str] =
    &["mongodb-aws", "gssapi", "mongodb-x509", "mongodb-oidc"];
const PG_FILE_PARAMS: &[&str] = &[
    "sslrootcert",
    "sslcert",
    "sslkey",
    "sslcrl",
    "sslcrldir",
    "passfile",
    "service",
    "servicefile",
];

/// The query parameters of a URL, keys lowercased, values raw.
fn query_params(url: &str) -> Vec<(String, String)> {
    let Some((_, query)) = url.split_once('?') else {
        return Vec::new();
    };
    let query = query.split('#').next().unwrap_or_default();
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (k.trim().to_ascii_lowercase(), v.trim().to_owned())
        })
        .collect()
}

/// Why this source config would use the server's own identity, files or
/// environment; `None` when it does not.
pub fn ambient_identity(spec: &SourceSpec) -> Option<String> {
    match spec.kind {
        SourceKind::Dynamodb => {
            let credentials = spec.config.get("credentials");
            let kind = credentials
                .and_then(|c| c.get("type"))
                .and_then(Value::as_str);
            match (credentials, kind) {
                (None | Some(Value::Null), _) | (_, Some("default")) => Some(
                    "DynamoDB `credentials: default` would use this server's own AWS identity: \
                     give a role to assume ({\"type\": \"assume_role\", \"role_arn\": \
                     \"arn:aws:iam::<account>:role/<name>\"}, trusted as GET \
                     /sources/dynamodb/trust shows) or static keys ({\"type\": \"static\", \"access_key_id\": \"...\", \
                     \"secret_access_key\": \"...\"})"
                        .to_owned(),
                ),
                _ => None,
            }
        }
        SourceKind::Mongodb => {
            let uri = spec.config.get("uri").and_then(Value::as_str)?;
            for (key, value) in query_params(uri) {
                if MONGO_FILE_OPTIONS.contains(&key.as_str()) {
                    return Some(format!(
                        "the MongoDB URI option `{key}` reads a file on this server: it is not \
                         available on a hosted server; use a URI without it (Atlas and \
                         public certificates need none)"
                    ));
                }
                if key == "authmechanism"
                    && MONGO_AMBIENT_MECHANISMS.contains(&value.to_ascii_lowercase().as_str())
                {
                    return Some(format!(
                        "the MongoDB auth mechanism `{value}` would use this server's own \
                         identity or files: use a username and password (SCRAM) in the URI"
                    ));
                }
            }
            None
        }
        SourceKind::Postgres | SourceKind::Supabase => {
            let url = spec.config.get("url").and_then(Value::as_str)?;
            query_params(url)
                .into_iter()
                // `sslrootcert=system` is libpq's keyword for the system
                // trust store, not a file: the server's roots are used anyway.
                .find(|(k, v)| {
                    PG_FILE_PARAMS.contains(&k.as_str())
                        && !(k == "sslrootcert" && v.eq_ignore_ascii_case("system"))
                })
                .map(|(key, _)| {
                    format!(
                        "the connection URL parameter `{key}` names a file or service on this \
                         server, which a hosted server never reads: paste the CA certificate \
                         as `ca_cert` instead"
                    )
                })
        }
        SourceKind::Kafka | SourceKind::Mysql | SourceKind::Firestore => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(kind: SourceKind, config: Value) -> SourceSpec {
        SourceSpec { kind, config }
    }

    #[test]
    fn dynamodb_default_credentials_are_the_servers() {
        for config in [
            json!({"region": "eu-west-1"}),
            json!({"region": "eu-west-1", "credentials": null}),
            json!({"region": "eu-west-1", "credentials": {"type": "default"}}),
        ] {
            let m = ambient_identity(&spec(SourceKind::Dynamodb, config)).unwrap_or_default();
            assert!(m.contains("assume_role") && m.contains("static"), "{m}");
        }
        for config in [
            json!({"region": "x", "credentials": {"type": "assume_role", "role_arn": "arn:aws:iam::1:role/r"}}),
            json!({"region": "x", "credentials": {"type": "static", "access_key_id": "A", "secret_access_key": "S"}}),
        ] {
            assert_eq!(ambient_identity(&spec(SourceKind::Dynamodb, config)), None);
        }
    }

    #[test]
    fn mongodb_file_options_and_host_identity_mechanisms_are_refused() {
        for uri in [
            "mongodb://u:p@h/?tlsCAFile=/etc/ssl/ca.pem",
            "mongodb://h/?tls=true&TLSCertificateKeyFile=/root/key.pem",
            "mongodb+srv://c.example.net/?sslCAFile=/x",
            "mongodb://h/?authMechanism=MONGODB-AWS",
            "mongodb://h/?authSource=$external&authMechanism=gssapi",
            "mongodb://h/?authMechanism=MONGODB-X509",
            "mongodb://h/?authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp",
        ] {
            assert!(
                ambient_identity(&spec(SourceKind::Mongodb, json!({"uri": uri}))).is_some(),
                "{uri}"
            );
        }
        for uri in [
            "mongodb+srv://u:p@c.example.net/?retryWrites=true&w=majority",
            "mongodb://u:p@h/?authMechanism=SCRAM-SHA-256&tls=true",
        ] {
            assert_eq!(
                ambient_identity(&spec(SourceKind::Mongodb, json!({"uri": uri}))),
                None,
                "{uri}"
            );
        }
    }

    #[test]
    fn postgres_file_parameters_are_refused() {
        for url in [
            "postgres://u:p@h/db?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem",
            "postgresql://u@h/db?passfile=/root/.pgpass",
            "postgres://h/db?service=prod",
            // Only the bare keyword is the system store; a file named
            // like it is still a file.
            "postgres://u:p@h/db?sslrootcert=./system",
        ] {
            assert!(
                ambient_identity(&spec(SourceKind::Postgres, json!({"url": url}))).is_some(),
                "{url}"
            );
        }
        // PlanetScale's connection strings name the system trust store.
        assert_eq!(
            ambient_identity(&spec(
                SourceKind::Postgres,
                json!({"url": "postgresql://u.b:p@x-useast1-1.horizon.psdb.cloud:5432/postgres?sslmode=verify-full&sslrootcert=system&sslnegotiation=direct"})
            )),
            None
        );
        assert_eq!(
            ambient_identity(&spec(
                SourceKind::Supabase,
                json!({"url": "postgres://u:p@db.x.supabase.co:5432/postgres?sslmode=require"})
            )),
            None
        );
        assert_eq!(
            ambient_identity(&spec(
                SourceKind::Postgres,
                json!({"host": "h", "user": "u"})
            )),
            None
        );
    }

    #[test]
    fn kafka_and_mysql_have_nothing_ambient() {
        assert_eq!(
            ambient_identity(&spec(
                SourceKind::Kafka,
                json!({"bootstrap_servers": ["k:9092"]})
            )),
            None
        );
        assert_eq!(
            ambient_identity(&spec(SourceKind::Mysql, json!({"host": "h", "user": "u"}))),
            None
        );
    }
}
