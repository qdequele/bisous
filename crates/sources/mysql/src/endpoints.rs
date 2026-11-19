//! The address a MySQL source connects to, for the server's host policy
//! (`SourceFactory::endpoints`).

use meili_connect_core::{Endpoint, SourceError, SourceKind, SourceSpec};

use crate::config::MysqlConfig;

/// The one host and port of the config. Both the SQL connections and the
/// binlog connection go there.
pub(crate) fn endpoints(spec: &SourceSpec) -> Result<Vec<Endpoint>, SourceError> {
    if spec.kind != SourceKind::Mysql {
        return Err(SourceError::Config(format!(
            "the mysql connector cannot read a {} config",
            spec.kind.as_str()
        )));
    }
    let config = MysqlConfig::from_json(&spec.config)?;
    let host = config.host.trim();
    if host.starts_with('/') {
        return Err(SourceError::Config(
            "mysql config: Unix sockets are not supported, give a host name or address".into(),
        ));
    }
    Ok(vec![Endpoint::new(host, config.port)])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(config: serde_json::Value) -> SourceSpec {
        SourceSpec {
            kind: SourceKind::Mysql,
            config,
        }
    }

    #[test]
    fn host_and_port() {
        assert_eq!(
            endpoints(&spec(json!({"host": "DB.example.com", "user": "u"}))).unwrap(),
            vec![Endpoint::new("db.example.com", 3306)]
        );
        assert_eq!(
            endpoints(&spec(
                json!({"host": "10.0.0.1", "port": 3307, "user": "u", "password": "p"})
            ))
            .unwrap(),
            vec![Endpoint::new("10.0.0.1", 3307)]
        );
        assert!(matches!(
            endpoints(&spec(json!({"host": "/tmp/mysql.sock", "user": "u"}))),
            Err(SourceError::Config(_))
        ));
        assert!(matches!(
            endpoints(&spec(json!({"user": "u"}))),
            Err(SourceError::Config(_))
        ));
        let wrong = SourceSpec {
            kind: SourceKind::Postgres,
            config: json!({"host": "h", "user": "u"}),
        };
        assert!(endpoints(&wrong).is_err());
    }
}
