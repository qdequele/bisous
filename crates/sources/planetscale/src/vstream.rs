//! The VStream side: connection, requests and positions.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use meili_connect_core::{Checkpoint, SourceError};
use serde::{Deserialize, Serialize};
use tonic::codec::{CompressionEncoding, Streaming};
use tonic::metadata::{Ascii, MetadataValue};
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

use crate::config::PlanetScaleConfig;
use crate::error;
use crate::proto::client::VitessClient;
use crate::proto::{
    Filter, Rule, ShardGtid, VEventType, VGtid, VStreamFlags, VStreamRequest, VStreamResponse,
};
use crate::schema::quote_ident;

/// Idle VStreams get a heartbeat this often: the stream proves it is alive
/// and the lag drops to 0.
pub const HEARTBEAT_SECS: u32 = 5;
/// Largest VStream message accepted: a batch of big rows, bounded.
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Sends the user and password on every call, as PlanetScale's own
/// connector does: `authorization: Basic base64(user:password)`.
#[derive(Clone)]
pub struct BasicAuth(MetadataValue<Ascii>);

impl std::fmt::Debug for BasicAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BasicAuth(<redacted>)")
    }
}

impl BasicAuth {
    pub fn new(user: &str, password: &str) -> Result<Self, SourceError> {
        let token = BASE64.encode(format!("{user}:{password}"));
        let mut value: MetadataValue<Ascii> = format!("Basic {token}")
            .parse()
            .map_err(|_| SourceError::Config("the user name is not valid in a header".into()))?;
        value.set_sensitive(true);
        Ok(Self(value))
    }
}

impl Interceptor for BasicAuth {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        req.metadata_mut().insert("authorization", self.0.clone());
        Ok(req)
    }
}

pub type Client = VitessClient<InterceptedService<Channel, BasicAuth>>;

pub async fn connect(cfg: &PlanetScaleConfig) -> Result<Client, SourceError> {
    let scheme = if cfg.tls { "https" } else { "http" };
    let mut endpoint = Endpoint::from_shared(format!("{scheme}://{}:{}", cfg.host, cfg.grpc_port))
        .map_err(|_| SourceError::Config(format!("invalid host {}", cfg.host)))?
        .connect_timeout(CONNECT_TIMEOUT)
        .tcp_keepalive(Some(Duration::from_secs(30)))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(20));
    if cfg.tls {
        endpoint = endpoint
            .tls_config(
                ClientTlsConfig::new()
                    .with_webpki_roots()
                    .domain_name(cfg.host.clone()),
            )
            .map_err(|e| SourceError::Config(format!("TLS setup failed: {e}")))?;
    }
    let channel = endpoint.connect().await.map_err(|e| error::transport(&e))?;
    let auth = BasicAuth::new(&cfg.user, cfg.password.expose())?;
    Ok(VitessClient::new(InterceptedService::new(channel, auth)))
}

/// A stored position: each shard's GTID set. The JSON of a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Position {
    pub shards: Vec<ShardPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardPosition {
    pub keyspace: String,
    pub shard: String,
    pub gtid: String,
}

impl Position {
    pub fn from_vgtid(v: &VGtid) -> Self {
        Self {
            shards: v
                .shard_gtids
                .iter()
                .map(|s| ShardPosition {
                    keyspace: s.keyspace.clone(),
                    shard: s.shard.clone(),
                    gtid: s.gtid.clone(),
                })
                .collect(),
        }
    }

    pub fn to_vgtid(&self) -> VGtid {
        VGtid {
            shard_gtids: self
                .shards
                .iter()
                .map(|s| ShardGtid {
                    keyspace: s.keyspace.clone(),
                    shard: s.shard.clone(),
                    gtid: s.gtid.clone(),
                })
                .collect(),
        }
    }

    pub fn to_checkpoint(&self) -> Result<Checkpoint, SourceError> {
        Checkpoint::new(self).map_err(|e| SourceError::Fatal(format!("position: {e}")))
    }

    /// A checkpoint of this connector, for `database`: every shard of it,
    /// each with a concrete GTID set.
    pub fn from_checkpoint(cp: &Checkpoint, database: &str) -> Result<Self, SourceError> {
        let pos: Position = cp.decode().map_err(|_| {
            SourceError::PositionLost("the checkpoint is not a VStream position".into())
        })?;
        if pos.shards.is_empty()
            || pos
                .shards
                .iter()
                .any(|s| s.keyspace != database || s.gtid.is_empty() || s.gtid == "current")
        {
            return Err(SourceError::PositionLost(format!(
                "the checkpoint is not a position of the database {database}"
            )));
        }
        Ok(pos)
    }
}

/// The request that streams `tables` of the database from `vgtid`.
pub fn request(cfg: &PlanetScaleConfig, vgtid: VGtid, tables: &[String]) -> VStreamRequest {
    VStreamRequest {
        tablet_type: cfg.tablet_type.proto() as i32,
        vgtid: Some(vgtid),
        filter: Some(Filter {
            rules: tables
                .iter()
                .map(|t| Rule {
                    r#match: t.clone(),
                    filter: format!("select * from {}", quote_ident(t)),
                })
                .collect(),
        }),
        flags: Some(VStreamFlags {
            minimize_skew: false,
            heartbeat_interval: HEARTBEAT_SECS,
            stop_on_reshard: false,
            cells: cfg.cells(),
        }),
    }
}

pub async fn open_stream(
    client: &mut Client,
    req: VStreamRequest,
) -> Result<Streaming<VStreamResponse>, SourceError> {
    client
        .vstream(tonic::Request::new(req))
        .await
        .map(tonic::Response::into_inner)
        .map_err(|s| error::grpc(&s))
}

/// Every shard's current position: a VStream from "current" on all shards,
/// read until its first VGTID event, then dropped.
pub async fn current_position(
    cfg: &PlanetScaleConfig,
    client: &mut Client,
    tables: &[String],
) -> Result<Position, SourceError> {
    let vgtid = VGtid {
        shard_gtids: vec![ShardGtid {
            keyspace: cfg.database.clone(),
            shard: String::new(),
            gtid: "current".into(),
        }],
    };
    let mut stream = open_stream(client, request(cfg, vgtid, tables)).await?;
    let read = async {
        loop {
            match stream.message().await {
                Ok(Some(resp)) => {
                    if let Some(v) = resp
                        .events
                        .iter()
                        .find(|e| e.r#type == VEventType::Vgtid as i32)
                        .and_then(|e| e.vgtid.as_ref())
                    {
                        return Ok(Position::from_vgtid(v));
                    }
                }
                Ok(None) => {
                    return Err(SourceError::Transient(
                        "VStream ended before giving a position".into(),
                    ));
                }
                Err(s) => return Err(error::grpc(&s)),
            }
        }
    };
    match tokio::time::timeout(Duration::from_secs(60), read).await {
        Ok(r) => r,
        Err(_) => Err(SourceError::Transient(
            "VStream gave no position within 60 s".into(),
        )),
    }
}

/// Accept gzip from vtgate, and messages up to the cap.
pub fn tune(client: Client) -> Client {
    client
        .accept_compressed(CompressionEncoding::Gzip)
        .max_decoding_message_size(MAX_MESSAGE_BYTES)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn cfg() -> PlanetScaleConfig {
        PlanetScaleConfig::from_json(&json!({
            "host": "aws.connect.psdb.cloud", "database": "shop", "user": "u", "password": "p"
        }))
        .unwrap()
    }

    #[test]
    fn basic_auth_is_the_standard_header() {
        let auth = BasicAuth::new("u", "p").unwrap();
        assert_eq!(auth.0.to_str().unwrap(), "Basic dTpw");
        assert!(!format!("{auth:?}").contains("dTpw"));
    }

    #[test]
    fn request_filters_the_mapped_tables() {
        let req = request(
            &cfg(),
            VGtid::default(),
            &["orders".into(), "we`ird".into()],
        );
        let rules = req.filter.unwrap().rules;
        assert_eq!(rules[0].r#match, "orders");
        assert_eq!(rules[1].filter, "select * from `we``ird`");
        let flags = req.flags.unwrap();
        assert_eq!(flags.cells, crate::config::PLANETSCALE_CELL);
        assert_eq!(flags.heartbeat_interval, HEARTBEAT_SECS);
    }

    #[test]
    fn positions_round_trip_and_are_checked() {
        let pos = Position {
            shards: vec![
                ShardPosition {
                    keyspace: "shop".into(),
                    shard: "-80".into(),
                    gtid: "MySQL56/a:1-5".into(),
                },
                ShardPosition {
                    keyspace: "shop".into(),
                    shard: "80-".into(),
                    gtid: "MySQL56/b:1-9".into(),
                },
            ],
        };
        let cp = pos.to_checkpoint().unwrap();
        assert_eq!(Position::from_checkpoint(&cp, "shop").unwrap(), pos);
        assert_eq!(Position::from_vgtid(&pos.to_vgtid()), pos);
        for bad in [
            Checkpoint(json!({"lsn": "0/1"})),
            Checkpoint(json!({"shards": []})),
            Checkpoint(
                json!({"shards": [{"keyspace": "shop", "shard": "-80", "gtid": "current"}]}),
            ),
        ] {
            assert!(matches!(
                Position::from_checkpoint(&bad, "shop"),
                Err(SourceError::PositionLost(_))
            ));
        }
        assert!(matches!(
            Position::from_checkpoint(&cp, "other"),
            Err(SourceError::PositionLost(_))
        ));
    }
}
