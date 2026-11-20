//! The slice of Vitess's protobuf API that VStream needs, written by hand
//! from `proto/{vtgate,binlogdata,query,topodata}.proto` (Vitess
//! `release-21.0`): field numbers and types match the upstream files
//! exactly; fields this connector never reads or sends are left out, which
//! proto3 decoding skips. No `protoc` at build time.

#![allow(clippy::doc_markdown)]

use prost::Message;

/// `vtgate.VStreamRequest`.
#[derive(Clone, PartialEq, Message)]
pub struct VStreamRequest {
    /// `topodata.TabletType`.
    #[prost(enumeration = "TabletType", tag = "2")]
    pub tablet_type: i32,
    #[prost(message, optional, tag = "3")]
    pub vgtid: Option<VGtid>,
    #[prost(message, optional, tag = "4")]
    pub filter: Option<Filter>,
    #[prost(message, optional, tag = "5")]
    pub flags: Option<VStreamFlags>,
}

/// `vtgate.VStreamFlags`.
#[derive(Clone, PartialEq, Message)]
pub struct VStreamFlags {
    #[prost(bool, tag = "1")]
    pub minimize_skew: bool,
    /// Seconds between heartbeats while idle.
    #[prost(uint32, tag = "2")]
    pub heartbeat_interval: u32,
    #[prost(bool, tag = "3")]
    pub stop_on_reshard: bool,
    #[prost(string, tag = "4")]
    pub cells: String,
}

/// `vtgate.VStreamResponse`.
#[derive(Clone, PartialEq, Message)]
pub struct VStreamResponse {
    #[prost(message, repeated, tag = "1")]
    pub events: Vec<VEvent>,
}

/// `binlogdata.VEvent`.
#[derive(Clone, PartialEq, Message)]
pub struct VEvent {
    #[prost(enumeration = "VEventType", tag = "1")]
    pub r#type: i32,
    /// Binlog timestamp, seconds; 0 when unknown.
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
    #[prost(string, tag = "3")]
    pub gtid: String,
    /// The SQL of a DDL event.
    #[prost(string, tag = "4")]
    pub statement: String,
    #[prost(message, optional, tag = "5")]
    pub row_event: Option<RowEvent>,
    #[prost(message, optional, tag = "6")]
    pub field_event: Option<FieldEvent>,
    #[prost(message, optional, tag = "7")]
    pub vgtid: Option<VGtid>,
    /// When the event was sent, nanoseconds.
    #[prost(int64, tag = "20")]
    pub current_time: i64,
    #[prost(string, tag = "22")]
    pub keyspace: String,
    #[prost(string, tag = "23")]
    pub shard: String,
}

/// `binlogdata.VEventType`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum VEventType {
    Unknown = 0,
    Gtid = 1,
    Begin = 2,
    Commit = 3,
    Rollback = 4,
    Ddl = 5,
    Insert = 6,
    Replace = 7,
    Update = 8,
    Delete = 9,
    Set = 10,
    Other = 11,
    Row = 12,
    Field = 13,
    Heartbeat = 14,
    Vgtid = 15,
    Journal = 16,
    Version = 17,
    Lastpk = 18,
    Savepoint = 19,
    CopyCompleted = 20,
}

/// `topodata.TabletType` (the values a VStream may read from).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
#[repr(i32)]
pub enum TabletType {
    Unknown = 0,
    Primary = 1,
    Replica = 2,
    Rdonly = 3,
}

/// `binlogdata.VGtid`: one position per shard.
#[derive(Clone, PartialEq, Message)]
pub struct VGtid {
    #[prost(message, repeated, tag = "1")]
    pub shard_gtids: Vec<ShardGtid>,
}

/// `binlogdata.ShardGtid`.
#[derive(Clone, PartialEq, Message)]
pub struct ShardGtid {
    #[prost(string, tag = "1")]
    pub keyspace: String,
    /// Empty: every shard of the keyspace (with `gtid` "current" or "").
    #[prost(string, tag = "2")]
    pub shard: String,
    /// `MySQL56/<uuid>:1-42`, or "current".
    #[prost(string, tag = "3")]
    pub gtid: String,
}

/// `binlogdata.Filter`.
#[derive(Clone, PartialEq, Message)]
pub struct Filter {
    #[prost(message, repeated, tag = "1")]
    pub rules: Vec<Rule>,
}

/// `binlogdata.Rule`.
#[derive(Clone, PartialEq, Message)]
pub struct Rule {
    /// A table name, or a regular expression starting with `/`.
    #[prost(string, tag = "1")]
    pub r#match: String,
    /// `select * from t`, or empty for every column.
    #[prost(string, tag = "2")]
    pub filter: String,
}

/// `binlogdata.RowEvent`.
#[derive(Clone, PartialEq, Message)]
pub struct RowEvent {
    #[prost(string, tag = "1")]
    pub table_name: String,
    #[prost(message, repeated, tag = "2")]
    pub row_changes: Vec<RowChange>,
    #[prost(string, tag = "3")]
    pub keyspace: String,
    #[prost(string, tag = "4")]
    pub shard: String,
}

/// `binlogdata.RowChange`: `before` only is a delete, `after` only an
/// insert, both an update.
#[derive(Clone, PartialEq, Message)]
pub struct RowChange {
    #[prost(message, optional, tag = "1")]
    pub before: Option<Row>,
    #[prost(message, optional, tag = "2")]
    pub after: Option<Row>,
}

/// `binlogdata.FieldEvent`: the columns of the next row events of a table.
#[derive(Clone, PartialEq, Message)]
pub struct FieldEvent {
    #[prost(string, tag = "1")]
    pub table_name: String,
    #[prost(message, repeated, tag = "2")]
    pub fields: Vec<Field>,
    #[prost(string, tag = "3")]
    pub keyspace: String,
    #[prost(string, tag = "4")]
    pub shard: String,
    /// ENUM and SET values are already strings (Vitess 20+).
    #[prost(bool, tag = "25")]
    pub enum_set_string_values: bool,
}

/// `query.Field`.
#[derive(Clone, PartialEq, Message)]
pub struct Field {
    #[prost(string, tag = "1")]
    pub name: String,
    /// `query.Type`.
    #[prost(int32, tag = "2")]
    pub r#type: i32,
}

/// `query.Row`: values concatenated, `lengths[i]` = -1 for NULL.
#[derive(Clone, PartialEq, Message)]
pub struct Row {
    #[prost(sint64, repeated, tag = "1")]
    pub lengths: Vec<i64>,
    #[prost(bytes = "vec", tag = "2")]
    pub values: Vec<u8>,
}

impl Row {
    /// The cells, in field order: `None` is NULL. `None` overall when the
    /// lengths do not fit the values (a malformed row).
    pub fn cells(&self) -> Option<Vec<Option<&[u8]>>> {
        let mut out = Vec::with_capacity(self.lengths.len());
        let mut at = 0usize;
        for &len in &self.lengths {
            if len < 0 {
                out.push(None);
                continue;
            }
            let end = at.checked_add(usize::try_from(len).ok()?)?;
            out.push(Some(self.values.get(at..end)?));
            at = end;
        }
        Some(out)
    }
}

/// The gRPC client of `vtgateservice.Vitess`, `VStream` only.
pub mod client {
    use tonic::codegen::http;

    use super::{VStreamRequest, VStreamResponse};

    #[derive(Debug, Clone)]
    pub struct VitessClient<T> {
        inner: tonic::client::Grpc<T>,
    }

    impl<T> VitessClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<tonic::codegen::StdError>,
        T::ResponseBody: tonic::codegen::Body<Data = tonic::codegen::Bytes> + Send + 'static,
        <T::ResponseBody as tonic::codegen::Body>::Error: Into<tonic::codegen::StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            Self {
                inner: tonic::client::Grpc::new(inner),
            }
        }

        pub fn accept_compressed(mut self, encoding: tonic::codec::CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }

        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }

        pub async fn vstream(
            &mut self,
            request: tonic::Request<VStreamRequest>,
        ) -> Result<tonic::Response<tonic::codec::Streaming<VStreamResponse>>, tonic::Status>
        {
            self.inner.ready().await.map_err(|e| {
                tonic::Status::unavailable(format!(
                    "the VStream service is not ready: {}",
                    e.into()
                ))
            })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static("/vtgateservice.Vitess/VStream");
            self.inner.server_streaming(request, path, codec).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_cells_follow_lengths() {
        let row = Row {
            lengths: vec![1, -1, 0, 3],
            values: b"7abc".to_vec(),
        };
        assert_eq!(
            row.cells().unwrap(),
            vec![Some(&b"7"[..]), None, Some(&b""[..]), Some(&b"abc"[..])]
        );
        let bad = Row {
            lengths: vec![5],
            values: b"ab".to_vec(),
        };
        assert!(bad.cells().is_none());
    }

    /// The wire tags match Vitess's: a request encoded here decodes field by
    /// field as upstream lays it out (tag 3 = vgtid, 4 = filter, 5 = flags).
    #[test]
    fn request_encodes_with_upstream_tags() {
        let req = VStreamRequest {
            tablet_type: TabletType::Replica as i32,
            vgtid: Some(VGtid {
                shard_gtids: vec![ShardGtid {
                    keyspace: "ks".into(),
                    shard: "-80".into(),
                    gtid: "current".into(),
                }],
            }),
            filter: Some(Filter {
                rules: vec![Rule {
                    r#match: "t".into(),
                    filter: "select * from `t`".into(),
                }],
            }),
            flags: Some(VStreamFlags {
                heartbeat_interval: 5,
                ..Default::default()
            }),
        };
        let bytes = req.encode_to_vec();
        // field 2 (varint) = 2, then field 3 (length-delimited).
        assert_eq!(&bytes[..3], &[0x10, 0x02, 0x1a]);
        assert_eq!(VStreamRequest::decode(bytes.as_slice()).unwrap(), req);
    }
}
