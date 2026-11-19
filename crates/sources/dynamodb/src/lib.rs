//! DynamoDB source: `Scan` snapshot and DynamoDB Streams.
//!
//! - **Config** ([`DynamoConfig`]): region, optional endpoint (DynamoDB
//!   Local), credentials (default chain, static keys, or an assumed role
//!   with an external id), scan and stream tuning. Tables come from the
//!   sync's stream mappings: `collection` is the table name.
//! - **Prerequisite**: DynamoDB Streams enabled on every table with view
//!   type `NEW_IMAGE` or `NEW_AND_OLD_IMAGES`. The connector never enables
//!   it itself.
//! - **IAM**: `dynamodb:DescribeTable`, `dynamodb:Scan` on the tables;
//!   `dynamodb:DescribeStream`, `dynamodb:GetShardIterator`,
//!   `dynamodb:GetRecords` on `<table arn>/stream/*`; `dynamodb:ListTables`
//!   is optional (discovery of unmapped tables).
//! - **Checkpoint**: `{ table: { stream_arn, shards: { shard_id: { seq,
//!   done } } } }`, see [`lineage`] for the format and for why a fresh
//!   sync is gapless.
//! - **Changes**: `INSERT`/`MODIFY` → `Upsert(NewImage)`, `REMOVE` (TTL
//!   expiries included) → `Delete(Keys)`. Keys are the partition key then
//!   the sort key. Order is kept per item, not across items; see
//!   [`stream`].
//! - **Errors**: credentials, permissions, a disabled stream or a wrong
//!   view type → `Config`; throttling, 5xx, network → `Transient`;
//!   trimmed records, an aged-out shard or a re-created stream →
//!   `PositionLost`.

pub mod client;
pub mod config;
pub mod convert;
pub mod errors;
pub mod lineage;
pub mod snapshot;
pub mod source;
pub mod stream;
mod util;

pub use config::{CredentialsConfig, DynamoConfig, ScanOptions, StreamOptions};
pub use lineage::{ShardCheckpoint, StreamCheckpoint, TableCheckpoint};
pub use source::{DynamoSource, DynamoSourceFactory};
