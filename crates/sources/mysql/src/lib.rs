//! MySQL and MariaDB source: row-based binlog with GTID.
//!
//! Lifecycle (see `meili_connect_core::Source`):
//!
//! - `open(None)` captures `@@global.gtid_executed` (or the binlog file and
//!   offset when `gtid_mode` is off) *before* the snapshot reads anything.
//! - `snapshot` reads every mapped table inside one
//!   `START TRANSACTION WITH CONSISTENT SNAPSHOT`, by keyset pages on the
//!   primary key.
//! - `stream` registers as a replica (`COM_BINLOG_DUMP_GTID`, or
//!   `COM_BINLOG_DUMP` in file mode) and decodes row events of mapped tables,
//!   emitting a `Checkpoint` at every transaction end and while idle.
//!
//! MySQL keeps nothing per consumer, so there is nothing to create, ack or
//! tear down on the origin. Binlog retention is the only thing a stalled
//! sync can outlive: a purged position is `PositionLost`.
//!
//! MariaDB (best effort): its GTIDs (`domain-server-seq`) are not MySQL
//! GTIDs, so a MariaDB source always runs in file+offset mode; `JSON` is a
//! `LONGTEXT` alias there and arrives as a string. Failover between MariaDB
//! servers therefore loses the position (resync).

#![cfg_attr(test, allow(clippy::unwrap_used, clippy::expect_used))]

mod config;
mod convert;
mod ddl;
mod endpoints;
mod error;
mod gtid;
mod position;
mod rows;
mod schema;
mod source;
mod stream;

pub use config::{MysqlConfig, TlsConfig, TlsMode, derive_server_id};
pub use gtid::GtidSet;
pub use position::Position;
pub use source::{MysqlSource, MysqlSourceFactory};
