use std::fmt::{self, Display};
use std::ops::{Add, Sub};
use std::time::Duration;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use surrealdb_kvs::value::KVValue;
use surrealdb_kvs::{EncodeContext, FeatureLevel, Release};
use surrealdb_types::{SqlFormat, ToSql, write_sql};
use uuid::Uuid;

use crate::expr::statements::info::InfoStructure;
use crate::stored::{Stored, canonical, decode_entry, encode_stored};
use crate::val::{Object, Value};
use crate::{Error, impl_kv_value_catalog};

/// A node in the cluster
#[revisioned(revision = 2)]
#[derive(
	Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Stored,
)]
#[stored(since = "4.0", floor_fixed)]
pub struct Node {
	/// The id of the node
	#[stored(n = 2)]
	pub id: Uuid,
	/// The heartbeat of the node
	#[stored(n = 4)]
	pub heartbeat: Timestamp,
	/// Whether the node is garbage collected
	#[stored(n = 6)]
	pub gc: bool,
	/// Public HTTP endpoint at which other cluster members can reach this node
	/// for cross-node messaging (live-query relay, etc.). `None` when not
	/// configured — single-node and shared-backend deployments leave this empty.
	#[revision(start = 2)]
	#[stored(n = 8)]
	pub http_endpoint: Option<String>,
}

impl_kv_value_catalog!(Node);

impl Node {
	/// Create a new Node entry
	pub fn new(id: Uuid, hb: Timestamp, gc: bool) -> Self {
		Self {
			id,
			heartbeat: hb,
			gc,
			http_endpoint: None,
		}
	}

	/// Create a new Node entry with a public HTTP endpoint for cross-node
	/// messaging.
	pub fn new_with_endpoint(
		id: Uuid,
		hb: Timestamp,
		gc: bool,
		http_endpoint: Option<String>,
	) -> Self {
		Self {
			id,
			heartbeat: hb,
			gc,
			http_endpoint,
		}
	}
	/// Mark this node as archived
	pub fn archive(&self) -> Self {
		Node {
			gc: true,
			..self.to_owned()
		}
	}
	/// Check if this node is active
	pub fn id(&self) -> Uuid {
		self.id
	}
	/// Check if this node is active
	pub fn is_active(&self) -> bool {
		!self.gc
	}
	/// Check if this node is archived
	pub fn is_archived(&self) -> bool {
		self.gc
	}
	/// Returns the node id if this node is archived, `None` otherwise.
	pub fn archived(&self) -> Option<Uuid> {
		self.is_archived().then_some(self.id)
	}
}

impl ToSql for Node {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "NODE {} SEEN {}", self.id, self.heartbeat);
		if self.gc {
			write_sql!(f, sql_fmt, " ARCHIVED");
		} else {
			write_sql!(f, sql_fmt, " ACTIVE");
		}
	}
}

impl InfoStructure for Node {
	fn structure(self) -> Value {
		let object = map! {
			"id" => Value::Uuid(self.id.into()),
			"seen" => self.heartbeat.structure(),
			"active" => Value::Bool(!self.gc),
		};
		Value::Object(Object::from(object))
	}
}

/// The release a node runs, recorded beside its heartbeat.
///
/// The record is the stored grammar at every feature level and is never merged with the record it
/// replaces: each write describes the node that wrote it, so an earlier release drops a later
/// release's ignorable fields when it rewrites its own record. `floor_fixed` keeps every release
/// able to read, rewrite and remove it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Stored)]
#[stored(since = "4.0", floor_fixed)]
pub struct NodeRelease {
	/// The release of the node's binary.
	#[stored(n = 2)]
	pub release: Release,
	/// The heartbeat of the node's own record this one was written beside.
	#[stored(n = 4)]
	pub heartbeat: Timestamp,
}

/// Written in the stored grammar whatever the store's feature level, so the record is not
/// levelled.
impl KVValue for NodeRelease {
	type KeyContext = ();

	fn kv_encode_value(&self) -> anyhow::Result<Vec<u8>> {
		let ctx = EncodeContext {
			level: FeatureLevel::At(Release::CURRENT),
			prior: None,
		};
		Ok(encode_stored(self, &ctx).map_err(Error::Entry)?.bytes)
	}

	fn kv_canonical(&self) -> anyhow::Result<Vec<u8>> {
		Ok(canonical(self).map_err(Error::Entry)?)
	}

	fn kv_decode_value(bytes: &[u8], _: ()) -> anyhow::Result<Self> {
		Ok(decode_entry::<Self>(bytes).map_err(Error::Entry)?)
	}
}

// This struct is meant to represent a timestamp that can be used to partially
// order events in a cluster. It should be derived from a timestamp oracle, such
// as the one available in TiKV via the client `TimestampExt` implementation.
/// A versionstamp-style timestamp used for cluster-node bookkeeping.
#[revisioned(revision = 1)]
#[derive(
	Clone, Copy, Default, Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Hash, Stored,
)]
#[stored(since = "4.0", floor_fixed)]
pub struct Timestamp {
	/// The raw timestamp value.
	#[stored(n = 2)]
	pub value: u64,
}

impl From<u64> for Timestamp {
	fn from(value: u64) -> Self {
		Timestamp {
			value,
		}
	}
}

impl Add<Duration> for Timestamp {
	type Output = Timestamp;
	fn add(self, rhs: Duration) -> Self::Output {
		Timestamp {
			value: self.value.wrapping_add(rhs.as_millis() as u64),
		}
	}
}

impl Sub<Duration> for Timestamp {
	type Output = Timestamp;
	fn sub(self, rhs: Duration) -> Self::Output {
		Timestamp {
			value: self.value.wrapping_sub(rhs.as_millis() as u64),
		}
	}
}

impl Display for Timestamp {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", self.value)
	}
}

impl ToSql for Timestamp {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "{}", self.value)
	}
}

impl InfoStructure for Timestamp {
	fn structure(self) -> Value {
		self.value.into()
	}
}

#[cfg(test)]
mod test {
	use std::time::Duration;

	use chrono::TimeZone;
	use chrono::prelude::Utc;

	use super::Timestamp;

	#[test]
	fn timestamps_can_be_added_duration() {
		let t = Utc.with_ymd_and_hms(2000, 1, 1, 12, 30, 0).unwrap();
		let ts = Timestamp {
			value: t.timestamp_millis() as u64,
		};

		let hour = Duration::from_secs(60 * 60);
		let ts = ts + hour;
		let ts = ts + hour;
		let ts = ts + hour;

		let end_time = Utc.timestamp_millis_opt(ts.value as i64).unwrap();
		let expected_end_time = Utc.with_ymd_and_hms(2000, 1, 1, 15, 30, 0).unwrap();
		assert_eq!(end_time, expected_end_time);
	}

	#[test]
	fn timestamps_can_be_subtracted_duration() {
		let t = Utc.with_ymd_and_hms(2000, 1, 1, 12, 30, 0).unwrap();
		let ts = Timestamp {
			value: t.timestamp_millis() as u64,
		};

		let hour = Duration::from_secs(60 * 60);
		let ts = ts - hour;
		let ts = ts - hour;
		let ts = ts - hour;

		let end_time = Utc.timestamp_millis_opt(ts.value as i64).unwrap();
		let expected_end_time = Utc.with_ymd_and_hms(2000, 1, 1, 9, 30, 0).unwrap();
		assert_eq!(end_time, expected_end_time);
	}
}
