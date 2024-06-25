use crate::sql::statements::info::InfoStructure;
use crate::sql::Value;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};
use std::ops::{Add, Sub};
use std::time::Duration;
use uuid::Uuid;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Store)]
#[non_exhaustive]
pub struct Node {
	pub id: Uuid,
	pub hb: Timestamp,
	#[revision(start = 2, default_fn = "default_gc")]
	pub gc: bool,
}

impl Node {
	/// Create a new Node entry
	pub fn new(id: Uuid, hb: Timestamp, gc: bool) -> Self {
		Self {
			id,
			hb,
			gc,
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
	// Return the node id if archived
	pub fn archived(&self) -> Option<Uuid> {
		match self.is_archived() {
			true => Some(self.id),
			false => None,
		}
	}
	// Sets the default gc value for old nodes
	fn default_gc(_revision: u16) -> bool {
		true
	}
}

impl Display for Node {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "NODE {} SEEN {}", self.id, self.hb)?;
		match self.gc {
			true => write!(f, " ARCHIVED")?,
			false => write!(f, " ACTIVE")?,
		};
		Ok(())
	}
}

impl InfoStructure for Node {
	fn structure(self) -> Value {
		Value::from(map! {
			"id".to_string() => Value::from(self.id),
			"seen".to_string() => self.hb.structure(),
			"active".to_string() => Value::from(!self.gc),
		})
	}
}

// This struct is meant to represent a timestamp that can be used to partially order
// events in a cluster. It should be derived from a timestamp oracle, such as the
// one available in TiKV via the client `TimestampExt` implementation.
#[revisioned(revision = 1)]
#[derive(
	Clone, Copy, Default, Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Hash, Store,
)]
#[non_exhaustive]
pub struct Timestamp {
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

impl InfoStructure for Timestamp {
	fn structure(self) -> Value {
		self.value.into()
	}
}

#[cfg(test)]
mod test {
	use crate::dbs::node::Timestamp;
	use chrono::prelude::Utc;
	use chrono::TimeZone;
	use std::time::Duration;

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
