use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use std::time::Duration;
use uuid::Uuid;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Store)]
#[non_exhaustive]
pub struct Node {
	pub id: Uuid,
	pub hb: Timestamp,
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
		self.gc == false
	}
	/// Check if this node is archived
	pub fn is_archived(&self) -> bool {
		self.gc == true
	}
	// Return the node id if archived
	pub fn archived(&self) -> Option<Uuid> {
		match self.is_archived() {
			true => Some(self.id),
			false => None,
		}
	}
}

// This struct is meant to represent a timestamp that can be used to partially order
// events in a cluster. It should be derived from a timestamp oracle, such as the
// one available in TiKV via the client `TimestampExt` implementation.
#[revisioned(revision = 1)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, PartialOrd, Deserialize, Serialize, Hash, Store)]
#[non_exhaustive]
pub struct Timestamp {
	pub value: u64,
}

impl Default for Timestamp {
	fn default() -> Self {
		Self {
			value: 0,
		}
	}
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
