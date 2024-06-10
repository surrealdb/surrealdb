use crate::err::Error;
use crate::err::Error::TimestampOverflow;
use crate::sql::Duration;
use derive::{Key, Store};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use uuid::Uuid;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash, Store)]
#[non_exhaustive]
pub struct Node {
	pub id: Uuid,
	pub heartbeat: Timestamp,
}

// NOTE: This is not a statement, but as per layering, keeping it here till we
// have a better structure.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Store)]
#[non_exhaustive]
pub struct ClusterMembership {
	pub name: String,
	// TiKV = TiKV TSO Timestamp as u64
	// not TiKV = local nanos as u64
	pub heartbeat: Timestamp,
}
// This struct is meant to represent a timestamp that can be used to partially order
// events in a cluster. It should be derived from a timestamp oracle, such as the
// one available in TiKV via the client `TimestampExt` implementation.
#[revisioned(revision = 1)]
#[derive(
	Clone, Copy, Debug, Eq, PartialEq, Deserialize, Serialize, Ord, PartialOrd, Hash, Store, Default,
)]
#[non_exhaustive]
pub struct Timestamp {
	pub value: u64,
}

impl From<u64> for Timestamp {
	fn from(ts: u64) -> Self {
		Timestamp {
			value: ts,
		}
	}
}

// This struct is to be used only when storing keys as the macro currently
// conflicts when you have Store and Key derive macros.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Key)]
#[non_exhaustive]
pub struct KeyTimestamp {
	pub value: u64,
}

impl From<&Timestamp> for KeyTimestamp {
	fn from(ts: &Timestamp) -> Self {
		KeyTimestamp {
			value: ts.value,
		}
	}
}

impl Add<&Duration> for &Timestamp {
	type Output = Timestamp;
	fn add(self, rhs: &Duration) -> Timestamp {
		Timestamp {
			value: self.value + rhs.as_millis() as u64,
		}
	}
}

impl Sub<&Duration> for &Timestamp {
	type Output = Result<Timestamp, Error>;
	fn sub(self, rhs: &Duration) -> Self::Output {
		let millis = rhs.as_millis() as u64;
		if self.value <= millis {
			// Removing the duration from this timestamp will cause it to overflow
			return Err(TimestampOverflow(format!(
				"Failed to subtract {} from {}",
				&millis, &self.value
			)));
		}
		Ok(Timestamp {
			value: self.value - millis,
		})
	}
}

#[cfg(test)]
mod test {
	use crate::dbs::node::Timestamp;
	use crate::sql::Duration;
	use chrono::prelude::Utc;
	use chrono::TimeZone;

	#[test]
	fn timestamps_can_be_added_duration() {
		let t = Utc.with_ymd_and_hms(2000, 1, 1, 12, 30, 0).unwrap();
		let ts = Timestamp {
			value: t.timestamp_millis() as u64,
		};

		let hour = Duration(core::time::Duration::from_secs(60 * 60));
		let ts = &ts + &hour;
		let ts = &ts + &hour;
		let ts = &ts + &hour;

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

		let hour = Duration(core::time::Duration::from_secs(60 * 60));
		let ts = (&ts - &hour).unwrap();
		let ts = (&ts - &hour).unwrap();
		let ts = (&ts - &hour).unwrap();

		let end_time = Utc.timestamp_millis_opt(ts.value as i64).unwrap();
		let expected_end_time = Utc.with_ymd_and_hms(2000, 1, 1, 9, 30, 0).unwrap();
		assert_eq!(end_time, expected_end_time);
	}
}
