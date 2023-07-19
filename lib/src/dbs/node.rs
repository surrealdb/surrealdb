use crate::err::Error;
use crate::err::Error::TimestampOverflow;
use derive::{Key, Store};
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use std::time::Duration;

// NOTE: This is not a statement, but as per layering, keeping it here till we
// have a better structure.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Store)]
pub struct ClusterMembership {
	pub name: String,
	// TiKV = TiKV TSO Timestamp as u64
	// not TiKV = local nanos as u64
	pub heartbeat: Timestamp,
}
// This struct is meant to represent a timestamp that can be used to partially order
// events in a cluster. It should be derived from a timestamp oracle, such as the
// one available in TiKV via the client `TimestampExt` implementation.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Store)]
pub struct Timestamp {
	pub value: u64,
}
// This struct is to be used only when storing keys as the macro currently
// conflicts when you have Store and Key derive macros.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Key)]
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

impl Add<Duration> for Timestamp {
	type Output = Timestamp;
	fn add(self, rhs: Duration) -> Timestamp {
		Timestamp {
			value: self.value + rhs.as_secs(),
		}
	}
}

impl Sub<Duration> for Timestamp {
	type Output = Result<Timestamp, Error>;
	fn sub(self, rhs: Duration) -> Self::Output {
		let millis = rhs.as_secs();
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

// TODO test
