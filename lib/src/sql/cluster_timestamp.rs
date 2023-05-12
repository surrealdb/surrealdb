use derive::{Key, Store};
use serde::{Deserialize, Serialize};
use std::ops::{Add, Sub};
use std::time::Duration;
use time::ext::NumericalStdDuration;

// This struct is meant to represent a timestamp that can be used to partially order
// events in a cluster. It should be derived from a timestamp oracle, such as the
// one available in TiKV via the client `TimestampExt` implementation.
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize, PartialOrd, Hash, Store, Key)]
pub struct Timestamp {
	pub value: u64,
}

impl Add<Duration> for Timestamp {
	type Output = Timestamp;
	fn add(self, rhs: Duration) -> Timestamp {
		Timestamp {
			value: self.value + rhs.as_secs() as u64,
		}
	}
}

impl Sub for Timestamp {
	type Output = Duration;
	fn sub(self, rhs: Timestamp) -> Duration {
		Duration::from_millis(self.value - rhs.value)
	}
}

impl Sub<Duration> for Timestamp {
	type Output = Timestamp;
	fn sub(self, rhs: Duration) -> Timestamp {
		Timestamp {
			value: self.value - (rhs.as_millis() as u64),
		}
	}
}

// TODO test
