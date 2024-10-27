use crate::sql::datetime::Datetime;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use chrono::{DateTime as ChronoDateTime, Utc};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Version(pub Datetime);

impl Version {
	/// Convert to nanosecond timestamp.
	pub fn to_u64(&self) -> u64 {
		self.0.timestamp_nanos_opt().unwrap_or_default() as u64
	}
}

impl From<String> for Version {
	fn from(value: String) -> Self {
		let parsed = ChronoDateTime::parse_from_rfc3339(value.as_str())
			.expect("Could not parse from GraphQL string value");

		Self(Datetime::from(parsed.with_timezone(&Utc)))
	}
}

impl fmt::Display for Version {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "VERSION {}", self.0)
	}
}
