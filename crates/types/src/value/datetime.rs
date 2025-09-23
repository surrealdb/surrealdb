use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use chrono::offset::LocalResult;
use chrono::{DateTime, TimeZone, Utc};
use revision::Revisioned;
use serde::{Deserialize, Serialize};

/// Represents a datetime value in SurrealDB
///
/// A datetime represents a specific point in time, stored as UTC.
/// This type wraps the `chrono::DateTime<Utc>` type.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Datetime(pub(crate) DateTime<Utc>);

impl Revisioned for Datetime {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		self.0.to_rfc3339().serialize_revisioned(writer)
	}

	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let s: String = Revisioned::deserialize_revisioned(reader)?;
		DateTime::parse_from_rfc3339(&s)
			.map_err(|err| revision::Error::Conversion(format!("invalid datetime format: {err:?}")))
			.map(|dt| Datetime(dt.to_utc()))
	}
}

impl Default for Datetime {
	fn default() -> Self {
		Self(Utc::now())
	}
}

impl Datetime {
	/// The minimum UTC datetime
	pub const MIN_UTC: Self = Datetime(DateTime::<Utc>::MIN_UTC);
	/// The maximum UTC datetime
	pub const MAX_UTC: Self = Datetime(DateTime::<Utc>::MAX_UTC);

	/// Returns the current UTC datetime
	pub fn now() -> Self {
		Self(Utc::now())
	}

	/// Create a new datetime from chrono DateTime<Utc>
	pub fn new(dt: DateTime<Utc>) -> Self {
		Self(dt)
	}

	/// Get the inner DateTime<Utc>
	pub fn inner(&self) -> DateTime<Utc> {
		self.0
	}

	/// Create a new datetime from a timestamp.
	pub fn from_timestamp(seconds: i64, nanos: u32) -> Option<Self> {
		match Utc.timestamp_opt(seconds, nanos) {
			LocalResult::Single(v) => Some(Self(v)),
			LocalResult::Ambiguous(_, _) => None,
			LocalResult::None => None,
		}
	}
}

impl From<DateTime<Utc>> for Datetime {
	fn from(v: DateTime<Utc>) -> Self {
		Self(v)
	}
}

impl From<Datetime> for DateTime<Utc> {
	fn from(x: Datetime) -> Self {
		x.0
	}
}

impl Display for Datetime {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "{}Z", self.0.format("%Y-%m-%dT%H:%M:%S%.9f"))
	}
}

impl TryFrom<(i64, u32)> for Datetime {
	type Error = anyhow::Error;

	fn try_from(v: (i64, u32)) -> Result<Self, Self::Error> {
		match Utc.timestamp_opt(v.0, v.1) {
			LocalResult::Single(v) => Ok(Self(v)),
			err => match err {
				LocalResult::Single(v) => Ok(Self(v)),
				LocalResult::Ambiguous(_, _) => {
					Err(anyhow::anyhow!("Ambiguous timestamp: {}, {}", v.0, v.1))
				}
				LocalResult::None => Err(anyhow::anyhow!("Invalid timestamp: {}, {}", v.0, v.1)),
			},
		}
	}
}

impl Deref for Datetime {
	type Target = DateTime<Utc>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}
