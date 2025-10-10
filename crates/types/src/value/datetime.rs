use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

use chrono::offset::LocalResult;
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::utils::escape::QuoteStr;
use crate::write_sql;

/// Represents a datetime value in SurrealDB
///
/// A datetime represents a specific point in time, stored as UTC.
/// This type wraps the `chrono::DateTime<Utc>` type.

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Datetime(pub(crate) DateTime<Utc>);

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

impl FromStr for Datetime {
	type Err = anyhow::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Ok(Self(DateTime::parse_from_rfc3339(s)?.to_utc()))
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
		self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true).fmt(f)
	}
}

impl ToSql for Datetime {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "d{}", QuoteStr(&self.to_string()))
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
