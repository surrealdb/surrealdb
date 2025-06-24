use crate::sql::duration::Duration;
use crate::sql::strand::Strand;
use crate::syn;
use anyhow::Result;
use chrono::offset::LocalResult;
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::{ops, str};

use super::escape::QuoteStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Datetime";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Datetime")]
#[non_exhaustive]
pub struct Datetime(pub DateTime<Utc>);

impl Datetime {
	pub const MIN_UTC: Self = Datetime(DateTime::<Utc>::MIN_UTC);
	pub const MAX_UTC: Self = Datetime(DateTime::<Utc>::MAX_UTC);
}

impl Default for Datetime {
	fn default() -> Self {
		Self(Utc::now())
	}
}

impl From<Datetime> for crate::val::Datetime {
	fn from(v: Datetime) -> Self {
		crate::val::Datetime(v.0)
	}
}

impl From<crate::val::Datetime> for Datetime {
	fn from(v: crate::val::Datetime) -> Self {
		Self(v.0)
	}
}

impl Deref for Datetime {
	type Target = DateTime<Utc>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Datetime {
	/// Convert the Datetime to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true)
	}

	/// Convert to nanosecond timestamp.
	pub fn to_u64(&self) -> Option<u64> {
		self.0.timestamp_nanos_opt().map(|v| v as u64)
	}

	/// Convert to nanosecond timestamp.
	pub fn to_i64(&self) -> Option<i64> {
		self.0.timestamp_nanos_opt()
	}

	/// Convert to second timestamp.
	pub fn to_secs(&self) -> i64 {
		self.0.timestamp()
	}
}

impl Display for Datetime {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "d{}", &QuoteStr(&self.to_raw()))
	}
}

impl ops::Sub<Self> for Datetime {
	type Output = Duration;
	fn sub(self, other: Self) -> Duration {
		match (self.0 - other.0).to_std() {
			Ok(d) => Duration::from(d),
			Err(_) => Duration::default(),
		}
	}
}
