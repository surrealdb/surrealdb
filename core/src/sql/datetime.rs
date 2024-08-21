use crate::sql::duration::Duration;
use crate::sql::strand::Strand;
use crate::syn;
use chrono::{offset::LocalResult, DateTime, SecondsFormat, TimeZone, Utc};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops;
use std::ops::Deref;
use std::str;
use std::str::FromStr;

use super::escape::quote_str;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Datetime";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Datetime")]
#[non_exhaustive]
pub struct Datetime(pub DateTime<Utc>);

impl Default for Datetime {
	fn default() -> Self {
		Self(Utc::now())
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

impl FromStr for Datetime {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Datetime {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Datetime {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Datetime {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match syn::datetime_raw(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl TryFrom<(i64, u32)> for Datetime {
	type Error = ();
	fn try_from(v: (i64, u32)) -> Result<Self, Self::Error> {
		match Utc.timestamp_opt(v.0, v.1) {
			LocalResult::Single(v) => Ok(Self(v)),
			_ => Err(()),
		}
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
}

impl Display for Datetime {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "d{}", &quote_str(&self.to_raw()))
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
