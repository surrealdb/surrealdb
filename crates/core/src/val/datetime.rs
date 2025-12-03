use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;
use std::{ops, str};

use anyhow::{Result, anyhow};
use chrono::offset::LocalResult;
use chrono::{DateTime, SecondsFormat, TimeZone, Utc};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::err::Error;
use crate::fmt::QuoteStr;
use crate::syn;
use crate::val::{Duration, TrySub};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Datetime")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Datetime(pub DateTime<Utc>);

impl Datetime {
	pub fn now() -> Datetime {
		Datetime(Utc::now())
	}
}

impl Datetime {
	pub const MIN_UTC: Self = Datetime(DateTime::<Utc>::MIN_UTC);
	pub const MAX_UTC: Self = Datetime(DateTime::<Utc>::MAX_UTC);
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

impl From<surrealdb_types::Datetime> for Datetime {
	fn from(v: surrealdb_types::Datetime) -> Self {
		Self(v.inner())
	}
}

impl From<Datetime> for surrealdb_types::Datetime {
	fn from(x: Datetime) -> Self {
		surrealdb_types::Datetime::from(x.0)
	}
}

impl FromStr for Datetime {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match syn::datetime(s) {
			Ok(v) => Ok(v.into()),
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
	/// Convert to nanosecond timestamp.
	pub fn to_u64(&self) -> Option<u64> {
		self.0.timestamp_nanos_opt().map(|v| v as u64)
	}

	pub fn to_version_stamp(&self) -> Result<u64> {
		self.to_u64().ok_or_else(|| anyhow!(Error::TimestampOverflow(self.to_string())))
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
		self.0.to_rfc3339_opts(SecondsFormat::AutoSi, true).fmt(f)
	}
}

impl ToSql for Datetime {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "d{}", QuoteStr(&self.to_string()))
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

impl TrySub for Datetime {
	type Output = Duration;
	fn try_sub(self, other: Self) -> Result<Duration> {
		(self.0 - other.0)
			.to_std()
			.map_err(|_| Error::ArithmeticNegativeOverflow(format!("{self} - {other}")))
			.map_err(anyhow::Error::new)
			.map(Duration::from)
	}
}

impl<F> Encode<F> for Datetime {
	fn encode<W: std::io::Write>(
		&self,
		w: &mut storekey::Writer<W>,
	) -> std::result::Result<(), storekey::EncodeError> {
		let encode = self.to_rfc3339_opts(SecondsFormat::AutoSi, true);
		Encode::<F>::encode(&encode, w)
	}
}

impl<'de, F> BorrowDecode<'de, F> for Datetime {
	fn borrow_decode(
		r: &mut storekey::BorrowReader<'de>,
	) -> std::result::Result<Self, storekey::DecodeError> {
		let s = r.read_str_cow()?;
		DateTime::parse_from_rfc3339(s.as_ref())
			.map_err(|_| storekey::DecodeError::InvalidFormat)
			.map(|x| Datetime(x.to_utc()))
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;
	use crate::types::PublicDatetime;

	#[rstest]
	#[case("2021-01-01T00:00:00Z", Datetime(DateTime::<Utc>::from_timestamp(1_609_459_200, 0).unwrap()), PublicDatetime::from_timestamp(1_609_459_200, 0).unwrap())]
	fn test_from_str(
		#[case] input: &str,
		#[case] expected: Datetime,
		#[case] expected_public: PublicDatetime,
	) {
		let internal_actual = Datetime::from_str(input).unwrap();
		let public_actual = PublicDatetime::from_str(input).unwrap();

		assert_eq!(internal_actual.timestamp(), expected.timestamp());

		assert_eq!(internal_actual, expected);
		assert_eq!(public_actual, expected_public);

		assert_eq!(internal_actual.to_string(), public_actual.to_string());
	}
}
