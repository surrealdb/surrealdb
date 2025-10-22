use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;
use std::str::FromStr;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};

use super::Datetime;
use crate::fmt::QuoteStr;
use crate::val::IndexFormat;

#[revisioned(revision = 1)]
#[derive(
	Clone,
	Copy,
	Debug,
	Default,
	Eq,
	Ord,
	PartialEq,
	PartialOrd,
	Serialize,
	Deserialize,
	Hash,
	Encode,
	BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::Uuid")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub struct Uuid(pub uuid::Uuid);

impl Uuid {
	/// Generate a new UUID
	pub fn new() -> Self {
		Self(uuid::Uuid::now_v7())
	}

	/// Generate a new V4 UUID
	pub fn new_v4() -> Self {
		Self(uuid::Uuid::new_v4())
	}

	/// Generate a new V7 UUID
	pub fn new_v7() -> Self {
		Self(uuid::Uuid::now_v7())
	}

	/// Generate a new V7 UUID
	pub fn new_v7_from_datetime(timestamp: Datetime) -> Self {
		let ts = uuid::Timestamp::from_unix(
			uuid::NoContext,
			timestamp.0.timestamp() as u64,
			timestamp.0.timestamp_subsec_nanos(),
		);
		Self(uuid::Uuid::new_v7(ts))
	}

	/// Generate a new nil UUID
	pub const fn nil() -> Self {
		Self(uuid::Uuid::nil())
	}

	/// Generate a new max UUID
	pub const fn max() -> Self {
		Self(uuid::Uuid::max())
	}

	/// Convert the Uuid to a raw String
	pub fn to_raw(self) -> String {
		self.0.to_string()
	}

	/// Generate a new UUID from a slice
	pub fn from_slice(slice: &[u8]) -> Result<Self, uuid::Error> {
		Ok(Self(uuid::Uuid::from_slice(slice)?))
	}
}

impl From<uuid::Uuid> for Uuid {
	fn from(v: uuid::Uuid) -> Self {
		Uuid(v)
	}
}

impl From<Uuid> for uuid::Uuid {
	fn from(s: Uuid) -> Self {
		s.0
	}
}

impl From<surrealdb_types::Uuid> for Uuid {
	fn from(v: surrealdb_types::Uuid) -> Self {
		Uuid(v.0)
	}
}

impl From<Uuid> for surrealdb_types::Uuid {
	fn from(x: Uuid) -> Self {
		surrealdb_types::Uuid::from(x.0)
	}
}

impl FromStr for Uuid {
	type Err = uuid::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		uuid::Uuid::try_parse(s).map(Uuid)
	}
}

impl Deref for Uuid {
	type Target = uuid::Uuid;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Uuid {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "u{}", QuoteStr(&self.0.to_string()))
	}
}
