use std::fmt::{self, Display};
use std::ops::{Deref, DerefMut};

use revision::Revisioned;
use serde::{Deserialize, Serialize};

use crate::Datetime;

/// Represents a UUID value in SurrealDB
///
/// A UUID (Universally Unique Identifier) is a 128-bit identifier that is unique across space and
/// time. This type wraps the `uuid::Uuid` type.
#[derive(
	Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize,
)]
pub struct Uuid(pub uuid::Uuid);

impl Revisioned for Uuid {
	fn revision() -> u16 {
		1
	}

	fn serialize_revisioned<W: std::io::Write>(
		&self,
		writer: &mut W,
	) -> Result<(), revision::Error> {
		self.0.to_string().serialize_revisioned(writer)
	}

	fn deserialize_revisioned<R: std::io::Read>(reader: &mut R) -> Result<Self, revision::Error> {
		let s: String = Revisioned::deserialize_revisioned(reader)?;
		uuid::Uuid::parse_str(&s)
			.map_err(|err| revision::Error::Conversion(format!("invalid UUID format: {err:?}")))
			.map(Uuid)
	}
}

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
	/// Convert the Uuid to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_string()
	}
	/// Convert a slice to a UUID
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

impl TryFrom<String> for Uuid {
	type Error = uuid::Error;

	fn try_from(v: String) -> Result<Self, Self::Error> {
		Ok(Self(uuid::Uuid::parse_str(&v)?))
	}
}

impl Deref for Uuid {
	type Target = uuid::Uuid;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Uuid {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl Display for Uuid {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		self.0.fmt(f)
	}
}
