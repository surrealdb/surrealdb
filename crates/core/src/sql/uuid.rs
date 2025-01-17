use crate::sql::{escape::quote_str, strand::Strand};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;
use std::str::FromStr;

use super::Datetime;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Uuid";

#[revisioned(revision = 1)]
#[derive(
	Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash,
)]
#[serde(rename = "$surrealdb::private::sql::Uuid")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Uuid(pub uuid::Uuid);

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

impl FromStr for Uuid {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<String> for Uuid {
	type Error = ();
	fn try_from(v: String) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<Strand> for Uuid {
	type Error = ();
	fn try_from(v: Strand) -> Result<Self, Self::Error> {
		Self::try_from(v.as_str())
	}
}

impl TryFrom<&str> for Uuid {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match uuid::Uuid::try_parse(v) {
			Ok(v) => Ok(Self(v)),
			Err(_) => Err(()),
		}
	}
}

impl Deref for Uuid {
	type Target = uuid::Uuid;
	fn deref(&self) -> &Self::Target {
		&self.0
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
}

impl Display for Uuid {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "u")?;
		Display::fmt(&quote_str(&self.0.to_string()), f)
	}
}

/// This module implements a serializer/deserializer that ensure reverse order.
/// Its works for UUIDv7 to get the more recent UUID first.
pub(crate) mod reverse {
	use serde::{Deserializer, Serializer};
	use uuid::{Bytes, Uuid};

	/// Custom serializer that reverses the bytes before serialization
	pub(crate) fn serialize<S>(u: &Uuid, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		// To invert the sort order, we apply bitwise NOT to the u128 value.
		// After that transformation,larger original values become smaller,
		// which flips the ascending sort order.
		let b = (!u.as_u128()).to_be_bytes();
		serde::Serialize::serialize(&b, serializer)
	}

	/// Custom deserializer that reverses the bytes back after deserialization
	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
	where
		D: Deserializer<'de>,
	{
		let b: Bytes = serde::Deserialize::deserialize(deserializer)?;

		Ok(Uuid::from_u128(!u128::from_be_bytes(b)))
	}

	#[cfg(test)]
	mod tests {
		use bincode::{DefaultOptions, Deserializer, Serializer};
		use uuid::{ContextV7, Timestamp, Uuid};

		#[test]
		fn ascendant_uuid() {
			let ser = |u: &Uuid| {
				let mut vec = Vec::new();
				let mut ser = Serializer::new(&mut vec, DefaultOptions::new());
				super::serialize(u, &mut ser).unwrap();
				vec
			};

			let de = |v: &Vec<u8>| {
				let mut de = Deserializer::from_slice(v, DefaultOptions::new());
				super::deserialize(&mut de).unwrap()
			};

			// Create the UUIDS
			let context = ContextV7::new();
			let u1 = Uuid::new_v7(Timestamp::now(&context));
			let u2 = Uuid::new_v7(Timestamp::now(&context));
			let u3 = Uuid::new_v7(Timestamp::now(&context));

			// Check that the initial ascendant order is valid
			assert!(u1 < u2, "u1: {u1}\nu2: {u2}");
			assert!(u2 < u3, "u2: {u2}\nu3: {u3}");
			assert!(u1 < u3, "u1: {u1}\nu3: {u3}");

			// Serialize the UUIDs
			let (v1, v2, v3) = (ser(&u1), ser(&u2), ser(&u3));

			// Check that the order of the vectors is now descendant
			assert!(v1 > v2, "v1: {v1:x?}\nv2: {v2:x?}");
			assert!(v2 > v3, "v2: {v2:x?}\nv3: {v3:x?}");
			assert!(v1 > v3, "v1: {v1:x?}\nv3: {v3:x?}");

			// Deserialize back the UUIDS
			let (uc1, uc2, uc3) = (de(&v1), de(&v2), de(&v3));

			// Check that the UUID are correct
			assert_eq!(u1, uc1);
			assert_eq!(u2, uc2);
			assert_eq!(u3, uc3);
		}
	}
}
