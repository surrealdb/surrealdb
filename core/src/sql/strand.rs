use crate::err::Error;
use crate::sql::escape::quote_plain_str;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::ops::{self};
use std::str;

use super::value::TryAdd;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Strand";

/// A string that doesn't contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Strand")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Strand(#[serde(with = "no_nul_bytes")] pub String);

impl From<String> for Strand {
	fn from(s: String) -> Self {
		debug_assert!(!s.contains('\0'));
		Strand(s)
	}
}

impl From<&str> for Strand {
	fn from(s: &str) -> Self {
		debug_assert!(!s.contains('\0'));
		Self::from(String::from(s))
	}
}

impl Deref for Strand {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Strand> for String {
	fn from(s: Strand) -> Self {
		s.0
	}
}

impl Strand {
	/// Get the underlying String slice
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
	/// Returns the underlying String
	pub fn as_string(self) -> String {
		self.0
	}
	/// Convert the Strand to a raw String
	pub fn to_raw(self) -> String {
		self.0
	}
}

impl Display for Strand {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		Display::fmt(&quote_plain_str(&self.0), f)
	}
}

impl ops::Add for Strand {
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		self.0.push_str(other.as_str());
		self
	}
}

impl TryAdd for Strand {
	type Output = Self;
	fn try_add(mut self, other: Self) -> Result<Self, Error> {
		if self.0.try_reserve(other.len()).is_ok() {
			self.0.push_str(other.as_str());
			Ok(self)
		} else {
			Err(Error::ArithmeticOverflow(format!("{self} + {other}")))
		}
	}
}

// serde(with = no_nul_bytes) will (de)serialize with no NUL bytes.
pub(crate) mod no_nul_bytes {
	use serde::{
		de::{self, Visitor},
		Deserializer, Serializer,
	};
	use std::fmt;

	pub(crate) fn serialize<S>(s: &str, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		debug_assert!(!s.contains('\0'));
		serializer.serialize_str(s)
	}

	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesVisitor;

		impl<'de> Visitor<'de> for NoNulBytesVisitor {
			type Value = String;

			fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
				formatter.write_str("a string without any NUL bytes")
			}

			fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				if value.contains('\0') {
					Err(de::Error::custom("contained NUL byte"))
				} else {
					Ok(value.to_owned())
				}
			}

			fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
			where
				E: de::Error,
			{
				if value.contains('\0') {
					Err(de::Error::custom("contained NUL byte"))
				} else {
					Ok(value)
				}
			}
		}

		deserializer.deserialize_string(NoNulBytesVisitor)
	}
}
