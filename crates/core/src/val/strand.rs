use crate::err::Error;
use crate::expr::escape::QuoteStr;
use crate::val::TryAdd;
use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::{
	Deref, {self},
};
use std::str;

/// A string that doesn't contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Strand")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Strand(#[serde(with = "no_nul_bytes")] String);

impl Strand {
	/// Create a new strand, returns None if the string contains a null byte.
	pub fn new(s: String) -> Option<Strand> {
		if s.contains('\0') {
			None
		} else {
			Some(Strand(s))
		}
	}

	/// Create a new strand, without checking the string.
	///
	/// # Safety
	/// Caller must ensure that string handed as an argument does not contain any null bytes.
	pub unsafe fn new_unchecked(s: String) -> Strand {
		// Check in debug mode if the variants
		debug_assert!(!s.contains('\0'));
		Strand(s)
	}

	pub fn into_string(self) -> String {
		self.0
	}

	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
}

impl From<String> for Strand {
	fn from(s: String) -> Self {
		// TODO: For now, fix this in the future.
		unsafe { Self::new_unchecked(s) }
	}
}

impl From<&str> for Strand {
	fn from(s: &str) -> Self {
		// TODO: For now, fix this in the future.
		unsafe { Self::new_unchecked(s.to_string()) }
	}
}

impl Deref for Strand {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Strand> for String {
	fn from(s: Strand) -> Self {
		s.0
	}
}

impl Display for Strand {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		QuoteStr(&self.0).fmt(f)
	}
}

// TODO: Dubious add implementation, concatination is not really an addition in rust.
impl ops::Add for Strand {
	type Output = Self;
	fn add(mut self, other: Self) -> Self {
		self.0.push_str(other.as_str());
		self
	}
}

impl TryAdd for Strand {
	type Output = Self;
	fn try_add(mut self, other: Self) -> Result<Self> {
		if self.0.try_reserve(other.len()).is_ok() {
			self.0.push_str(other.as_str());
			Ok(self)
		} else {
			Err(anyhow::Error::new(Error::InsufficientReserve(format!(
				"additional string of length {} bytes",
				other.0.len()
			))))
		}
	}
}

// serde(with = no_nul_bytes) will (de)serialize with no NUL bytes.
pub(crate) mod no_nul_bytes {
	use serde::de::{self, Visitor};
	use serde::{Deserializer, Serializer};
	use std::fmt;

	pub(crate) fn serialize<S>(s: &str, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		if s.contains('\0') {
			return Err(<S::Error as serde::ser::Error>::custom(
				"to be serialized string contained a null byte",
			));
		}
		serializer.serialize_str(s)
	}

	pub(crate) fn deserialize<'de, D>(deserializer: D) -> Result<String, D::Error>
	where
		D: Deserializer<'de>,
	{
		struct NoNulBytesVisitor;

		impl Visitor<'_> for NoNulBytesVisitor {
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
