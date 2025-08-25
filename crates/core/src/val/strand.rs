use std::borrow::Borrow;
use std::fmt::{self, Display, Formatter};
use std::ops::{
	Deref, {self},
};
use std::str;

use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::err::Error;
use crate::expr::escape::QuoteStr;
use crate::val::TryAdd;

#[derive(Eq, PartialEq, Ord, PartialOrd, Debug)]
#[repr(transparent)]
pub struct StrandRef(str);

impl StrandRef {
	/// # Safety
	///
	/// string must not have a null byte in it
	pub const unsafe fn new_unchecked(s: &str) -> &StrandRef {
		unsafe {
			// This is safe as StrandRef has the same representation as str.
			std::mem::transmute(s)
		}
	}
}

impl ToOwned for StrandRef {
	type Owned = Strand;

	fn to_owned(&self) -> Self::Owned {
		Strand(self.0.to_owned())
	}
}

/// Fast way of removing null bytes in place without having to realloc the
/// string.
fn remove_null_bytes(s: String) -> String {
	let mut bytes = s.into_bytes();
	let mut write = 0;
	for i in 0..bytes.len() {
		let b = bytes[i];
		if b == 0 {
			continue;
		}
		bytes[write] = b;
		write += 1;
	}
	// remove duplicated bytes at the end.
	bytes.truncate(write);
	unsafe {
		// Safety: bytes were derived from a string,
		// we only removed all bytes which were 0 so we still have a valid utf8 string.
		String::from_utf8_unchecked(bytes)
	}
}

/// A string that doesn't contain NUL bytes.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::Strand")]
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

	/// Create a new strand from a string.
	/// Removes all null bytes if there are any
	pub fn new_lossy(s: String) -> Strand {
		Strand(remove_null_bytes(s))
	}

	/// Create a new strand, without checking the string.
	///
	/// # Safety
	/// Caller must ensure that string handed as an argument does not contain
	/// any null bytes.
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

	pub fn to_raw_string(&self) -> String {
		self.0.clone()
	}
}

impl Borrow<StrandRef> for Strand {
	fn borrow(&self) -> &StrandRef {
		// Safety:  both strand and strandref uphold no null bytes.
		unsafe { StrandRef::new_unchecked(self.as_str()) }
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

// TODO: Change this to str, possibly.
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

// TODO: Dubious add implementation, concatination is not really an addition in
// rust.
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
	use std::fmt;

	use serde::de::{self, Visitor};
	use serde::{Deserializer, Serializer};

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
