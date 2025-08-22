use std::fmt::{self, Display, Formatter};
use std::ops::Deref;
use std::str;

use anyhow::Result;
use revision::revisioned;

use crate::expr::Value;
use crate::expr::escape::EscapeIdent;
use crate::expr::statements::info::InfoStructure;
use crate::val::{Strand, Table};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Ident(String);

impl Ident {
	/// Create a new identifier
	///
	/// This function checks if the string has a null byte, returns None if it
	/// has.
	pub fn new(str: String) -> Option<Self> {
		if str.contains('\0') {
			return None;
		}
		Some(Ident(str))
	}

	/// Create a new identifier
	///
	/// This function checks if the string has a null byte, returns an error if it
	/// has.
	pub fn try_new(str: String) -> Result<Self> {
		if str.contains('\0') {
			return Err(anyhow::anyhow!("String contains null byte"));
		}
		Ok(Ident(str))
	}

	/// Create a new identifier
	///
	/// # Safety
	/// Caller should ensure that the string does not contain a null byte.
	pub unsafe fn new_unchecked(s: String) -> Self {
		debug_assert!(!s.as_bytes().contains(&0));
		Ident(s)
	}

	pub fn from_strand(str: Strand) -> Self {
		Ident(str.into_string())
	}

	/// Convert ident into a strand.
	pub fn into_strand(self) -> Strand {
		// Safety: both ident and Strand uphold the no-null byte invariant.
		unsafe { Strand::new_unchecked(self.0) }
	}

	// Convert into a string.
	pub fn into_string(self) -> String {
		self.0
	}

	/// Returns the slice of the underlying string.
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}

	/// Convert the Ident to a raw String
	pub fn to_raw_string(&self) -> String {
		self.0.clone()
	}

	/// Checks if this field is the `type` field
	pub(crate) fn is_type(&self) -> bool {
		self.0.as_str() == "type"
	}
	/// Checks if this field is the `coordinates` field
	pub(crate) fn is_coordinates(&self) -> bool {
		self.0.as_str() == "coordinates"
	}
	/// Checks if this field is the `geometries` field
	pub(crate) fn is_geometries(&self) -> bool {
		self.0.as_str() == "geometries"
	}
}

impl Deref for Ident {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Ident {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		EscapeIdent(&self.0).fmt(f)
	}
}

impl From<Table> for Ident {
	fn from(value: Table) -> Self {
		Ident(value.into_string())
	}
}

impl InfoStructure for Ident {
	fn structure(self) -> Value {
		self.to_raw_string().into()
	}
}
