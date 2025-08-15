use std::fmt::{self, Display, Formatter};
use std::ops::Deref;

use crate::sql::escape::EscapeIdent;
use crate::val::Strand;

/// An identifier.
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Hash)]
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
	/// # Safety
	/// Caller should ensure that the string does not contain a null byte.
	pub unsafe fn new_unchecked(str: String) -> Self {
		Ident(str)
	}

	pub fn from_strand(strand: Strand) -> Self {
		Ident(strand.into_string())
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
}

impl From<Strand> for Ident {
	fn from(value: Strand) -> Self {
		Ident(value.into_string())
	}
}

impl From<Ident> for Strand {
	fn from(value: Ident) -> Self {
		value.into_strand()
	}
}

impl Deref for Ident {
	type Target = str;

	fn deref(&self) -> &Self::Target {
		self.0.as_str()
	}
}

impl From<crate::expr::Ident> for Ident {
	fn from(v: crate::expr::Ident) -> Self {
		Self(v.into_string())
	}
}

impl From<Ident> for crate::expr::Ident {
	fn from(v: Ident) -> Self {
		unsafe { Self::new_unchecked(v.into_string()) }
	}
}

impl Ident {
	/// Convert the Ident to a raw String
	pub fn to_raw(&self) -> String {
		self.0.to_string()
	}
}

impl Display for Ident {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		EscapeIdent(&self.0).fmt(f)
	}
}
