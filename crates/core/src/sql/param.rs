use std::fmt;

use crate::sql::Ident;
use crate::sql::escape::EscapeKwFreeIdent;
use crate::val::Strand;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Param(String);

impl Param {
	/// Create a new identifier
	///
	/// This function checks if the string has a null byte, returns None if it
	/// has.
	pub fn new(str: String) -> Option<Self> {
		if str.contains('\0') {
			return None;
		}
		Some(Self(str))
	}

	/// Create a new identifier
	///
	/// # Safety
	/// Caller should ensure that the string does not contain a null byte.
	pub unsafe fn new_unchecked(str: String) -> Self {
		Self(str)
	}

	pub fn from_strand(strand: Strand) -> Self {
		Param(strand.into_string())
	}

	pub fn ident(self) -> Ident {
		unsafe { Ident::new_unchecked(self.0) }
	}

	// Convert into a string.
	pub fn into_string(self) -> String {
		self.0
	}
}

impl From<Ident> for Param {
	fn from(value: Ident) -> Self {
		Param(value.into_string())
	}
}

impl fmt::Display for Param {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "${}", EscapeKwFreeIdent(&self.0))
	}
}

impl From<Param> for crate::expr::Param {
	fn from(v: Param) -> Self {
		// Safety: Null byte guarenteed is upheld by param.
		unsafe { Self::new_unchecked(v.0) }
	}
}

impl From<crate::expr::Param> for Param {
	fn from(v: crate::expr::Param) -> Self {
		// Safety: Null byte guarenteed is upheld by param.
		unsafe { Self::new_unchecked(v.ident().into_string()) }
	}
}
