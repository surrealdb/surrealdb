use crate::err::Error;
use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use std::ops::{
	Deref, {self},
};
use std::str;

use super::escape::QuoteStr;

/// A string that doesn't contain NUL bytes.
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Strand(String);

impl Deref for Strand {
	type Target = str;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

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

	pub fn into_inner(self) -> String {
		self.0
	}
}

impl From<Strand> for crate::val::Strand {
	fn from(v: Strand) -> Self {
		Self(v.0)
	}
}

impl From<crate::val::Strand> for Strand {
	fn from(v: crate::val::Strand) -> Self {
		Self(v.0)
	}
}

impl Display for Strand {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		QuoteStr(&self.0).fmt(f)
	}
}
