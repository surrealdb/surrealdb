use std::fmt;
use std::ops::Deref;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::Ident;
use crate::expr::escape::EscapeIdent;
use crate::val::Strand;

/// A value type referencing a specific table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Table")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Table(String);

impl Table {
	/// Create a new strand, returns None if the string contains a null byte.
	pub fn new(s: String) -> Option<Table> {
		if s.contains('\0') {
			None
		} else {
			Some(Table(s))
		}
	}

	/// Create a new strand, without checking the string.
	///
	/// # Safety
	/// Caller must ensure that string handed as an argument does not contain
	/// any null bytes.
	pub unsafe fn new_unchecked(s: String) -> Table {
		// Check in debug mode if the variants
		debug_assert!(!s.contains('\0'));
		Table(s)
	}

	pub fn from_strand(s: Strand) -> Table {
		Table(s.into_string())
	}

	pub fn into_strand(self) -> Strand {
		unsafe { Strand::new_unchecked(self.0) }
	}

	pub fn into_string(self) -> String {
		self.0
	}

	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl Deref for Table {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<Ident> for Table {
	fn from(value: Ident) -> Self {
		Table(value.into_string())
	}
}

impl fmt::Display for Table {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		EscapeIdent(&self.0).fmt(f)
	}
}
