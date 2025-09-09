use std::fmt;
use std::ops::Deref;

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::expr::escape::EscapeIdent;

/// A value type referencing a specific table.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Table")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Table(String);

impl Table {
	/// Create a new strand, returns None if the string contains a null byte.
	pub fn new(s: String) -> Table {
		Table(s)
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
