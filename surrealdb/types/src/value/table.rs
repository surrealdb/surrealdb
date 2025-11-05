use std::fmt::Display;
use std::ops::Deref;

use serde::{Deserialize, Serialize};

use crate::utils::escape::EscapeSqonIdent;
use crate::{ToSql, write_sql};

/// A value type referencing a specific table.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Table(String);

impl Table {
	/// Create a new table.
	pub fn new(s: impl Into<String>) -> Self {
		Table(s.into())
	}

	/// Convert the table to a string.
	pub fn into_string(self) -> String {
		self.0
	}

	/// Get the table as a string.
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

impl Deref for Table {
	type Target = String;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl Display for Table {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.0)
	}
}

impl ToSql for Table {
	fn fmt_sql(&self, f: &mut String) {
		write_sql!(f, "{}", EscapeSqonIdent(&self.0))
	}
}

impl From<&str> for Table {
	fn from(s: &str) -> Self {
		Table::new(s.to_string())
	}
}

impl From<String> for Table {
	fn from(s: String) -> Self {
		Table::new(s)
	}
}
