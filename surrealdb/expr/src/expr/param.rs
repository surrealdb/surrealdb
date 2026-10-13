use std::ops::Deref;
use std::str;

use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Param(Strand);

impl Param {
	/// Convert into the underlying `Strand`.
	pub fn into_strand(self) -> Strand {
		self.0
	}

	/// returns the identifier section of the parameter,
	/// i.e. `$foo` without the `$` so: `foo`
	pub fn as_str(&self) -> &str {
		self.0.as_str()
	}
}

impl From<String> for Param {
	fn from(v: String) -> Self {
		Self(v.into())
	}
}

impl From<Strand> for Param {
	fn from(v: Strand) -> Self {
		Self(v)
	}
}

impl Deref for Param {
	type Target = str;
	fn deref(&self) -> &Self::Target {
		self.0.as_str()
	}
}

impl ToSql for Param {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		f.push('$');
		EscapeKwFreeIdent(self.as_str()).fmt_sql(f, fmt);
	}
}
