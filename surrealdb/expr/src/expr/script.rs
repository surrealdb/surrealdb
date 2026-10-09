use std::ops::Deref;
use std::str;

use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
pub struct Script(pub String);

impl From<String> for Script {
	fn from(s: String) -> Self {
		Self(s)
	}
}

impl From<&str> for Script {
	fn from(s: &str) -> Self {
		Self::from(String::from(s))
	}
}

impl Deref for Script {
	type Target = String;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl ToSql for Script {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		self.0.fmt_sql(f, fmt)
	}
}
