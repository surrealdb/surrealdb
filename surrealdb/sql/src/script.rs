use std::ops::Deref;

use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Script(#[cfg_attr(feature = "arbitrary", arbitrary(default))] pub String);

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
