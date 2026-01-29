use std::ops::Deref;
use std::str;

use priority_lfu::DeepSizeOf;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash, DeepSizeOf)]
pub(crate) struct Script(pub String);

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

impl surrealdb_types::ToSql for Script {
	fn fmt_sql(&self, f: &mut String, fmt: surrealdb_types::SqlFormat) {
		let sql_script: crate::sql::Script = self.clone().into();
		sql_script.fmt_sql(f, fmt);
	}
}
