use std::fmt;

use surrealdb_types::{SqlFormat, ToSql};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum Base {
	#[default]
	Root,
	Ns,
	Db,
}

impl fmt::Display for Base {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match self {
			Self::Ns => f.write_str("NAMESPACE"),
			Self::Db => f.write_str("DATABASE"),
			Self::Root => f.write_str("ROOT"),
		}
	}
}

impl ToSql for Base {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		let sql_base: crate::sql::Base = self.clone().into();
		sql_base.fmt_sql(f, sql_fmt);
	}
}
