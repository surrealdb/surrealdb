use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::ModuleName;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveModuleStatement {
	pub name: ModuleName,
	pub if_exists: bool,
}

impl ToSql for RemoveModuleStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE MODULE");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(f, sql_fmt, " {}", self.name);
	}
}
