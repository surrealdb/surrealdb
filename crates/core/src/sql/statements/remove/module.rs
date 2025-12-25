use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::sql::ModuleName;

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

impl From<RemoveModuleStatement> for crate::expr::statements::RemoveModuleStatement {
	fn from(v: RemoveModuleStatement) -> Self {
		crate::expr::statements::RemoveModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
impl From<crate::expr::statements::RemoveModuleStatement> for RemoveModuleStatement {
	fn from(v: crate::expr::statements::RemoveModuleStatement) -> Self {
		RemoveModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
