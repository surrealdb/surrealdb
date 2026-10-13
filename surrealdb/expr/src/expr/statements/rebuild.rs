use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::val::TableName;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl ToSql for RebuildStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RebuildIndexStatement {
	pub name: Strand,
	pub table: TableName,
	pub if_exists: bool,
	pub concurrently: bool,
}

impl ToSql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::rebuild::RebuildIndexStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
