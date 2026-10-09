use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveModelStatement {
	pub name: Strand,
	pub version: Strand,
	pub if_exists: bool,
}

impl ToSql for RemoveModelStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "REMOVE MODEL");
		if self.if_exists {
			write_sql!(f, sql_fmt, " IF EXISTS");
		}
		write_sql!(
			f,
			sql_fmt,
			" ml::{}<{}>",
			common::fmt::EscapeKwFreeIdent(self.name.as_str()),
			self.version.as_str()
		);
	}
}
