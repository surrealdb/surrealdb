use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveParamStatement {
	pub name: Strand,
	pub if_exists: bool,
}

impl ToSql for RemoveParamStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE PARAM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " ${}", common::fmt::EscapeKwFreeIdent(self.name.as_str()));
	}
}
