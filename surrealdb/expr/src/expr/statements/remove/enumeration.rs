use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveEnumStatement {
	pub name: Strand,
	pub if_exists: bool,
}

impl ToSql for RemoveEnumStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "REMOVE ENUM");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(self.name.as_str()));
	}
}
