use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveFunctionStatement {
	pub name: Strand,
	pub if_exists: bool,
}

impl ToSql for RemoveFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		// Bypass ident display since we don't want backticks arround the ident.
		write_sql!(f, fmt, "REMOVE FUNCTION");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " fn::{}", EscapeKwFreeIdent(self.name.as_str()));
	}
}
