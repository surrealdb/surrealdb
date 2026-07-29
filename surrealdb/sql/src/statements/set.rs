use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::{CoverStmts, Expr, Kind};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct SetStatement {
	pub name: Strand,
	pub what: Expr,
	pub kind: Option<Kind>,
}

impl ToSql for SetStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "LET ${}", EscapeKwFreeIdent(self.name.as_str()));
		if let Some(ref kind) = self.kind {
			write_sql!(f, fmt, ": {}", kind);
		}
		write_sql!(f, fmt, " = {}", CoverStmts(&self.what));
	}
}
