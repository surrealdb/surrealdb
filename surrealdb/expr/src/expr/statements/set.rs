use common::fmt::EscapeKwFreeIdent;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use crate::expr::{CoverStmts, Expr, Kind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SetStatement {
	pub name: Strand,
	pub what: Expr,
	pub kind: Option<Kind>,
}

impl SetStatement {
	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		self.what.read_only()
	}

	/// returns if the set is setting a name a user may not set: a protected param or a name the
	/// system binds.
	pub fn is_protected_set(&self) -> bool {
		surrealdb_cnf::is_refused_let_name(self.name.as_str())
	}
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

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use crate::syn;

	#[test]
	fn check_type() {
		let query = syn::expr("LET $param = 5").unwrap();
		assert_eq!(query.to_sql(), "LET $param = 5");

		let query = syn::expr("LET $param: number = 5").unwrap();
		assert_eq!(query.to_sql(), "LET $param: number = 5");
	}
}
