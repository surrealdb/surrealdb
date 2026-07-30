use surrealdb_cnf::PROTECTED_PARAM_NAMES;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Expr, Kind};

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

	/// returns if the set is setting a protected param.
	pub fn is_protected_set(&self) -> bool {
		PROTECTED_PARAM_NAMES.contains(&self.name.as_str())
	}
}

impl ToSql for SetStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_stmt: crate::sql::statements::SetStatement = self.clone().into();
		sql_stmt.fmt_sql(f, fmt);
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
