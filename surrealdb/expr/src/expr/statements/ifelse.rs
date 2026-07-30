use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::Expr;

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of
	/// else if's
	pub exprs: Vec<(Expr, Expr)>,
	/// the final else body, if there is one
	pub close: Option<Expr>,
}

impl IfelseStatement {
	/// Check if we require a writeable transaction
	pub fn read_only(&self) -> bool {
		self.exprs.iter().all(|x| x.0.read_only() && x.1.read_only())
			&& self.close.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}

	/// Check if any branch directly contains a data-modifying statement.
	pub fn has_direct_write(&self) -> bool {
		self.exprs.iter().any(|x| x.0.has_direct_write() || x.1.has_direct_write())
			|| self.close.as_ref().map(|x| x.has_direct_write()).unwrap_or(false)
	}
}

impl ToSql for IfelseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::ifelse::IfelseStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::syn;

	#[test]
	fn format_pretty() {
		let query = syn::expr("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(query.to_sql(), "IF 1 { 1 } ELSE IF 2 { 2 }");
		// Single-statement blocks stay inline even in pretty mode
		assert_eq!(query.to_sql_pretty(), "IF 1 { 1 } ELSE IF 2 { 2 }");
	}
}
