use surrealdb_types::{SqlFormat, ToSql};

use crate::expr::{Data, Expr, Literal, Output};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct CreateStatement {
	// A keyword modifier indicating if we are expecting a single result or several
	pub only: bool,
	// Where we are creating (i.e. table, or record ID)
	pub what: Vec<Expr>,
	// The data associated with the record being created
	pub data: Option<Data>,
	//  What the result of the statement should resemble (i.e. Diff or no result etc).
	pub output: Option<Output>,
	// The timeout for the statement
	pub timeout: Expr,
}

impl Default for CreateStatement {
	fn default() -> Self {
		Self {
			only: Default::default(),
			what: Default::default(),
			data: Default::default(),
			output: Default::default(),
			timeout: Expr::Literal(Literal::None),
		}
	}
}

impl ToSql for CreateStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::create::CreateStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
