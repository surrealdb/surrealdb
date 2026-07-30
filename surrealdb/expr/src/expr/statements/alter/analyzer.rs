use surrealdb_types::{SqlFormat, ToSql};

use super::AlterKind;
use crate::expr::{Expr, Filter, Literal, Tokenizer};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct AlterAnalyzerStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub function: AlterKind<String>,
	pub tokenizers: AlterKind<Vec<Tokenizer>>,
	pub filters: AlterKind<Vec<Filter>>,
	pub comment: AlterKind<String>,
}

impl Default for AlterAnalyzerStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			function: AlterKind::None,
			tokenizers: AlterKind::None,
			filters: AlterKind::None,
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterAnalyzerStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterAnalyzerStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
