use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::expr::filter::Filter;
use crate::expr::tokenizer::Tokenizer;
use crate::expr::{Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub function: Option<Strand>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Expr,
}

impl Default for DefineAnalyzerStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			function: None,
			tokenizers: None,
			filters: None,
			comment: Expr::Literal(Literal::None),
		}
	}
}
impl ToSql for DefineAnalyzerStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_stmt: crate::sql::statements::define::DefineAnalyzerStatement = self.clone().into();
		sql_stmt.fmt_sql(f, fmt);
	}
}
