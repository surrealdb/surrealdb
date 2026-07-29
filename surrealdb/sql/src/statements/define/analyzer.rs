use common::fmt::Fmt;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::analyzer_function::fmt_analyzer_function;
use crate::filter::Filter;
use crate::tokenizer::{Tokenizer, write_tokenizers_sql};
use crate::{CoverStmts, Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
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
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE ANALYZER");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
		if let Some(ref i) = self.function {
			fmt_analyzer_function(f, sql_fmt, i.as_str());
		}
		if let Some(v) = &self.tokenizers {
			write_sql!(f, sql_fmt, " TOKENIZERS ");
			write_tokenizers_sql(f, sql_fmt, v.iter().copied());
		}
		if let Some(v) = &self.filters {
			write_sql!(f, sql_fmt, " FILTERS {}", Fmt::comma_separated(v.iter()));
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}
