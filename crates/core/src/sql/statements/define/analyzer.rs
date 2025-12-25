use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::{CoverStmts, EscapeKwFreeIdent, Fmt};
use crate::sql::filter::Filter;
use crate::sql::tokenizer::Tokenizer;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineAnalyzerStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub function: Option<String>,
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
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " OVERWRITE"),
		}
		write_sql!(f, sql_fmt, " {}", self.name);
		if let Some(ref i) = self.function {
			f.push_str(" FUNCTION fn");
			for x in i.split("::") {
				f.push_str("::");
				EscapeKwFreeIdent(x).fmt_sql(f, sql_fmt);
			}
		}
		if let Some(v) = &self.tokenizers {
			let tokens: Vec<String> = v.iter().map(|f| f.to_sql()).collect();
			write_sql!(f, sql_fmt, " TOKENIZERS {}", tokens.join(","));
		}
		if let Some(v) = &self.filters {
			write_sql!(f, sql_fmt, " FILTERS {}", Fmt::comma_separated(v.iter()));
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
	}
}

impl From<DefineAnalyzerStatement> for crate::expr::statements::DefineAnalyzerStatement {
	fn from(v: DefineAnalyzerStatement) -> Self {
		crate::expr::statements::DefineAnalyzerStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			function: v.function,
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineAnalyzerStatement> for DefineAnalyzerStatement {
	fn from(v: crate::expr::statements::DefineAnalyzerStatement) -> Self {
		DefineAnalyzerStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			function: v.function,
			tokenizers: v.tokenizers.map(|v| v.into_iter().map(Into::into).collect()),
			filters: v.filters.map(|v| v.into_iter().map(Into::into).collect()),
			comment: v.comment.into(),
		}
	}
}
