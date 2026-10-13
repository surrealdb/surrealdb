use common::fmt::{Fmt, QuoteStr};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::analyzer_function::fmt_analyzer_function;
use crate::filter::Filter;
use crate::tokenizer::{Tokenizer, write_tokenizers_sql};
use crate::{CoverStmts, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER ANALYZER`.
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
		write_sql!(f, fmt, "ALTER ANALYZER");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", CoverStmts(&self.name));

		match self.function {
			AlterKind::Set(ref v) => fmt_analyzer_function(f, fmt, v),
			AlterKind::Drop => f.push_str(" DROP FUNCTION"),
			AlterKind::None => {}
		}

		match self.tokenizers {
			AlterKind::Set(ref v) => {
				write_sql!(f, fmt, " TOKENIZERS ");
				write_tokenizers_sql(f, fmt, v.iter().copied());
			}
			AlterKind::Drop => f.push_str(" DROP TOKENIZERS"),
			AlterKind::None => {}
		}

		match self.filters {
			AlterKind::Set(ref v) => {
				write_sql!(f, fmt, " FILTERS {}", Fmt::comma_separated(v.iter()));
			}
			AlterKind::Drop => f.push_str(" DROP FILTERS"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
