use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{EscapeKwFreeIdent, Fmt, QuoteStr};
use crate::sql::filter::Filter;
use crate::sql::tokenizer::Tokenizer;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER ANALYZER`.
pub struct AlterAnalyzerStatement {
	pub name: String,
	pub if_exists: bool,
	pub function: AlterKind<String>,
	pub tokenizers: AlterKind<Vec<Tokenizer>>,
	pub filters: AlterKind<Vec<Filter>>,
	pub comment: AlterKind<String>,
}

impl ToSql for AlterAnalyzerStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER ANALYZER");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", EscapeKwFreeIdent(&self.name));

		match self.function {
			AlterKind::Set(ref v) => {
				f.push_str(" FUNCTION fn");
				for x in v.split("::") {
					f.push_str("::");
					EscapeKwFreeIdent(x).fmt_sql(f, fmt);
				}
			}
			AlterKind::Drop => f.push_str(" DROP FUNCTION"),
			AlterKind::None => {}
		}

		match self.tokenizers {
			AlterKind::Set(ref v) => {
				let tokens: Vec<String> = v.iter().map(|t| t.to_sql()).collect();
				write_sql!(f, fmt, " TOKENIZERS {}", tokens.join(","));
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

impl From<AlterAnalyzerStatement> for crate::expr::statements::alter::AlterAnalyzerStatement {
	fn from(v: AlterAnalyzerStatement) -> Self {
		crate::expr::statements::alter::AlterAnalyzerStatement {
			name: v.name,
			if_exists: v.if_exists,
			function: v.function.into(),
			tokenizers: match v.tokenizers {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(Into::into).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			filters: match v.filters {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(Into::into).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterAnalyzerStatement> for AlterAnalyzerStatement {
	fn from(v: crate::expr::statements::alter::AlterAnalyzerStatement) -> Self {
		AlterAnalyzerStatement {
			name: v.name,
			if_exists: v.if_exists,
			function: v.function.into(),
			tokenizers: match v.tokenizers {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(Into::into).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			filters: match v.filters {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(Into::into).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}
