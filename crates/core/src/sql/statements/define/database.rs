use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::DefineKind;
use crate::fmt::CoverStmts;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub strict: bool,
	pub comment: Expr,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
			changefeed: None,
			strict: false,
		}
	}
}

impl ToSql for DefineDatabaseStatement {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		write_sql!(f, sql_fmt, "DEFINE DATABASE");
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write_sql!(f, sql_fmt, " OVERWRITE"),
			DefineKind::IfNotExists => write_sql!(f, sql_fmt, " IF NOT EXISTS"),
		}
		write_sql!(f, sql_fmt, " {}", CoverStmts(&self.name));
		if self.strict {
			f.push_str(" STRICT");
		}
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write_sql!(f, sql_fmt, " COMMENT {}", CoverStmts(&self.comment));
		}
		if let Some(ref v) = self.changefeed {
			write_sql!(f, sql_fmt, " {v}");
		}
	}
}

impl From<DefineDatabaseStatement> for crate::expr::statements::DefineDatabaseStatement {
	fn from(v: DefineDatabaseStatement) -> Self {
		crate::expr::statements::DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.into(),
			changefeed: v.changefeed.map(Into::into),
			strict: v.strict,
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineDatabaseStatement> for DefineDatabaseStatement {
	fn from(v: crate::expr::statements::DefineDatabaseStatement) -> Self {
		DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			strict: v.strict,
			comment: v.comment.into(),
			changefeed: v.changefeed.map(Into::into),
		}
	}
}
