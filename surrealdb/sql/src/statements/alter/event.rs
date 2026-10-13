use common::fmt::{Fmt, QuoteStr};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::{CoverStmts, EventKind, Expr, Literal};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER EVENT`.
pub struct AlterEventStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
	pub when: AlterKind<Expr>,
	pub then: AlterKind<Vec<Expr>>,
	pub comment: AlterKind<String>,
	pub kind: AlterKind<EventKind>,
}

impl Default for AlterEventStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
			when: AlterKind::None,
			then: AlterKind::None,
			comment: AlterKind::None,
			kind: AlterKind::None,
		}
	}
}

impl ToSql for AlterEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER EVENT");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what));

		match self.kind {
			AlterKind::Set(ref k) => match k {
				EventKind::Sync => {}
				EventKind::Async {
					retry,
					max_depth,
				} => {
					write_sql!(f, fmt, " ASYNC RETRY {} MAXDEPTH {}", retry, max_depth);
				}
			},
			AlterKind::Drop => f.push_str(" DROP ASYNC"),
			AlterKind::None => {}
		}

		match self.when {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " WHEN {}", CoverStmts(v)),
			AlterKind::Drop => f.push_str(" DROP WHEN"),
			AlterKind::None => {}
		}

		match self.then {
			AlterKind::Set(ref v) => {
				write_sql!(f, fmt, " THEN {}", Fmt::comma_separated(v.iter().map(CoverStmts)));
			}
			AlterKind::Drop => f.push_str(" DROP THEN"),
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
