use common::fmt::{Fmt, QuoteStr};
use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::literal::Literal;
use crate::statements::define::ApiAction;
use crate::statements::define::config::api::ApiConfig;
use crate::{ApiMethod, CoverStmts, Expr};

/// A single `FOR` clause within an `ALTER API` statement.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum AlterApiClause {
	/// `FOR any [config] [THEN expr | DROP THEN]`
	ForAny {
		config: Option<ApiConfig>,
		fallback: AlterKind<Expr>,
	},
	/// `FOR method1, method2 [config] THEN expr`
	SetAction(ApiAction),
	/// `FOR method1, method2 DROP THEN`
	DropAction {
		#[cfg_attr(feature = "arbitrary", arbitrary(with = crate::arbitrary::atleast_one))]
		methods: Vec<ApiMethod>,
	},
}

impl ToSql for AlterApiClause {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match self {
			AlterApiClause::ForAny {
				config,
				fallback,
			} => {
				f.push_str(" FOR any");
				if let Some(c) = config {
					write_sql!(f, fmt, "{}", c);
				}
				match fallback {
					AlterKind::Set(v) => write_sql!(f, fmt, " THEN {}", CoverStmts(v)),
					AlterKind::Drop => f.push_str(" DROP THEN"),
					AlterKind::None => {}
				}
			}
			AlterApiClause::SetAction(action) => {
				write_sql!(f, fmt, " {}", action);
			}
			AlterApiClause::DropAction {
				methods,
			} => {
				f.push_str(" FOR ");
				f.push_str(&Fmt::comma_separated(methods.iter()).to_sql());
				f.push_str(" DROP THEN");
			}
		}
	}
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER API`.
pub struct AlterApiStatement {
	pub path: Expr,
	pub if_exists: bool,
	pub clauses: Vec<AlterApiClause>,
	pub comment: AlterKind<String>,
}

impl Default for AlterApiStatement {
	fn default() -> Self {
		Self {
			path: Expr::Literal(Literal::None),
			if_exists: false,
			clauses: Vec::new(),
			comment: AlterKind::None,
		}
	}
}

impl ToSql for AlterApiStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		write_sql!(f, fmt, "ALTER API");
		if self.if_exists {
			write_sql!(f, fmt, " IF EXISTS");
		}
		write_sql!(f, fmt, " {}", CoverStmts(&self.path));

		for clause in &self.clauses {
			clause.fmt_sql(f, fmt);
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}
