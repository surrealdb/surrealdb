use surrealdb_types::{SqlFormat, ToSql, write_sql};

use super::AlterKind;
use crate::fmt::{CoverStmts, QuoteStr};
use crate::sql::Expr;
use crate::sql::literal::Literal;
use crate::sql::statements::define::ApiAction;
use crate::sql::statements::define::config::api::ApiConfig;

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
/// AST node for `ALTER API`.
pub struct AlterApiStatement {
	pub path: Expr,
	pub if_exists: bool,
	pub actions: Option<Vec<ApiAction>>,
	pub fallback: AlterKind<Expr>,
	pub config: Option<ApiConfig>,
	pub comment: AlterKind<String>,
}

impl Default for AlterApiStatement {
	fn default() -> Self {
		Self {
			path: Expr::Literal(Literal::None),
			if_exists: false,
			actions: None,
			fallback: AlterKind::None,
			config: None,
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

		if let Some(ref config) = self.config {
			write_sql!(f, fmt, "{}", config);
		}

		match self.fallback {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " THEN {}", CoverStmts(v)),
			AlterKind::Drop => f.push_str(" DROP THEN"),
			AlterKind::None => {}
		}

		if let Some(ref actions) = self.actions {
			for action in actions {
				write_sql!(f, fmt, " {}", action);
			}
		}

		match self.comment {
			AlterKind::Set(ref v) => write_sql!(f, fmt, " COMMENT {}", QuoteStr(v)),
			AlterKind::Drop => f.push_str(" DROP COMMENT"),
			AlterKind::None => {}
		}
	}
}

impl From<AlterApiStatement> for crate::expr::statements::alter::AlterApiStatement {
	fn from(v: AlterApiStatement) -> Self {
		crate::expr::statements::alter::AlterApiStatement {
			path: v.path.into(),
			if_exists: v.if_exists,
			actions: v.actions.map(|a| a.into_iter().map(Into::into).collect()),
			fallback: v.fallback.into(),
			config: v.config.map(Into::into),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterApiStatement> for AlterApiStatement {
	fn from(v: crate::expr::statements::alter::AlterApiStatement) -> Self {
		AlterApiStatement {
			path: v.path.into(),
			if_exists: v.if_exists,
			actions: v.actions.map(|a| a.into_iter().map(Into::into).collect()),
			fallback: v.fallback.into(),
			config: v.config.map(Into::into),
			comment: v.comment.into(),
		}
	}
}
