//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::api`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::api::*;

impl From<AlterApiClause> for crate::expr::statements::alter::AlterApiClause {
	fn from(v: AlterApiClause) -> Self {
		match v {
			AlterApiClause::ForAny {
				config,
				fallback,
			} => crate::expr::statements::alter::AlterApiClause::ForAny {
				config: config.map(Into::into),
				fallback: fallback.into(),
			},
			AlterApiClause::SetAction(a) => {
				crate::expr::statements::alter::AlterApiClause::SetAction(a.into())
			}
			AlterApiClause::DropAction {
				methods,
			} => crate::expr::statements::alter::AlterApiClause::DropAction {
				methods: methods.into_iter().map(From::from).collect(),
			},
		}
	}
}

impl From<crate::expr::statements::alter::AlterApiClause> for AlterApiClause {
	fn from(v: crate::expr::statements::alter::AlterApiClause) -> Self {
		match v {
			crate::expr::statements::alter::AlterApiClause::ForAny {
				config,
				fallback,
			} => AlterApiClause::ForAny {
				config: config.map(Into::into),
				fallback: fallback.into(),
			},
			crate::expr::statements::alter::AlterApiClause::SetAction(a) => {
				AlterApiClause::SetAction(a.into())
			}
			crate::expr::statements::alter::AlterApiClause::DropAction {
				methods,
			} => AlterApiClause::DropAction {
				methods: methods.into_iter().map(From::from).collect(),
			},
		}
	}
}

impl From<AlterApiStatement> for crate::expr::statements::alter::AlterApiStatement {
	fn from(v: AlterApiStatement) -> Self {
		crate::expr::statements::alter::AlterApiStatement {
			path: v.path.into(),
			if_exists: v.if_exists,
			clauses: v.clauses.into_iter().map(Into::into).collect(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterApiStatement> for AlterApiStatement {
	fn from(v: crate::expr::statements::alter::AlterApiStatement) -> Self {
		AlterApiStatement {
			path: v.path.into(),
			if_exists: v.if_exists,
			clauses: v.clauses.into_iter().map(Into::into).collect(),
			comment: v.comment.into(),
		}
	}
}
