//! `sql` -> `expr` conversions for [`crate::sql::statements::define::api`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::api::*;

impl From<DefineApiStatement> for crate::expr::statements::DefineApiStatement {
	fn from(v: DefineApiStatement) -> Self {
		crate::expr::statements::DefineApiStatement {
			kind: v.kind.into(),
			path: v.path.into(),
			actions: v.actions.into_iter().map(Into::into).collect(),
			fallback: v.fallback.map(Into::into),
			config: v.config.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineApiStatement> for DefineApiStatement {
	fn from(v: crate::expr::statements::DefineApiStatement) -> Self {
		DefineApiStatement {
			kind: v.kind.into(),
			path: v.path.into(),
			actions: v.actions.into_iter().map(Into::into).collect(),
			fallback: v.fallback.map(Into::into),
			config: v.config.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<ApiAction> for crate::expr::statements::define::ApiAction {
	fn from(v: ApiAction) -> Self {
		crate::expr::statements::define::ApiAction {
			methods: v.methods.into_iter().map(From::from).collect(),
			action: v.action.into(),
			config: v.config.into(),
		}
	}
}

impl From<crate::expr::statements::define::ApiAction> for ApiAction {
	fn from(v: crate::expr::statements::define::ApiAction) -> Self {
		ApiAction {
			methods: v.methods.into_iter().map(From::from).collect(),
			action: v.action.into(),
			config: v.config.into(),
		}
	}
}
