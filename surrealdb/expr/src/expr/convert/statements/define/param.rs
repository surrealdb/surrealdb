//! `sql` -> `expr` conversions for [`crate::sql::statements::define::param`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::param::*;

impl From<DefineParamStatement> for crate::expr::statements::DefineParamStatement {
	fn from(v: DefineParamStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			value: v.value.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineParamStatement> for DefineParamStatement {
	fn from(v: crate::expr::statements::DefineParamStatement) -> Self {
		DefineParamStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			value: v.value.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}
