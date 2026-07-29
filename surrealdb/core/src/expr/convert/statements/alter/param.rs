//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::param`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::param::*;

impl From<AlterParamStatement> for crate::expr::statements::alter::AlterParamStatement {
	fn from(v: AlterParamStatement) -> Self {
		crate::expr::statements::alter::AlterParamStatement {
			name: v.name,
			if_exists: v.if_exists,
			value: v.value.map(Into::into),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::alter::AlterParamStatement> for AlterParamStatement {
	fn from(v: crate::expr::statements::alter::AlterParamStatement) -> Self {
		AlterParamStatement {
			name: v.name,
			if_exists: v.if_exists,
			value: v.value.map(Into::into),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}
