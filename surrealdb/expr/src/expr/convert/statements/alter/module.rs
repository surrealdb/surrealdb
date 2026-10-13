//! `sql` -> `expr` conversions for [`crate::sql::statements::alterule`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::module::*;

impl From<AlterModuleStatement> for crate::expr::statements::alter::AlterModuleStatement {
	fn from(v: AlterModuleStatement) -> Self {
		crate::expr::statements::alter::AlterModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::alter::AlterModuleStatement> for AlterModuleStatement {
	fn from(v: crate::expr::statements::alter::AlterModuleStatement) -> Self {
		AlterModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
		}
	}
}
