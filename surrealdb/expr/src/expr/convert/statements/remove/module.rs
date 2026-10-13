//! `sql` -> `expr` conversions for [`crate::sql::statements::removeule`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::module::*;

impl From<RemoveModuleStatement> for crate::expr::statements::RemoveModuleStatement {
	fn from(v: RemoveModuleStatement) -> Self {
		crate::expr::statements::RemoveModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveModuleStatement> for RemoveModuleStatement {
	fn from(v: crate::expr::statements::RemoveModuleStatement) -> Self {
		RemoveModuleStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
