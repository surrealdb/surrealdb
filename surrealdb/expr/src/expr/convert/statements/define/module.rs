//! `sql` -> `expr` conversions for [`crate::sql::statements::defineule`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::module::*;

impl From<DefineModuleStatement> for crate::expr::statements::DefineModuleStatement {
	fn from(v: DefineModuleStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.map(Into::into),
			executable: v.executable.into(),
			unsigned: v.unsigned,
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}

impl From<crate::expr::statements::DefineModuleStatement> for DefineModuleStatement {
	fn from(v: crate::expr::statements::DefineModuleStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.map(Into::into),
			executable: v.executable.into(),
			unsigned: v.unsigned,
			comment: v.comment.into(),
			permissions: v.permissions.into(),
		}
	}
}
