//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::index`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::index::*;

impl From<AlterIndexStatement> for crate::expr::statements::alter::AlterIndexStatement {
	fn from(v: AlterIndexStatement) -> Self {
		crate::expr::statements::alter::AlterIndexStatement {
			name: v.name.into(),
			table: v.table.into(),
			if_exists: v.if_exists,
			prepare_remove: v.prepare_remove,
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterIndexStatement> for AlterIndexStatement {
	fn from(v: crate::expr::statements::alter::AlterIndexStatement) -> Self {
		AlterIndexStatement {
			name: v.name.into(),
			table: v.table.into(),
			if_exists: v.if_exists,
			prepare_remove: v.prepare_remove,
			comment: v.comment.into(),
		}
	}
}
