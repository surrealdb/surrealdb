//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::index`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::index::*;

impl From<RemoveIndexStatement> for crate::expr::statements::RemoveIndexStatement {
	fn from(v: RemoveIndexStatement) -> Self {
		crate::expr::statements::RemoveIndexStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveIndexStatement> for RemoveIndexStatement {
	fn from(v: crate::expr::statements::RemoveIndexStatement) -> Self {
		RemoveIndexStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			what: v.what.into(),
		}
	}
}
