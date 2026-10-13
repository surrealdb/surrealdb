//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::table`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::table::*;

impl From<RemoveTableStatement> for crate::expr::statements::RemoveTableStatement {
	fn from(v: RemoveTableStatement) -> Self {
		crate::expr::statements::RemoveTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveTableStatement> for RemoveTableStatement {
	fn from(v: crate::expr::statements::RemoveTableStatement) -> Self {
		RemoveTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
