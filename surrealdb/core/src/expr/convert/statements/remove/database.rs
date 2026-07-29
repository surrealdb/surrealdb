//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::database`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::database::*;

impl From<RemoveDatabaseStatement> for crate::expr::statements::RemoveDatabaseStatement {
	fn from(v: RemoveDatabaseStatement) -> Self {
		crate::expr::statements::RemoveDatabaseStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveDatabaseStatement> for RemoveDatabaseStatement {
	fn from(v: crate::expr::statements::RemoveDatabaseStatement) -> Self {
		RemoveDatabaseStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
