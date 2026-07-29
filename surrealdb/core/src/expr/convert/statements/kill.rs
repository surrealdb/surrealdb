//! `sql` -> `expr` conversions for [`crate::sql::statements::kill`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::kill::*;

impl From<KillStatement> for crate::expr::statements::KillStatement {
	fn from(v: KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::statements::KillStatement> for KillStatement {
	fn from(v: crate::expr::statements::KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}
