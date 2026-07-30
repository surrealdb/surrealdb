//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::api`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::api::*;

impl From<RemoveApiStatement> for crate::expr::statements::remove::RemoveApiStatement {
	fn from(v: RemoveApiStatement) -> Self {
		crate::expr::statements::remove::RemoveApiStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveApiStatement> for RemoveApiStatement {
	fn from(v: crate::expr::statements::remove::RemoveApiStatement) -> Self {
		RemoveApiStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
