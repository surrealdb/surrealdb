//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::function`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::function::*;

impl From<RemoveFunctionStatement> for crate::expr::statements::RemoveFunctionStatement {
	fn from(v: RemoveFunctionStatement) -> Self {
		crate::expr::statements::RemoveFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveFunctionStatement> for RemoveFunctionStatement {
	fn from(v: crate::expr::statements::RemoveFunctionStatement) -> Self {
		RemoveFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
		}
	}
}
