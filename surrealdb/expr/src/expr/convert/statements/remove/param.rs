//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::param`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::param::*;

impl From<RemoveParamStatement> for crate::expr::statements::RemoveParamStatement {
	fn from(v: RemoveParamStatement) -> Self {
		crate::expr::statements::RemoveParamStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::RemoveParamStatement> for RemoveParamStatement {
	fn from(v: crate::expr::statements::RemoveParamStatement) -> Self {
		RemoveParamStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
		}
	}
}
