//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::config`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::config::*;

impl From<RemoveConfigStatement> for crate::expr::statements::remove::RemoveConfigStatement {
	fn from(v: RemoveConfigStatement) -> Self {
		crate::expr::statements::remove::RemoveConfigStatement {
			kind: v.kind.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::remove::RemoveConfigStatement> for RemoveConfigStatement {
	fn from(v: crate::expr::statements::remove::RemoveConfigStatement) -> Self {
		RemoveConfigStatement {
			kind: v.kind.into(),
			if_exists: v.if_exists,
		}
	}
}
