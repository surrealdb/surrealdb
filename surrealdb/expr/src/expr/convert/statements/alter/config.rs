//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::config`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::config::*;

impl From<AlterConfigStatement> for crate::expr::statements::alter::AlterConfigStatement {
	fn from(v: AlterConfigStatement) -> Self {
		crate::expr::statements::alter::AlterConfigStatement {
			if_exists: v.if_exists,
			inner: v.inner.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterConfigStatement> for AlterConfigStatement {
	fn from(v: crate::expr::statements::alter::AlterConfigStatement) -> Self {
		AlterConfigStatement {
			if_exists: v.if_exists,
			inner: v.inner.into(),
			comment: v.comment.into(),
		}
	}
}
