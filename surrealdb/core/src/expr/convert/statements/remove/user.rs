//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::user`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::user::*;

impl From<RemoveUserStatement> for crate::expr::statements::RemoveUserStatement {
	fn from(v: RemoveUserStatement) -> Self {
		crate::expr::statements::RemoveUserStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			base: v.base.into(),
		}
	}
}

impl From<crate::expr::statements::RemoveUserStatement> for RemoveUserStatement {
	fn from(v: crate::expr::statements::RemoveUserStatement) -> Self {
		RemoveUserStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			base: v.base.into(),
		}
	}
}
