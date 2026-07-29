//! `sql` -> `expr` conversions for [`crate::sql::statements::remove::namespace`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::remove::namespace::*;

impl From<RemoveNamespaceStatement> for crate::expr::statements::RemoveNamespaceStatement {
	fn from(v: RemoveNamespaceStatement) -> Self {
		crate::expr::statements::RemoveNamespaceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}

impl From<crate::expr::statements::RemoveNamespaceStatement> for RemoveNamespaceStatement {
	fn from(v: crate::expr::statements::RemoveNamespaceStatement) -> Self {
		RemoveNamespaceStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			expunge: v.expunge,
		}
	}
}
