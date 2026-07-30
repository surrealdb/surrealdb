//! `sql` -> `expr` conversions for [`crate::sql::statements::define::access`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::access::*;

impl From<DefineAccessStatement> for crate::expr::statements::DefineAccessStatement {
	fn from(v: DefineAccessStatement) -> Self {
		crate::expr::statements::DefineAccessStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			access_type: v.access_type.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineAccessStatement> for DefineAccessStatement {
	fn from(v: crate::expr::statements::DefineAccessStatement) -> Self {
		DefineAccessStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			access_type: v.access_type.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment.into(),
		}
	}
}
