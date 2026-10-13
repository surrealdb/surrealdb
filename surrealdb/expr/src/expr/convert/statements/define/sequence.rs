//! `sql` -> `expr` conversions for [`crate::sql::statements::define::sequence`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::sequence::*;

impl From<DefineSequenceStatement> for crate::expr::statements::define::DefineSequenceStatement {
	fn from(v: DefineSequenceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch.into(),
			start: v.start.into(),
			timeout: v.timeout.into(),
		}
	}
}

impl From<crate::expr::statements::define::DefineSequenceStatement> for DefineSequenceStatement {
	fn from(v: crate::expr::statements::define::DefineSequenceStatement) -> Self {
		DefineSequenceStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch.into(),
			start: v.start.into(),
			timeout: v.timeout.into(),
		}
	}
}
