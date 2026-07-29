//! `sql` -> `expr` conversions for [`crate::sql::statements::define::database`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::database::*;

impl From<DefineDatabaseStatement> for crate::expr::statements::DefineDatabaseStatement {
	fn from(v: DefineDatabaseStatement) -> Self {
		crate::expr::statements::DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			comment: v.comment.into(),
			changefeed: v.changefeed.map(Into::into),
			strict: v.strict,
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineDatabaseStatement> for DefineDatabaseStatement {
	fn from(v: crate::expr::statements::DefineDatabaseStatement) -> Self {
		DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			strict: v.strict,
			comment: v.comment.into(),
			changefeed: v.changefeed.map(Into::into),
		}
	}
}
