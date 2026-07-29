//! `sql` -> `expr` conversions for [`crate::sql::statements::define::bucket`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::bucket::*;

impl From<DefineBucketStatement> for crate::expr::statements::define::DefineBucketStatement {
	fn from(v: DefineBucketStatement) -> Self {
		crate::expr::statements::define::DefineBucketStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::define::DefineBucketStatement> for DefineBucketStatement {
	fn from(v: crate::expr::statements::define::DefineBucketStatement) -> Self {
		DefineBucketStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			backend: v.backend.map(Into::into),
			permissions: v.permissions.into(),
			readonly: v.readonly,
			comment: v.comment.into(),
		}
	}
}
