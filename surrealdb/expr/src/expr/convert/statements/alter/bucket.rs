//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::bucket`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::bucket::*;

impl From<AlterBucketStatement> for crate::expr::statements::alter::AlterBucketStatement {
	fn from(v: AlterBucketStatement) -> Self {
		crate::expr::statements::alter::AlterBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			backend: v.backend.into(),
			permissions: v.permissions.map(Into::into),
			readonly: v.readonly.into(),
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterBucketStatement> for AlterBucketStatement {
	fn from(v: crate::expr::statements::alter::AlterBucketStatement) -> Self {
		AlterBucketStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			backend: v.backend.into(),
			permissions: v.permissions.map(Into::into),
			readonly: v.readonly.into(),
			comment: v.comment.into(),
		}
	}
}
