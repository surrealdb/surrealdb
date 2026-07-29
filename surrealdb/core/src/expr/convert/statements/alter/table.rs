//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::table`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::table::*;

impl From<AlterTableStatement> for crate::expr::statements::alter::AlterTableStatement {
	fn from(v: AlterTableStatement) -> Self {
		crate::expr::statements::alter::AlterTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
			compact: v.compact,
		}
	}
}

impl From<crate::expr::statements::alter::AlterTableStatement> for AlterTableStatement {
	fn from(v: crate::expr::statements::alter::AlterTableStatement) -> Self {
		AlterTableStatement {
			name: v.name.into(),
			if_exists: v.if_exists,
			schemafull: v.schemafull.into(),
			permissions: v.permissions.map(Into::into),
			changefeed: v.changefeed.into(),
			comment: v.comment.into(),
			kind: v.kind.map(Into::into),
			compact: v.compact,
		}
	}
}
