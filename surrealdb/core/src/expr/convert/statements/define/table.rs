//! `sql` -> `expr` conversions for [`crate::sql::statements::define::table`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::table::*;

impl From<DefineTableStatement> for crate::expr::statements::DefineTableStatement {
	fn from(v: DefineTableStatement) -> Self {
		crate::expr::statements::DefineTableStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.into(),
			table_type: v.table_type.into(),
			graphql_alias: v.graphql_alias,
			graphql_deprecated: v.graphql_deprecated,
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineTableStatement> for DefineTableStatement {
	fn from(v: crate::expr::statements::DefineTableStatement) -> Self {
		DefineTableStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			drop: v.drop,
			full: v.full,
			view: v.view.map(Into::into),
			permissions: v.permissions.into(),
			changefeed: v.changefeed.map(Into::into),
			comment: v.comment.into(),
			table_type: v.table_type.into(),
			graphql_alias: v.graphql_alias,
			graphql_deprecated: v.graphql_deprecated,
		}
	}
}
