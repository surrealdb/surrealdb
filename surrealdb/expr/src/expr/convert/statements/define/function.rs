//! `sql` -> `expr` conversions for [`crate::sql::statements::define::function`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::function::*;

impl From<DefineFunctionStatement> for crate::expr::statements::DefineFunctionStatement {
	fn from(v: DefineFunctionStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			args: v.args.into_iter().map(|(i, k)| (i, k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
			returns: v.returns.map(Into::into),
			graphql_alias: v.graphql_alias,
			graphql_deprecated: v.graphql_deprecated,
		}
	}
}

impl From<crate::expr::statements::DefineFunctionStatement> for DefineFunctionStatement {
	fn from(v: crate::expr::statements::DefineFunctionStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			args: v.args.into_iter().map(|(i, k)| (i, k.into())).collect(),
			block: v.block.into(),
			comment: v.comment.into(),
			permissions: v.permissions.into(),
			returns: v.returns.map(Into::into),
			graphql_alias: v.graphql_alias,
			graphql_deprecated: v.graphql_deprecated,
		}
	}
}
