//! `sql` -> `expr` conversions for [`crate::sql::statements::define::field`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::define::field::*;

impl From<DefineDefault> for crate::expr::statements::define::DefineDefault {
	fn from(value: DefineDefault) -> Self {
		match value {
			DefineDefault::None => crate::expr::statements::define::DefineDefault::None,
			DefineDefault::Always(expr) => {
				crate::expr::statements::define::DefineDefault::Always(expr.into())
			}
			DefineDefault::Set(expr) => {
				crate::expr::statements::define::DefineDefault::Set(expr.into())
			}
		}
	}
}

impl From<crate::expr::statements::define::DefineDefault> for DefineDefault {
	fn from(value: crate::expr::statements::define::DefineDefault) -> Self {
		match value {
			crate::expr::statements::define::DefineDefault::None => DefineDefault::None,
			crate::expr::statements::define::DefineDefault::Always(expr) => {
				DefineDefault::Always(expr.into())
			}
			crate::expr::statements::define::DefineDefault::Set(expr) => {
				DefineDefault::Set(expr.into())
			}
		}
	}
}

impl From<DefineFieldStatement> for crate::expr::statements::DefineFieldStatement {
	fn from(v: DefineFieldStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			flexible: v.flexible,
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment.into(),
			reference: v.reference.map(Into::into),
			graphql_alias: v.graphql_alias,
			graphql_deprecated: v.graphql_deprecated,
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineFieldStatement> for DefineFieldStatement {
	fn from(v: crate::expr::statements::DefineFieldStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			readonly: v.readonly,
			field_kind: v.field_kind.map(Into::into),
			flexible: v.flexible,
			value: v.value.map(Into::into),
			assert: v.assert.map(Into::into),
			computed: v.computed.map(Into::into),
			default: v.default.into(),
			permissions: v.permissions.into(),
			comment: v.comment.into(),
			reference: v.reference.map(Into::into),
			graphql_alias: v.graphql_alias,
			graphql_deprecated: v.graphql_deprecated,
		}
	}
}
