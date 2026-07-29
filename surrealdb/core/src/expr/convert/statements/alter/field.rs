//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::field`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::field::*;

impl From<crate::expr::statements::alter::AlterDefault> for AlterDefault {
	fn from(value: crate::expr::statements::alter::AlterDefault) -> Self {
		match value {
			crate::expr::statements::alter::AlterDefault::None => AlterDefault::None,
			crate::expr::statements::alter::AlterDefault::Drop => AlterDefault::Drop,
			crate::expr::statements::alter::AlterDefault::Always(expr) => {
				AlterDefault::Always(expr.into())
			}
			crate::expr::statements::alter::AlterDefault::Set(expr) => {
				AlterDefault::Set(expr.into())
			}
		}
	}
}

impl From<AlterDefault> for crate::expr::statements::alter::AlterDefault {
	fn from(value: AlterDefault) -> Self {
		match value {
			AlterDefault::None => crate::expr::statements::alter::AlterDefault::None,
			AlterDefault::Drop => crate::expr::statements::alter::AlterDefault::Drop,
			AlterDefault::Always(expr) => {
				crate::expr::statements::alter::AlterDefault::Always(expr.into())
			}
			AlterDefault::Set(expr) => {
				crate::expr::statements::alter::AlterDefault::Set(expr.into())
			}
		}
	}
}

impl From<AlterFieldStatement> for crate::expr::statements::alter::AlterFieldStatement {
	fn from(v: AlterFieldStatement) -> Self {
		crate::expr::statements::alter::AlterFieldStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			kind: v.kind.into(),
			flexible: v.flexible.into(),
			readonly: v.readonly.into(),
			value: v.value.into(),
			assert: v.assert.into(),
			default: v.default.into(),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.into(),
			reference: v.reference.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterFieldStatement> for AlterFieldStatement {
	fn from(v: crate::expr::statements::alter::AlterFieldStatement) -> Self {
		AlterFieldStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			kind: v.kind.into(),
			flexible: v.flexible.into(),
			readonly: v.readonly.into(),
			value: v.value.into(),
			assert: v.assert.into(),
			default: v.default.into(),
			permissions: v.permissions.map(Into::into),
			comment: v.comment.into(),
			reference: v.reference.into(),
		}
	}
}
