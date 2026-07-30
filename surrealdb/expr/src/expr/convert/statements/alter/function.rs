//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::function`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::AlterKind;
use crate::sql::statements::alter::function::*;

impl From<AlterFunctionStatement> for crate::expr::statements::alter::AlterFunctionStatement {
	fn from(v: AlterFunctionStatement) -> Self {
		crate::expr::statements::alter::AlterFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
			args: match v.args {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(|(n, k)| (n, k.into())).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			block: v.block.into(),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
			returns: v.returns.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterFunctionStatement> for AlterFunctionStatement {
	fn from(v: crate::expr::statements::alter::AlterFunctionStatement) -> Self {
		AlterFunctionStatement {
			name: v.name,
			if_exists: v.if_exists,
			args: match v.args {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(|(n, k)| (n, k.into())).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			block: v.block.into(),
			comment: v.comment.into(),
			permissions: v.permissions.map(Into::into),
			returns: v.returns.into(),
		}
	}
}
