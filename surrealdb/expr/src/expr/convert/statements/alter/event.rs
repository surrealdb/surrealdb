//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::event`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::AlterKind;
use crate::sql::statements::alter::event::*;

impl From<AlterEventStatement> for crate::expr::statements::alter::AlterEventStatement {
	fn from(v: AlterEventStatement) -> Self {
		crate::expr::statements::alter::AlterEventStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			when: v.when.into(),
			then: match v.then {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(
					x.into_iter().map(Into::into).collect(),
				),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
			kind: v.kind.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterEventStatement> for AlterEventStatement {
	fn from(v: crate::expr::statements::alter::AlterEventStatement) -> Self {
		AlterEventStatement {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
			when: v.when.into(),
			then: match v.then {
				crate::expr::statements::alter::AlterKind::Set(x) => {
					AlterKind::Set(x.into_iter().map(Into::into).collect())
				}
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
			kind: v.kind.into(),
		}
	}
}
