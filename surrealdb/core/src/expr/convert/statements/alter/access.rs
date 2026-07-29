//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::access`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::sql::statements::alter::AlterKind;
use crate::sql::statements::alter::access::*;

impl From<AlterAccessStatement> for crate::expr::statements::alter::AlterAccessStatement {
	fn from(v: AlterAccessStatement) -> Self {
		crate::expr::statements::alter::AlterAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			authenticate: v.authenticate.into(),
			grant_duration: match v.grant_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d)),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			token_duration: match v.token_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d)),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			session_duration: match v.session_duration {
				AlterKind::Set(d) => crate::expr::statements::alter::AlterKind::Set(Some(d)),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Set(None),
				AlterKind::None => crate::expr::statements::alter::AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::alter::AlterAccessStatement> for AlterAccessStatement {
	fn from(v: crate::expr::statements::alter::AlterAccessStatement) -> Self {
		AlterAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			authenticate: v.authenticate.into(),
			grant_duration: match v.grant_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => AlterKind::Set(d),
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			token_duration: match v.token_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => AlterKind::Set(d),
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			session_duration: match v.session_duration {
				crate::expr::statements::alter::AlterKind::Set(Some(d)) => AlterKind::Set(d),
				crate::expr::statements::alter::AlterKind::Set(None) => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::Drop => AlterKind::Drop,
				crate::expr::statements::alter::AlterKind::None => AlterKind::None,
			},
			comment: v.comment.into(),
		}
	}
}
