//! `sql` -> `expr` conversions for [`crate::sql::statements::alter::user`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use crate::catalog::ScramCredential;
use crate::sql::statements::alter::AlterKind;
use crate::sql::statements::alter::user::*;
use crate::sql::statements::define::user::PassType;

impl From<AlterUserStatement> for crate::expr::statements::alter::AlterUserStatement {
	fn from(v: AlterUserStatement) -> Self {
		// Determine the SCRAM change before consuming `pass_type` for the hash.
		// An explicit PASSSCRAM wins; PASSWORD derives a new verifier; PASSHASH
		// clears any stale verifier (the plaintext is unknown); no clause leaves
		// SCRAM untouched. The verifier is validated at parse time, so `expect`
		// upholds that invariant loudly rather than silently clearing a bad one.
		let scram = if let Some(ref s) = v.scram {
			Some(Some(
				ScramCredential::from_verifier_string(s)
					.expect("PASSSCRAM verifier must be validated at parse time"),
			))
		} else {
			match v.pass_type {
				Some(PassType::Password(ref p)) => Some(Some(ScramCredential::generate(p))),
				Some(PassType::Hash(_)) => Some(None),
				Some(PassType::Unset) | None => None,
			}
		};

		let hash = v.pass_type.and_then(|pt| match pt {
			PassType::Unset => None,
			PassType::Hash(h) => Some(h),
			PassType::Password(p) => Some(crate::iam::hash_password(&p)),
		});

		crate::expr::statements::alter::AlterUserStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			hash,
			scram,
			roles: match v.roles {
				AlterKind::Set(x) => crate::expr::statements::alter::AlterKind::Set(x),
				AlterKind::Drop => crate::expr::statements::alter::AlterKind::Drop,
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

impl From<crate::expr::statements::alter::AlterUserStatement> for AlterUserStatement {
	fn from(v: crate::expr::statements::alter::AlterUserStatement) -> Self {
		AlterUserStatement {
			name: v.name.into(),
			base: v.base.into(),
			if_exists: v.if_exists,
			pass_type: v.hash.map(PassType::Hash),
			// A cleared verifier (`Some(None)`) has no SQL representation; the
			// accompanying PASSHASH already conveys the password change.
			scram: v.scram.flatten().map(|c| c.to_verifier_string()),
			roles: match v.roles {
				crate::expr::statements::alter::AlterKind::Set(x) => AlterKind::Set(x),
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
