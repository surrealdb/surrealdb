//! `sql` -> `expr` conversions for [`crate::sql::statements::define::user`].
//!
//! Lives in core rather than beside the AST: `surrealdb-sql` sits below
//! core, so it cannot name `expr` types.

use rand::distr::{Alphanumeric, SampleString};

use crate::iam::ScramCredential;
use crate::sql::statements::define::user::*;

#[allow(clippy::fallible_impl_from)]
impl From<DefineUserStatement> for crate::expr::statements::DefineUserStatement {
	fn from(v: DefineUserStatement) -> Self {
		// An explicit PASSSCRAM verifier takes precedence (import/round-trip);
		// otherwise derive from the plaintext password when one is provided.
		// The verifier is validated at parse time (both DEFINE USER parsers call
		// `from_verifier_string` and bail on error), so `expect` upholds that
		// invariant loudly instead of silently dropping a bad verifier — the same
		// way the Argon2 hashing treats its impossible failure.
		let scram = if let Some(ref s) = v.scram {
			Some(
				ScramCredential::from_verifier_string(s)
					.expect("PASSSCRAM verifier must be validated at parse time"),
			)
		} else if let PassType::Password(ref p) = v.pass_type {
			Some(ScramCredential::generate(p))
		} else {
			None
		};

		let hash = match v.pass_type {
			PassType::Unset => String::new(),
			PassType::Hash(x) => x,
			// TODO: Move out of AST.
			PassType::Password(p) => crate::iam::hash_password(&p),
		};

		let code = Alphanumeric.sample_string(&mut rand::rng(), 128);

		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			hash,
			code,
			scram,
			roles: v.roles,
			duration: crate::expr::user::UserDuration {
				token: v.token_duration.into(),
				session: v.session_duration.into(),
			},
			comment: v.comment.into(),
		}
	}
}

impl From<crate::expr::statements::DefineUserStatement> for DefineUserStatement {
	fn from(v: crate::expr::statements::DefineUserStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			pass_type: PassType::Hash(v.hash),
			scram: v.scram.as_ref().map(|c| c.to_verifier_string()),
			roles: v.roles,
			token_duration: v.duration.token.into(),
			session_duration: v.duration.session.into(),
			comment: v.comment.into(),
		}
	}
}
