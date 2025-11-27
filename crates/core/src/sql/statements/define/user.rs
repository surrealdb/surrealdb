use std::fmt::{self, Display};

use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, SaltString};
use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;

use super::DefineKind;
use crate::fmt::{CoverStmts, EscapeKwFreeIdent, QuoteStr};
use crate::sql::{Base, Expr, Literal};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PassType {
	#[default]
	Unset,
	Hash(String),
	Password(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub pass_type: PassType,
	pub roles: Vec<String>,
	pub token_duration: Expr,
	pub session_duration: Expr,

	pub comment: Expr,
}

impl Default for DefineUserStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			pass_type: PassType::Unset,
			roles: vec![],
			token_duration: Expr::Literal(Literal::None),
			session_duration: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
		}
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}

		write!(f, " {} ON {}", CoverStmts(&self.name), &self.base)?;

		match self.pass_type {
			PassType::Unset => write!(f, " PASSHASH \"\" ")?,
			PassType::Hash(ref x) => write!(f, " PASSHASH {}", QuoteStr(x))?,
			PassType::Password(ref x) => write!(f, " PASSWORD {}", QuoteStr(x))?,
		}

		write!(f, " ROLES ")?;
		for (idx, r) in self.roles.iter().enumerate() {
			if idx != 0 {
				f.write_str(", ")?
			}

			let r = r.to_uppercase();
			EscapeKwFreeIdent(&r).fmt(f)?;
		}

		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		write!(f, " FOR TOKEN ",)?;
		CoverStmts(&self.token_duration).fmt(f)?;
		CoverStmts(&self.session_duration).fmt(f)?;
		write!(f, " COMMENT {}", CoverStmts(&self.comment))?;
		Ok(())
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<DefineUserStatement> for crate::expr::statements::DefineUserStatement {
	fn from(v: DefineUserStatement) -> Self {
		let hash = match v.pass_type {
			PassType::Unset => String::new(),
			PassType::Hash(x) => x,
			// TODO: Move out of AST.
			PassType::Password(p) => Argon2::default()
				.hash_password(p.as_bytes(), &SaltString::generate(&mut OsRng))
				.expect("password hashing should not fail")
				.to_string(),
		};

		let code = rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(128)
			.map(char::from)
			.collect::<String>();

		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			hash,
			code,
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
			roles: v.roles,
			token_duration: v.duration.token.into(),
			session_duration: v.duration.session.into(),
			comment: v.comment.into(),
		}
	}
}
