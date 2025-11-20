use std::fmt::{self, Display};

use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, SaltString};
use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;

use super::DefineKind;
use crate::fmt::{EscapeKwFreeIdent, QuoteStr};
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
	pub token_duration: Option<Expr>,
	pub session_duration: Option<Expr>,

	pub comment: Option<Expr>,
}

impl Default for DefineUserStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			base: Base::Root,
			pass_type: PassType::Unset,
			roles: vec![],
			token_duration: None,
			session_duration: None,
			comment: None,
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

		write!(f, " {} ON {}", self.name, self.base)?;

		match self.pass_type {
			PassType::Unset => write!(f, " PASSHASH \"\" ")?,
			PassType::Hash(ref x) => write!(f, " PASSHASH {}", QuoteStr(x))?,
			PassType::Password(ref x) => write!(f, " PASSWORD {}", QuoteStr(x))?,
		}

		write!(f, " ROLES ")?;
		for (idx, r) in self.roles.iter().enumerate() {
			if idx != 0 {
				f.write_str(", ")?;
			}

			let r = r.to_uppercase();
			EscapeKwFreeIdent(&r).fmt(f)?;
		}

		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		write!(f, " FOR TOKEN ",)?;
		match self.token_duration {
			Some(Expr::Literal(Literal::None)) => f.write_str("(NONE)")?,
			Some(ref dur) => dur.fmt(f)?,
			None => f.write_str("NONE")?,
		}
		write!(f, ", FOR SESSION ",)?;
		match self.session_duration {
			Some(Expr::Literal(Literal::None)) => f.write_str("(NONE)")?,
			Some(ref dur) => dur.fmt(f)?,
			None => f.write_str("NONE")?,
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
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
				token: v.token_duration.map(Into::into),
				session: v.session_duration.map(Into::into),
			},
			comment: v.comment.map(|x| x.into()),
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
			token_duration: v.duration.token.map(Into::into),
			session_duration: v.duration.session.map(Into::into),
			comment: v.comment.map(|x| x.into()),
		}
	}
}
