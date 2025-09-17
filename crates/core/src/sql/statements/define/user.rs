use std::fmt::{self, Display};

use argon2::Argon2;
use argon2::password_hash::{PasswordHasher, SaltString};
use rand::Rng;
use rand::distributions::Alphanumeric;
use rand::rngs::OsRng;

use super::DefineKind;
use crate::fmt::{EscapeIdent, Fmt, QuoteStr};
use crate::sql::Base;
use crate::val::Duration;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum PassType {
	#[default]
	Unset,
	Hash(String),
	Password(String),
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: String,
	pub base: Base,
	pub pass_type: PassType,
	pub roles: Vec<String>,
	pub token_duration: Option<Duration>,
	pub session_duration: Option<Duration>,

	pub comment: Option<String>,
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}

		write!(f, " {} ON {}", EscapeIdent(&self.name), self.base,)?;

		match self.pass_type {
			PassType::Unset => write!(f, "  PASSHASH \"\" ")?,
			PassType::Hash(ref x) => write!(f, "  PASSHASH {}", QuoteStr(x))?,
			PassType::Password(ref x) => write!(f, "  PASSWORD {}", QuoteStr(x))?,
		}

		write!(
			f,
			" ROLES {}",
			Fmt::comma_separated(
				&self
					.roles
					.iter()
					.map(|r| EscapeIdent(r.to_string().to_uppercase()))
					.collect::<Vec<_>>()
			),
		)?;
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		write!(
			f,
			" FOR TOKEN {},",
			match self.token_duration {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		write!(
			f,
			" FOR SESSION {}",
			match self.session_duration {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", QuoteStr(v))?
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
				.unwrap()
				.to_string(),
		};

		let code = rand::thread_rng()
			.sample_iter(&Alphanumeric)
			.take(128)
			.map(char::from)
			.collect::<String>();

		Self {
			kind: v.kind.into(),
			name: v.name,
			base: v.base.into(),
			hash,
			code,
			roles: v.roles,
			duration: crate::expr::user::UserDuration {
				token: v.token_duration,
				session: v.session_duration,
			},
			comment: v.comment,
		}
	}
}

impl From<crate::expr::statements::DefineUserStatement> for DefineUserStatement {
	fn from(v: crate::expr::statements::DefineUserStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			base: v.base.into(),
			pass_type: PassType::Hash(v.hash),
			roles: v.roles,
			token_duration: v.duration.token,
			session_duration: v.duration.session,
			comment: v.comment,
		}
	}
}
