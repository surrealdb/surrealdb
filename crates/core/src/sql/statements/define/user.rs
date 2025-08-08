use crate::sql::{
	Base, Duration, Ident, Strand, ToSql, escape::QuoteStr, fmt::Fmt, user::UserDuration,
};
use argon2::{
	Argon2,
	password_hash::{PasswordHasher, SaltString},
};

use rand::{Rng, distributions::Alphanumeric, rngs::OsRng};
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 4)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineUserStatement {
	pub name: Ident,
	pub base: Base,
	pub hash: String,
	pub code: String,
	pub roles: Vec<Ident>,
	#[revision(start = 3)]
	pub duration: UserDuration,
	pub comment: Option<Strand>,
	#[revision(start = 2)]
	pub if_not_exists: bool,
	#[revision(start = 4)]
	pub overwrite: bool,
}

#[expect(clippy::fallible_impl_from)]
impl From<(Base, &str, &str, &str)> for DefineUserStatement {
	fn from((base, user, pass, role): (Base, &str, &str, &str)) -> Self {
		DefineUserStatement {
			base,
			name: user.into(),
			hash: Argon2::default()
				.hash_password(pass.as_ref(), &SaltString::generate(&mut OsRng))
				.unwrap()
				.to_string(),
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			roles: vec![role.into()],
			duration: UserDuration::default(),
			comment: None,
			if_not_exists: false,
			overwrite: false,
		}
	}
}

impl DefineUserStatement {
	pub(crate) fn from_parsed_values(
		name: Ident,
		base: Base,
		roles: Vec<Ident>,
		duration: UserDuration,
	) -> Self {
		DefineUserStatement {
			name,
			base,
			roles,
			duration,
			code: rand::thread_rng()
				.sample_iter(&Alphanumeric)
				.take(128)
				.map(char::from)
				.collect::<String>(),
			..Default::default()
		}
	}

	pub(crate) fn set_password(&mut self, password: &str) {
		self.hash = Argon2::default()
			.hash_password(password.as_bytes(), &SaltString::generate(&mut OsRng))
			.unwrap()
			.to_string()
	}

	pub(crate) fn set_passhash(&mut self, passhash: String) {
		self.hash = passhash;
	}

	pub(crate) fn set_token_duration(&mut self, duration: Option<Duration>) {
		self.duration.token = duration;
	}

	pub(crate) fn set_session_duration(&mut self, duration: Option<Duration>) {
		self.duration.session = duration;
	}
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(
			f,
			" {} ON {} PASSHASH {} ROLES {}",
			self.name,
			self.base,
			QuoteStr(&self.hash),
			Fmt::comma_separated(
				&self.roles.iter().map(|r| r.to_string().to_uppercase()).collect::<Vec<String>>()
			),
		)?;
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		write!(
			f,
			" FOR TOKEN {},",
			match self.duration.token {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		write!(
			f,
			" FOR SESSION {}",
			match self.duration.session {
				Some(dur) => format!("{}", dur),
				None => "NONE".to_string(),
			}
		)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v.to_sql())?
		}
		Ok(())
	}
}

impl From<DefineUserStatement> for crate::expr::statements::DefineUserStatement {
	fn from(v: DefineUserStatement) -> Self {
		Self {
			name: v.name.into(),
			base: v.base.into(),
			hash: v.hash,
			code: v.code,
			roles: v.roles.into_iter().map(Into::into).collect(),
			duration: v.duration.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineUserStatement> for DefineUserStatement {
	fn from(v: crate::expr::statements::DefineUserStatement) -> Self {
		Self {
			name: v.name.into(),
			base: v.base.into(),
			hash: v.hash,
			code: v.code,
			roles: v.roles.into_iter().map(Into::into).collect(),
			duration: v.duration.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}
