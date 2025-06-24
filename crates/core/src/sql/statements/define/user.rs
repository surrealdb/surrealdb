use super::DefineKind;
use crate::sql::escape::QuoteStr;
use crate::sql::fmt::Fmt;
use crate::sql::user::UserDuration;
use crate::sql::{Base, Duration, Ident, Strand};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum PassType {
	#[default]
	Unset,
	Hash(String),
	Password(String),
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineUserStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub base: Base,
	pub pass_type: PassType,
	pub roles: Vec<Ident>,
	pub token_duration: Option<Duration>,
	pub session_duration: Option<Duration>,

	pub comment: Option<Strand>,
}

impl Display for DefineUserStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE USER")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE"),
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS"),
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
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl From<DefineUserStatement> for crate::expr::statements::DefineUserStatement {
	fn from(v: DefineUserStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			hash: v.hash,
			roles: v.roles.into_iter().map(Into::into).collect(),
			duration: v.duration.into(),
			comment: v.comment.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineUserStatement> for DefineUserStatement {
	fn from(v: crate::expr::statements::DefineUserStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			hash: v.hash,
			roles: v.roles.into_iter().map(Into::into).collect(),
			duration: v.duration.into(),
			comment: v.comment.map(Into::into),
		}
	}
}
