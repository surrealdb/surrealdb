use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;

use crate::iam::{Action, ResourceKind};
use crate::sql::{AccessType, Base, Ident, SqlValue, Strand, access::AccessDuration};
use anyhow::{Result, bail};

use rand::Rng;
use rand::distributions::Alphanumeric;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 3)]
#[derive(Clone, Default, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineAccessStatement {
	pub name: Ident,
	pub base: Base,
	pub kind: AccessType,
	#[revision(start = 2)]
	pub authenticate: Option<SqlValue>,
	pub duration: AccessDuration,
	pub comment: Option<Strand>,
	pub if_not_exists: bool,
	#[revision(start = 3)]
	pub overwrite: bool,
}

impl DefineAccessStatement {
	/// Generate a random key to be used to sign session tokens
	/// This key will be used to sign tokens issued with this access method
	/// This value is used by default in every access method other than JWT
	pub(crate) fn random_key() -> String {
		rand::thread_rng().sample_iter(&Alphanumeric).take(128).map(char::from).collect::<String>()
	}

	/// Returns a version of the statement where potential secrets are redacted
	/// This function should be used when displaying the statement to datastore users
	/// This function should NOT be used when displaying the statement for export purposes
	pub fn redacted(&self) -> DefineAccessStatement {
		let mut das = self.clone();
		das.kind = match das.kind {
			AccessType::Jwt(ac) => AccessType::Jwt(ac.redacted()),
			AccessType::Record(mut ac) => {
				ac.jwt = ac.jwt.redacted();
				AccessType::Record(ac)
			}
			AccessType::Bearer(mut ac) => {
				ac.jwt = ac.jwt.redacted();
				AccessType::Bearer(ac)
			}
		};
		das
	}
}

impl Display for DefineAccessStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ACCESS",)?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		// The specific access method definition is displayed by AccessType
		write!(f, " {} ON {} TYPE {}", self.name, self.base, self.kind)?;
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write!(f, " AUTHENTICATE {v}")?
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		if self.kind.can_issue_grants() {
			write!(
				f,
				" FOR GRANT {},",
				match self.duration.grant {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		if self.kind.can_issue_tokens() {
			write!(
				f,
				" FOR TOKEN {},",
				match self.duration.token {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
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

impl From<DefineAccessStatement> for crate::expr::statements::DefineAccessStatement {
	fn from(v: DefineAccessStatement) -> Self {
		crate::expr::statements::DefineAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			kind: v.kind.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}

impl From<crate::expr::statements::DefineAccessStatement> for DefineAccessStatement {
	fn from(v: crate::expr::statements::DefineAccessStatement) -> Self {
		DefineAccessStatement {
			name: v.name.into(),
			base: v.base.into(),
			kind: v.kind.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment.map(Into::into),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
		}
	}
}
