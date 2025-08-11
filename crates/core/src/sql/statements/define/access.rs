use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::access::AccessDuration;
use crate::sql::{AccessType, Base, Expr, Ident};
use crate::val::Strand;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Option<Strand>,
}

impl Display for DefineAccessStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ACCESS",)?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => {
				write!(f, " OVERWRITE")?;
			}
			DefineKind::IfNotExists => {
				write!(f, " IF NOT EXISTS")?;
			}
		}
		// The specific access method definition is displayed by AccessType
		write!(f, " {} ON {} TYPE {}", self.name, self.base, self.access_type)?;
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write!(f, " AUTHENTICATE {v}")?
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		if self.access_type.can_issue_grants() {
			write!(
				f,
				" FOR GRANT {},",
				match self.duration.grant {
					Some(dur) => format!("{}", dur),
					None => "NONE".to_string(),
				}
			)?;
		}
		if self.access_type.can_issue_tokens() {
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
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			access_type: v.access_type.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment,
		}
	}
}

impl From<crate::expr::statements::DefineAccessStatement> for DefineAccessStatement {
	fn from(v: crate::expr::statements::DefineAccessStatement) -> Self {
		DefineAccessStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			base: v.base.into(),
			access_type: v.access_type.into(),
			authenticate: v.authenticate.map(Into::into),
			duration: v.duration.into(),
			comment: v.comment,
		}
	}
}
