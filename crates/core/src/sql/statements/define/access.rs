use std::fmt::{self, Display};

use super::DefineKind;
use crate::fmt::CoverStmts;
use crate::sql::access::AccessDuration;
use crate::sql::{AccessType, Base, Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineAccessStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub base: Base,
	pub access_type: AccessType,
	pub authenticate: Option<Expr>,
	pub duration: AccessDuration,
	pub comment: Expr,
}

impl Display for DefineAccessStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE ACCESS")?;
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
		write!(f, " {} ON {} TYPE {}", CoverStmts(&self.name), self.base, self.access_type)?;
		// The additional authentication clause
		if let Some(ref v) = self.authenticate {
			write!(f, " AUTHENTICATE {}", CoverStmts(v))?
		}
		// Always print relevant durations so defaults can be changed in the future
		// If default values were not printed, exports would not be forward compatible
		// None values need to be printed, as they are different from the default values
		write!(f, " DURATION")?;
		if self.access_type.can_issue_grants() {
			write!(f, " FOR GRANT {},", CoverStmts(&self.duration.grant))?;
		}
		if self.access_type.can_issue_tokens() {
			write!(f, " FOR TOKEN {},", CoverStmts(&self.duration.token))?;
		}

		write!(f, " FOR SESSION {}", CoverStmts(&self.duration.session))?;
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write!(f, " COMMENT {}", CoverStmts(&self.comment))?;
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
			comment: v.comment.into(),
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
			comment: v.comment.into(),
		}
	}
}
