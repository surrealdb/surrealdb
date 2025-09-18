use std::fmt::{self, Display};

use super::DefineKind;
use crate::fmt::{EscapeIdent, QuoteStr};
use crate::sql::changefeed::ChangeFeed;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: String,
	pub comment: Option<String>,
	pub changefeed: Option<ChangeFeed>,
}

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", EscapeIdent(&self.name))?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", QuoteStr(v))?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		Ok(())
	}
}

impl From<DefineDatabaseStatement> for crate::expr::statements::DefineDatabaseStatement {
	fn from(v: DefineDatabaseStatement) -> Self {
		crate::expr::statements::DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name,
			comment: v.comment,
			changefeed: v.changefeed.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineDatabaseStatement> for DefineDatabaseStatement {
	fn from(v: crate::expr::statements::DefineDatabaseStatement) -> Self {
		DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name,
			comment: v.comment,
			changefeed: v.changefeed.map(Into::into),
		}
	}
}
