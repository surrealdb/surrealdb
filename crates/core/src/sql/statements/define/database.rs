use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::changefeed::ChangeFeed;
use crate::sql::{Expr, Literal};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub strict: bool,
	pub comment: Option<Expr>,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: None,
			changefeed: None,
			strict: false,
		}
	}
}

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
		if self.strict {
			write!(f, " STRICT")?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?;
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
			name: v.name.into(),
			comment: v.comment.map(|x| x.into()),
			changefeed: v.changefeed.map(Into::into),
			strict: v.strict,
		}
	}
}

#[allow(clippy::fallible_impl_from)]
impl From<crate::expr::statements::DefineDatabaseStatement> for DefineDatabaseStatement {
	fn from(v: crate::expr::statements::DefineDatabaseStatement) -> Self {
		DefineDatabaseStatement {
			kind: v.kind.into(),
			id: v.id,
			name: v.name.into(),
			strict: v.strict,
			comment: v.comment.map(|x| x.into()),
			changefeed: v.changefeed.map(Into::into),
		}
	}
}
