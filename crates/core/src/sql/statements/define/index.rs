use std::fmt::{self, Display};

use super::DefineKind;
use crate::fmt::{EscapeIdent, Fmt, QuoteStr};
use crate::sql::{Idiom, Index};

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: String,
	pub what: String,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub comment: Option<String>,
	pub concurrently: bool,
}

impl Display for DefineIndexStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE INDEX")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {} ON {}", EscapeIdent(&self.name), EscapeIdent(&self.what))?;
		if !self.cols.is_empty() {
			write!(f, " FIELDS {}", Fmt::comma_separated(self.cols.iter()))?;
		}
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", QuoteStr(v))?
		}
		if self.concurrently {
			write!(f, " CONCURRENTLY")?
		}
		Ok(())
	}
}

impl From<DefineIndexStatement> for crate::expr::statements::DefineIndexStatement {
	fn from(v: DefineIndexStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			what: v.what,
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment,
			concurrently: v.concurrently,
		}
	}
}

impl From<crate::expr::statements::DefineIndexStatement> for DefineIndexStatement {
	fn from(v: crate::expr::statements::DefineIndexStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name,
			what: v.what,
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment,
			concurrently: v.concurrently,
		}
	}
}
