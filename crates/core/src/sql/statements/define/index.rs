use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::fmt::Fmt;
use crate::sql::{Ident, Idiom, Index};
use crate::val::Strand;

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub what: Ident,
	pub cols: Vec<Idiom>,
	pub index: Index,
	pub comment: Option<Strand>,
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
		write!(
			f,
			" {} ON {} FIELDS {}",
			self.name,
			self.what,
			Fmt::comma_separated(self.cols.iter())
		)?;
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
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
			name: v.name.into(),
			what: v.what.into(),
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
			name: v.name.into(),
			what: v.what.into(),
			cols: v.cols.into_iter().map(From::from).collect(),
			index: v.index.into(),
			comment: v.comment,
			concurrently: v.concurrently,
		}
	}
}
