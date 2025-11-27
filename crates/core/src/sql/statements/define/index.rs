use std::fmt::{self, Display};

use super::DefineKind;
use crate::fmt::{CoverStmts, Fmt};
use crate::sql::{Expr, Index};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DefineIndexStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub what: Expr,
	pub cols: Vec<Expr>,
	pub index: Index,
	pub comment: Expr,
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
		write!(f, " {} ON {}", CoverStmts(&self.name), CoverStmts(&self.what))?;
		if !self.cols.is_empty() {
			write!(f, " FIELDS {}", Fmt::comma_separated(self.cols.iter().map(CoverStmts)))?;
		}
		if Index::Idx != self.index {
			write!(f, " {}", self.index)?;
		}
		write!(f, " COMMENT {}", CoverStmts(&self.comment))?;
		if self.concurrently {
			write!(f, " CONCURRENTLY")?;
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
			comment: v.comment.into(),
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
			comment: v.comment.into(),
			concurrently: v.concurrently,
		}
	}
}
