use std::fmt::{self, Display};

use super::DefineKind;
use crate::{
	fmt::CoverStmts,
	sql::{Expr, Literal, Timeout},
};

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub(crate) struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub batch: Expr,
	pub start: Expr,
	pub timeout: Option<Timeout>,
}

impl Default for DefineSequenceStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			name: Expr::Literal(Literal::None),
			batch: Expr::Literal(Literal::Integer(0)),
			start: Expr::Literal(Literal::Integer(0)),
			timeout: None,
		}
	}
}

impl Display for DefineSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SEQUENCE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(
			f,
			" {} BATCH {} START {}",
			CoverStmts(&self.name),
			CoverStmts(&self.batch),
			CoverStmts(&self.start)
		)?;
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl From<DefineSequenceStatement> for crate::expr::statements::define::DefineSequenceStatement {
	fn from(v: DefineSequenceStatement) -> Self {
		Self {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch.into(),
			start: v.start.into(),
			timeout: v.timeout.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::define::DefineSequenceStatement> for DefineSequenceStatement {
	fn from(v: crate::expr::statements::define::DefineSequenceStatement) -> Self {
		DefineSequenceStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch.into(),
			start: v.start.into(),
			timeout: v.timeout.map(Into::into),
		}
	}
}
