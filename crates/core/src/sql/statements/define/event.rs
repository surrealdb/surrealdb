use crate::sql::{Expr, Ident};
use crate::val::Strand;
use std::fmt::{self, Display};

use super::DefineKind;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub what: Ident,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Option<Strand>,
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE"),
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS"),
		}
		write!(f, " {} ON {} WHEN {} THEN {}", self.name, self.what, self.when, self.then)?;
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {v}")?
		}
		Ok(())
	}
}

impl From<DefineEventStatement> for crate::expr::statements::DefineEventStatement {
	fn from(v: DefineEventStatement) -> Self {
		crate::expr::statements::DefineEventStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			when: v.when.into(),
			then: v.then.into(),
			comment: v.comment.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::DefineEventStatement> for DefineEventStatement {
	fn from(v: crate::expr::statements::DefineEventStatement) -> Self {
		DefineEventStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			what: v.what.into(),
			when: v.when.into(),
			then: v.then.into(),
			comment: v.comment.map(Into::into),
		}
	}
}
