use std::fmt::{self, Display};

use super::DefineKind;
use crate::sql::{Ident, Timeout};

#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Timeout>,
}

impl Display for DefineSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SEQUENCE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {} BATCH {} START {}", self.name, self.batch, self.start)?;
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
			batch: v.batch,
			start: v.start,
			timeout: v.timeout.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::define::DefineSequenceStatement> for DefineSequenceStatement {
	fn from(v: crate::expr::statements::define::DefineSequenceStatement) -> Self {
		DefineSequenceStatement {
			kind: v.kind.into(),
			name: v.name.into(),
			batch: v.batch,
			start: v.start,
			timeout: v.timeout.map(Into::into),
		}
	}
}
