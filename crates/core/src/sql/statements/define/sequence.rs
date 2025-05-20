
use crate::sql::{Ident, Timeout};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineSequenceStatement {
	pub name: Ident,
	pub if_not_exists: bool,
	pub overwrite: bool,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Timeout>,
}

impl Display for DefineSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SEQUENCE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
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
			name: v.name.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			batch: v.batch,
			start: v.start,
			timeout: v.timeout.map(Into::into),
		}
	}
}

impl From<crate::expr::statements::define::DefineSequenceStatement> for DefineSequenceStatement {
	fn from(v: crate::expr::statements::define::DefineSequenceStatement) -> Self {
		DefineSequenceStatement {
			name: v.name.into(),
			if_not_exists: v.if_not_exists,
			overwrite: v.overwrite,
			batch: v.batch,
			start: v.start,
			timeout: v.timeout.map(Into::into),
		}
	}
}
