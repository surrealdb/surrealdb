use std::fmt;
use std::fmt::{Display, Formatter};

use crate::sql::ident::Ident;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl Display for RebuildStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Index(v) => Display::fmt(v, f),
		}
	}
}

impl From<RebuildStatement> for crate::expr::statements::rebuild::RebuildStatement {
	fn from(v: RebuildStatement) -> Self {
		match v {
			RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildStatement> for RebuildStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildStatement) -> Self {
		match v {
			crate::expr::statements::rebuild::RebuildStatement::Index(v) => Self::Index(v.into()),
		}
	}
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RebuildIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
}

impl Display for RebuildIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REBUILD INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}

impl From<RebuildIndexStatement> for crate::expr::statements::rebuild::RebuildIndexStatement {
	fn from(v: RebuildIndexStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}

impl From<crate::expr::statements::rebuild::RebuildIndexStatement> for RebuildIndexStatement {
	fn from(v: crate::expr::statements::rebuild::RebuildIndexStatement) -> Self {
		Self {
			name: v.name.into(),
			what: v.what.into(),
			if_exists: v.if_exists,
		}
	}
}
