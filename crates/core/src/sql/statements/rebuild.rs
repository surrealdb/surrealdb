use crate::sql::ident::Ident;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub enum RebuildStatement {
	Index(RebuildIndexStatement),
}

impl RebuildStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		true
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

crate::sql::impl_display_from_sql!(RebuildStatement);

impl crate::sql::DisplaySql for RebuildStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		match self {
			Self::Index(v) => Display::fmt(v, f),
		}
	}
}

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RebuildIndexStatement {
	pub name: Ident,
	pub what: Ident,
	pub if_exists: bool,
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

crate::sql::impl_display_from_sql!(RebuildIndexStatement);

impl crate::sql::DisplaySql for RebuildIndexStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REBUILD INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
