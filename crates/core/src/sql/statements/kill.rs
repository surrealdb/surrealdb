use crate::sql::SqlValue;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct KillStatement {
	// Uuid of Live Query
	// or Param resolving to Uuid of Live Query
	pub id: SqlValue,
}

impl From<KillStatement> for crate::expr::statements::KillStatement {
	fn from(v: KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}

impl From<crate::expr::statements::KillStatement> for KillStatement {
	fn from(v: crate::expr::statements::KillStatement) -> Self {
		Self {
			id: v.id.into(),
		}
	}
}

crate::sql::impl_display_from_sql!(KillStatement);

impl crate::sql::DisplaySql for KillStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "KILL {}", self.id)
	}
}
