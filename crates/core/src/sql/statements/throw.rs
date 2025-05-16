use crate::sql::SqlValue;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ThrowStatement {
	pub error: SqlValue,
}

impl ThrowStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		false
	}
}

impl From<ThrowStatement> for crate::expr::statements::ThrowStatement {
	fn from(v: ThrowStatement) -> Self {
		Self {
			error: v.error.into(),
		}
	}
}

impl From<crate::expr::statements::ThrowStatement> for ThrowStatement {
	fn from(v: crate::expr::statements::ThrowStatement) -> Self {
		Self {
			error: v.error.into(),
		}
	}
}

crate::sql::impl_display_from_sql!(ThrowStatement);

impl crate::sql::DisplaySql for ThrowStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "THROW {}", self.error)
	}
}
