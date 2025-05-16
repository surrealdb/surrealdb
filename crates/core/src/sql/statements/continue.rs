
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct ContinueStatement;

impl ContinueStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		false
	}
}

impl From<ContinueStatement> for crate::expr::statements::ContinueStatement {
	fn from(_v: ContinueStatement) -> Self {
		Self {}
	}
}

impl From<crate::expr::statements::ContinueStatement> for ContinueStatement {
	fn from(_v: crate::expr::statements::ContinueStatement) -> Self {
		Self {}
	}
}

crate::sql::impl_display_from_sql!(ContinueStatement);

impl crate::sql::DisplaySql for ContinueStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("CONTINUE")
	}
}
