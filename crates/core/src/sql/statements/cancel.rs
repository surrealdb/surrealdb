use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct CancelStatement;

impl From<CancelStatement> for crate::expr::statements::cancel::CancelStatement {
	fn from(_: CancelStatement) -> Self {
		Self
	}
}

impl From<crate::expr::statements::cancel::CancelStatement> for CancelStatement {
	fn from(_: crate::expr::statements::cancel::CancelStatement) -> Self {
		Self
	}
}

crate::sql::impl_display_from_sql!(CancelStatement);

impl crate::sql::DisplaySql for CancelStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("CANCEL TRANSACTION")
	}
}
