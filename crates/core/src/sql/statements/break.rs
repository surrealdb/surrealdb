use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::sql::value::Value;
use crate::sql::ControlFlow;
use crate::{ctx::Context, sql::FlowResult};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct BreakStatement;

impl BreakStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		false
	}
}

impl From<BreakStatement> for crate::expr::statements::BreakStatement {
	fn from(_v: BreakStatement) -> Self {
		Self {}
	}
}

impl From<crate::expr::statements::BreakStatement> for BreakStatement {
	fn from(_v: crate::expr::statements::BreakStatement) -> Self {
		Self {}
	}
}

crate::sql::impl_display_from_sql!(BreakStatement);

impl crate::sql::DisplaySql for BreakStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("BREAK")
	}
}
