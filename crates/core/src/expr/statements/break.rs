use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::value::Value;
use crate::expr::ControlFlow;
use crate::{ctx::Context, expr::FlowResult};

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

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context,
		_opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		Err(ControlFlow::Break)
	}
}

crate::expr::impl_display_from_sql!(BreakStatement);

impl crate::expr::DisplaySql for BreakStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("BREAK")
	}
}
