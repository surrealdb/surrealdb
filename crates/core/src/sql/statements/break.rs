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

impl fmt::Display for BreakStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		f.write_str("BREAK")
	}
}
