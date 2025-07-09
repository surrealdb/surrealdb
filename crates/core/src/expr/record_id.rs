use crate::dbs::Options;
use crate::expr::FlowResult;
use crate::expr::escape::EscapeRid;
use crate::expr::id::RecordIdKeyLit;
use crate::val::RecordId;
use crate::{ctx::Context, doc::CursorDoc};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdLit {
	/// Table name
	pub tb: String,
	pub id: RecordIdKeyLit,
}

impl RecordIdLit {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<RecordId> {
		Ok(RecordId {
			table: self.tb.clone(),
			key: self.id.compute(stk, ctx, opt, doc).await?,
		})
	}
}

impl fmt::Display for RecordIdLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.tb), self.id)
	}
}
