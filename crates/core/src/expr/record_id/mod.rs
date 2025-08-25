use std::fmt;

use reblessive::tree::Stk;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResult;
use crate::expr::escape::EscapeRid;
use crate::val::RecordId;

pub mod key;
pub use key::{RecordIdKeyGen, RecordIdKeyLit};
pub mod range;
pub use range::RecordIdKeyRangeLit;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RecordIdLit {
	/// Table name
	pub table: String,
	pub key: RecordIdKeyLit,
}

impl RecordIdLit {
	pub(crate) fn is_static(&self) -> bool {
		self.key.is_static()
	}

	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<RecordId> {
		Ok(RecordId {
			table: self.table.clone(),
			key: self.key.compute(stk, ctx, opt, doc).await?,
		})
	}
}

impl fmt::Display for RecordIdLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:{}", EscapeRid(&self.table), self.key)
	}
}
