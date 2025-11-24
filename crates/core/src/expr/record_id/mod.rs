use std::fmt;

use reblessive::tree::Stk;

use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResult;
use crate::val::RecordId;
use crate::{ctx::Context, fmt::EscapeIdent};

pub(crate) mod key;
pub(crate) use key::{RecordIdKeyGen, RecordIdKeyLit};
pub(crate) mod range;
pub(crate) use range::RecordIdKeyRangeLit;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RecordIdLit {
	/// Table name
	pub table: String,
	pub(crate) key: RecordIdKeyLit,
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
		write!(f, "{}:{}", EscapeIdent(&self.table), self.key)
	}
}
