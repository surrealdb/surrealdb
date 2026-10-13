pub(crate) mod key;
pub(crate) mod range;

use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::FlowResult;
use crate::expr::record_id::RecordIdLit;
use crate::val::RecordId;

pub(crate) async fn record_id_lit_compute(
	this: &RecordIdLit,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> FlowResult<RecordId> {
	Ok(RecordId {
		table: this.table.clone(),
		key: crate::legacy::record_id_key_lit_compute(&this.key, stk, ctx, opt, doc).await?,
	})
}
