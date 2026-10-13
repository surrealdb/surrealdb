use std::ops::Bound;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::record_id::range::RecordIdKeyRangeLit;
use crate::val::RecordIdKeyRange;

/// Process the values in the bounds for this IdRange
pub(crate) async fn record_id_key_range_lit_compute(
	this: &RecordIdKeyRangeLit,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<RecordIdKeyRange> {
	let start = match &this.start {
		Bound::Included(beg) => Bound::Included(
			stk.run(|stk| crate::legacy::record_id_key_lit_compute(beg, stk, ctx, opt, doc))
				.await?,
		),
		Bound::Excluded(beg) => Bound::Excluded(
			stk.run(|stk| crate::legacy::record_id_key_lit_compute(beg, stk, ctx, opt, doc))
				.await?,
		),
		Bound::Unbounded => Bound::Unbounded,
	};

	let end = match &this.end {
		Bound::Included(end) => Bound::Included(
			stk.run(|stk| crate::legacy::record_id_key_lit_compute(end, stk, ctx, opt, doc))
				.await?,
		),
		Bound::Excluded(end) => Bound::Excluded(
			stk.run(|stk| crate::legacy::record_id_key_lit_compute(end, stk, ctx, opt, doc))
				.await?,
		),
		Bound::Unbounded => Bound::Unbounded,
	};

	// The TryFrom implementation ensures that the bounds do not contain an
	// `Id::Range` value
	Ok(RecordIdKeyRange {
		start,
		end,
	})
}
