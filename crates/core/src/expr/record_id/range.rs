use std::fmt;
use std::ops::Bound;

use anyhow::Result;
use reblessive::tree::Stk;

use super::RecordIdKeyLit;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::RecordIdKeyRange;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct RecordIdKeyRangeLit {
	pub start: Bound<RecordIdKeyLit>,
	pub end: Bound<RecordIdKeyLit>,
}

impl fmt::Display for RecordIdKeyRangeLit {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.start {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(v) => write!(f, "{v}"),
			Bound::Excluded(v) => write!(f, "{v}>"),
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(v) => write!(f, "..{v}"),
			Bound::Included(v) => write!(f, "..={v}"),
		}?;
		Ok(())
	}
}

impl RecordIdKeyRangeLit {
	pub(crate) fn is_static(&self) -> bool {
		let res = match &self.start {
			Bound::Included(x) => x.is_static(),
			Bound::Excluded(x) => x.is_static(),
			Bound::Unbounded => true,
		};

		if !res {
			return false;
		}

		match &self.end {
			Bound::Included(x) => x.is_static(),
			Bound::Excluded(x) => x.is_static(),
			Bound::Unbounded => true,
		}
	}

	/// Process the values in the bounds for this IdRange
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<RecordIdKeyRange> {
		let start = match &self.start {
			Bound::Included(beg) => {
				Bound::Included(stk.run(|stk| beg.compute(stk, ctx, opt, doc)).await?)
			}
			Bound::Excluded(beg) => {
				Bound::Excluded(stk.run(|stk| beg.compute(stk, ctx, opt, doc)).await?)
			}
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match &self.end {
			Bound::Included(end) => {
				Bound::Included(stk.run(|stk| end.compute(stk, ctx, opt, doc)).await?)
			}
			Bound::Excluded(end) => {
				Bound::Excluded(stk.run(|stk| end.compute(stk, ctx, opt, doc)).await?)
			}
			Bound::Unbounded => Bound::Unbounded,
		};

		// The TryFrom implementation ensures that the bounds do not contain an
		// `Id::Range` value
		Ok(RecordIdKeyRange {
			start,
			end,
		})
	}
}
