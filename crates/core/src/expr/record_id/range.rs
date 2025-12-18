use std::ops::Bound;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::RecordIdKeyLit;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::val::RecordIdKeyRange;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RecordIdKeyRangeLit {
	pub(crate) start: Bound<RecordIdKeyLit>,
	pub(crate) end: Bound<RecordIdKeyLit>,
}

impl RecordIdKeyRangeLit {
	pub(crate) fn is_static(&self) -> bool {
		let res = match &self.start {
			Bound::Included(x) => x.is_pure(),
			Bound::Excluded(x) => x.is_pure(),
			Bound::Unbounded => true,
		};

		if !res {
			return false;
		}

		match &self.end {
			Bound::Included(x) => x.is_pure(),
			Bound::Excluded(x) => x.is_pure(),
			Bound::Unbounded => true,
		}
	}

	/// Process the values in the bounds for this IdRange
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
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

impl ToSql for RecordIdKeyRangeLit {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let range: crate::sql::record_id::range::RecordIdKeyRangeLit = self.clone().into();
		range.fmt_sql(f, fmt);
	}
}
