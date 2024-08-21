use super::Id;
use crate::{
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{Range, Value},
};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, ops::Bound};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct IdRange {
	pub beg: Bound<Id>,
	pub end: Bound<Id>,
}

impl TryFrom<(Bound<Id>, Bound<Id>)> for IdRange {
	type Error = Error;
	fn try_from((beg, end): (Bound<Id>, Bound<Id>)) -> Result<Self, Self::Error> {
		if matches!(beg, Bound::Included(Id::Range(_)) | Bound::Excluded(Id::Range(_))) {
			return Err(Error::IdInvalid {
				value: "range".into(),
			});
		}

		if matches!(end, Bound::Included(Id::Range(_)) | Bound::Excluded(Id::Range(_))) {
			return Err(Error::IdInvalid {
				value: "range".into(),
			});
		}

		Ok(IdRange {
			beg,
			end,
		})
	}
}

impl TryFrom<Range> for IdRange {
	type Error = Error;
	fn try_from(v: Range) -> Result<Self, Self::Error> {
		let beg = match v.beg {
			Bound::Included(beg) => Bound::Included(Id::try_from(beg)?),
			Bound::Excluded(beg) => Bound::Excluded(Id::try_from(beg)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match v.end {
			Bound::Included(end) => Bound::Included(Id::try_from(end)?),
			Bound::Excluded(end) => Bound::Excluded(Id::try_from(end)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		// The TryFrom implementation ensures that the bounds do not contain an `Id::Range` value
		IdRange::try_from((beg, end))
	}
}

impl TryFrom<Value> for IdRange {
	type Error = Error;
	fn try_from(v: Value) -> Result<Self, Self::Error> {
		match v {
			Value::Range(v) => IdRange::try_from(*v),
			v => Err(Error::IdInvalid {
				value: v.kindof().to_string(),
			}),
		}
	}
}

impl PartialOrd for IdRange {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for IdRange {
	fn cmp(&self, other: &Self) -> Ordering {
		match &self.beg {
			Bound::Unbounded => match &other.beg {
				Bound::Unbounded => Ordering::Equal,
				_ => Ordering::Less,
			},
			Bound::Included(v) => match &other.beg {
				Bound::Unbounded => Ordering::Greater,
				Bound::Included(w) => match v.cmp(w) {
					Ordering::Equal => match &self.end {
						Bound::Unbounded => match &other.end {
							Bound::Unbounded => Ordering::Equal,
							_ => Ordering::Greater,
						},
						Bound::Included(v) => match &other.end {
							Bound::Unbounded => Ordering::Less,
							Bound::Included(w) => v.cmp(w),
							_ => Ordering::Greater,
						},
						Bound::Excluded(v) => match &other.end {
							Bound::Excluded(w) => v.cmp(w),
							_ => Ordering::Less,
						},
					},
					ordering => ordering,
				},
				_ => Ordering::Less,
			},
			Bound::Excluded(v) => match &other.beg {
				Bound::Excluded(w) => match v.cmp(w) {
					Ordering::Equal => match &self.end {
						Bound::Unbounded => match &other.end {
							Bound::Unbounded => Ordering::Equal,
							_ => Ordering::Greater,
						},
						Bound::Included(v) => match &other.end {
							Bound::Unbounded => Ordering::Less,
							Bound::Included(w) => v.cmp(w),
							_ => Ordering::Greater,
						},
						Bound::Excluded(v) => match &other.end {
							Bound::Excluded(w) => v.cmp(w),
							_ => Ordering::Less,
						},
					},
					ordering => ordering,
				},
				_ => Ordering::Greater,
			},
		}
	}
}

impl fmt::Display for IdRange {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		match &self.beg {
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

impl IdRange {
	/// Process the values in the bounds for this IdRange
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<IdRange, Error> {
		let beg = match &self.beg {
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

		// The TryFrom implementation ensures that the bounds do not contain an `Id::Range` value
		IdRange::try_from((beg, end))
	}
}
