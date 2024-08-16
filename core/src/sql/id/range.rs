use crate::{
	ctx::Context,
	dbs::Options,
	doc::CursorDoc,
	err::Error,
	sql::{Range, Value},
};

use super::{value::IdValue, Id};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, ops::Bound};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct IdRange {
	pub beg: Bound<IdValue>,
	pub end: Bound<IdValue>,
}

impl From<(Bound<IdValue>, Bound<IdValue>)> for IdRange {
	fn from((beg, end): (Bound<IdValue>, Bound<IdValue>)) -> Self {
		IdRange {
			beg,
			end,
		}
	}
}

impl TryFrom<Range> for IdRange {
	type Error = Error;
	fn try_from(v: Range) -> Result<Self, Self::Error> {
		let beg = match v.beg {
			Bound::Included(beg) => Bound::Included(IdValue::try_from(beg)?),
			Bound::Excluded(beg) => Bound::Excluded(IdValue::try_from(beg)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match v.end {
			Bound::Included(end) => Bound::Included(IdValue::try_from(end)?),
			Bound::Excluded(end) => Bound::Excluded(IdValue::try_from(end)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Ok(IdRange {
			beg,
			end,
		})
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

impl TryFrom<Id> for IdRange {
	type Error = Error;
	fn try_from(v: Id) -> Result<Self, Self::Error> {
		match v {
			Id::Range(v) => Ok(v),
			Id::Value(_) => Err(Error::IdInvalid {
				value: "idvalue".to_string(),
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
	pub fn new(beg: Bound<IdValue>, end: Bound<IdValue>) -> IdRange {
		IdRange {
			beg,
			end,
		}
	}
}

impl IdRange {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<IdRange, Error> {
		let beg = match &self.beg {
			Bound::Included(beg) => Bound::Included(beg.compute(stk, ctx, opt, doc).await?),
			Bound::Excluded(beg) => Bound::Excluded(beg.compute(stk, ctx, opt, doc).await?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match &self.end {
			Bound::Included(end) => Bound::Included(end.compute(stk, ctx, opt, doc).await?),
			Bound::Excluded(end) => Bound::Excluded(end.compute(stk, ctx, opt, doc).await?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Ok(IdRange {
			beg,
			end,
		})
	}
}
