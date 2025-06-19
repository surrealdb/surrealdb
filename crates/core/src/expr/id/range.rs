use super::RecordIdKeyLit;
use crate::{ctx::Context, dbs::Options, doc::CursorDoc, err::Error, expr::Value};
use anyhow::{Result, bail};
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::{cmp::Ordering, fmt, ops::Bound};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct KeyRange {
	pub beg: Bound<RecordIdKeyLit>,
	pub end: Bound<RecordIdKeyLit>,
}

impl TryFrom<(Bound<RecordIdKeyLit>, Bound<RecordIdKeyLit>)> for KeyRange {
	type Error = anyhow::Error;
	fn try_from(
		(beg, end): (Bound<RecordIdKeyLit>, Bound<RecordIdKeyLit>),
	) -> Result<Self, Self::Error> {
		if matches!(
			beg,
			Bound::Included(RecordIdKeyLit::Range(_)) | Bound::Excluded(RecordIdKeyLit::Range(_))
		) {
			bail!(Error::IdInvalid {
				value: "range".into(),
			});
		}

		if matches!(
			end,
			Bound::Included(RecordIdKeyLit::Range(_)) | Bound::Excluded(RecordIdKeyLit::Range(_))
		) {
			bail!(Error::IdInvalid {
				value: "range".into(),
			});
		}

		Ok(KeyRange {
			beg,
			end,
		})
	}
}

impl TryFrom<Range> for KeyRange {
	type Error = anyhow::Error;
	fn try_from(v: Range) -> Result<Self, Self::Error> {
		let beg = match v.beg {
			Bound::Included(beg) => Bound::Included(RecordIdKeyLit::try_from(beg)?),
			Bound::Excluded(beg) => Bound::Excluded(RecordIdKeyLit::try_from(beg)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match v.end {
			Bound::Included(end) => Bound::Included(RecordIdKeyLit::try_from(end)?),
			Bound::Excluded(end) => Bound::Excluded(RecordIdKeyLit::try_from(end)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		// The TryFrom implementation ensures that the bounds do not contain an `Id::Range` value
		KeyRange::try_from((beg, end))
	}
}

impl TryFrom<Value> for KeyRange {
	type Error = anyhow::Error;
	fn try_from(v: Value) -> Result<Self, Self::Error> {
		match v {
			Value::Range(v) => KeyRange::try_from(*v),
			v => Err(anyhow::Error::new(Error::IdInvalid {
				value: v.kindof().to_string(),
			})),
		}
	}
}

impl PartialOrd for KeyRange {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for KeyRange {
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

impl fmt::Display for KeyRange {
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

impl KeyRange {
	/// Process the values in the bounds for this IdRange
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<KeyRange> {
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
		KeyRange::try_from((beg, end))
	}
}
