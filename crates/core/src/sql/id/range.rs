use super::Id;
use crate::{
	err::Error,
	sql::{Range, SqlValue},
};
use anyhow::{Result, bail};
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
	type Error = anyhow::Error;
	fn try_from((beg, end): (Bound<Id>, Bound<Id>)) -> Result<Self, Self::Error> {
		if matches!(beg, Bound::Included(Id::Range(_)) | Bound::Excluded(Id::Range(_))) {
			bail!(Error::IdInvalid {
				value: "range".into(),
			});
		}

		if matches!(end, Bound::Included(Id::Range(_)) | Bound::Excluded(Id::Range(_))) {
			bail!(Error::IdInvalid {
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
	type Error = anyhow::Error;
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

impl TryFrom<SqlValue> for IdRange {
	type Error = anyhow::Error;
	fn try_from(v: SqlValue) -> Result<Self, Self::Error> {
		match v {
			SqlValue::Range(v) => IdRange::try_from(*v),
			v => Err(anyhow::Error::new(Error::IdInvalid {
				value: v.kindof().to_string(),
			})),
		}
	}
}

impl From<IdRange> for crate::expr::IdRange {
	fn from(v: IdRange) -> Self {
		Self {
			beg: match v.beg {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match v.end {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
		}
	}
}

impl From<crate::expr::IdRange> for IdRange {
	fn from(v: crate::expr::IdRange) -> Self {
		Self {
			beg: match v.beg {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match v.end {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
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
