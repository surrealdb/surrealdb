use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::Value;
use crate::syn;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;
use std::str::FromStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Range";

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Range {
	pub beg: Bound<Value>,
	pub end: Bound<Value>,
}

impl FromStr for Range {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		Self::try_from(s)
	}
}

impl TryFrom<&str> for Range {
	type Error = ();
	fn try_from(v: &str) -> Result<Self, Self::Error> {
		match syn::range(v) {
			Ok(v) => Ok(v),
			_ => Err(()),
		}
	}
}

impl Range {
	/// Construct a new range
	pub fn new(beg: Bound<Value>, end: Bound<Value>) -> Self {
		Self {
			beg,
			end,
		}
	}

	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value, Error> {
		Ok(Value::Range(Box::new(Range {
			beg: match &self.beg {
				Bound::Included(v) => Bound::Included(v.compute(stk, ctx, opt, doc).await?),
				Bound::Excluded(v) => Bound::Excluded(v.compute(stk, ctx, opt, doc).await?),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match &self.end {
				Bound::Included(v) => Bound::Included(v.compute(stk, ctx, opt, doc).await?),
				Bound::Excluded(v) => Bound::Excluded(v.compute(stk, ctx, opt, doc).await?),
				Bound::Unbounded => Bound::Unbounded,
			},
		})))
	}
}

impl PartialOrd for Range {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match &self.beg {
			Bound::Unbounded => match &other.beg {
				Bound::Unbounded => Some(Ordering::Equal),
				_ => Some(Ordering::Less),
			},
			Bound::Included(v) => match &other.beg {
				Bound::Unbounded => Some(Ordering::Greater),
				Bound::Included(w) => match v.partial_cmp(w) {
					Some(Ordering::Equal) => match &self.end {
						Bound::Unbounded => match &other.end {
							Bound::Unbounded => Some(Ordering::Equal),
							_ => Some(Ordering::Greater),
						},
						Bound::Included(v) => match &other.end {
							Bound::Unbounded => Some(Ordering::Less),
							Bound::Included(w) => v.partial_cmp(w),
							_ => Some(Ordering::Greater),
						},
						Bound::Excluded(v) => match &other.end {
							Bound::Excluded(w) => v.partial_cmp(w),
							_ => Some(Ordering::Less),
						},
					},
					ordering => ordering,
				},
				_ => Some(Ordering::Less),
			},
			Bound::Excluded(v) => match &other.beg {
				Bound::Excluded(w) => match v.partial_cmp(w) {
					Some(Ordering::Equal) => match &self.end {
						Bound::Unbounded => match &other.end {
							Bound::Unbounded => Some(Ordering::Equal),
							_ => Some(Ordering::Greater),
						},
						Bound::Included(v) => match &other.end {
							Bound::Unbounded => Some(Ordering::Less),
							Bound::Included(w) => v.partial_cmp(w),
							_ => Some(Ordering::Greater),
						},
						Bound::Excluded(v) => match &other.end {
							Bound::Excluded(w) => v.partial_cmp(w),
							_ => Some(Ordering::Less),
						},
					},
					ordering => ordering,
				},
				_ => Some(Ordering::Greater),
			},
		}
	}
}

impl Ord for Range {
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

impl fmt::Display for Range {
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
