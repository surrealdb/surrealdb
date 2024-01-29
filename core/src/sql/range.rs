use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{strand::no_nul_bytes, Id, Value};
use crate::syn;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;
use std::str::FromStr;

pub(crate) const TOKEN: &str = "$surrealdb::private::sql::Range";

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[revisioned(revision = 1)]
pub struct Range {
	#[serde(with = "no_nul_bytes")]
	pub tb: String,
	pub beg: Bound<Id>,
	pub end: Bound<Id>,
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
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Ok(Value::Range(Box::new(Range {
			tb: self.tb.clone(),
			beg: match &self.beg {
				Bound::Included(id) => Bound::Included(id.compute(ctx, opt, txn, doc).await?),
				Bound::Excluded(id) => Bound::Excluded(id.compute(ctx, opt, txn, doc).await?),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match &self.end {
				Bound::Included(id) => Bound::Included(id.compute(ctx, opt, txn, doc).await?),
				Bound::Excluded(id) => Bound::Excluded(id.compute(ctx, opt, txn, doc).await?),
				Bound::Unbounded => Bound::Unbounded,
			},
		})))
	}
}

impl PartialOrd for Range {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		match self.tb.partial_cmp(&other.tb) {
			Some(Ordering::Equal) => match &self.beg {
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
			},
			ordering => ordering,
		}
	}
}

impl fmt::Display for Range {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}:", self.tb)?;
		match &self.beg {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(id) => write!(f, "{id}"),
			Bound::Excluded(id) => write!(f, "{id}>"),
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(id) => write!(f, "..{id}"),
			Bound::Included(id) => write!(f, "..={id}"),
		}?;
		Ok(())
	}
}
