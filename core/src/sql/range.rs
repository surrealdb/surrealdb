use super::{Datetime, Id};
use crate::cnf::GENERATION_ALLOCATION_LIMIT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::{Number, Subquery, Value};
use crate::syn;
use chrono::Duration;
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

impl Range {
	pub fn slice<'a, T>(&self, s: &'a [T]) -> Option<&'a [T]> {
		let r = match self.end {
			Bound::Included(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize();
				s.get(..=x)?
			}
			Bound::Excluded(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize();
				s.get(..x)?
			}
			Bound::Unbounded => s,
		};
		let r = match self.beg {
			Bound::Included(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize();
				r.get(x..)?
			}
			Bound::Excluded(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize().saturating_add(1);
				r.get(x..)?
			}
			Bound::Unbounded => r,
		};
		Some(r)
	}

	pub fn slice_mut<'a, T>(&self, s: &'a mut [T]) -> Option<&'a mut [T]> {
		let r = match self.end {
			Bound::Included(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize();
				s.get_mut(..x)?
			}
			Bound::Excluded(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize();
				s.get_mut(..=x)?
			}
			Bound::Unbounded => s,
		};
		let r = match self.beg {
			Bound::Included(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize();
				r.get_mut(x..)?
			}
			Bound::Excluded(ref x) => {
				let Value::Number(ref x) = x else {
					return None;
				};
				let x = x.to_usize().saturating_add(1);
				r.get_mut(x..)?
			}
			Bound::Unbounded => r,
		};
		Some(r)
	}
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

impl From<(Bound<Id>, Bound<Id>)> for Range {
	fn from(v: (Bound<Id>, Bound<Id>)) -> Self {
		fn convert(v: Bound<Id>) -> Bound<Value> {
			match v {
				Bound::Included(v) => Bound::Included(v.into()),
				Bound::Excluded(v) => Bound::Excluded(v.into()),
				Bound::Unbounded => Bound::Unbounded,
			}
		}

		Self {
			beg: convert(v.0),
			end: convert(v.1),
		}
	}
}

impl From<(Value, Value)> for Range {
	fn from(v: (Value, Value)) -> Self {
		Self {
			beg: Bound::Included(v.0),
			end: Bound::Excluded(v.1),
		}
	}
}

impl From<(Bound<Value>, Bound<Value>)> for Range {
	fn from(v: (Bound<Value>, Bound<Value>)) -> Self {
		Self {
			beg: v.0,
			end: v.1,
		}
	}
}

impl TryInto<std::ops::Range<i64>> for Range {
	type Error = Error;
	fn try_into(self) -> Result<std::ops::Range<i64>, Self::Error> {
		let beg = match self.beg {
			Bound::Unbounded => i64::MIN,
			Bound::Included(beg) => to_i64(beg)?,
			Bound::Excluded(beg) => to_i64(beg)? + 1,
		};

		let end = match self.end {
			Bound::Unbounded => i64::MAX,
			Bound::Included(end) => to_i64(end)? + 1,
			Bound::Excluded(end) => to_i64(end)?,
		};

		if (beg + *GENERATION_ALLOCATION_LIMIT as i64) < end {
			Err(Error::RangeTooBig {
				max: *GENERATION_ALLOCATION_LIMIT,
			})
		} else {
			Ok(beg..end)
		}
	}
}

impl TryInto<std::ops::Range<Datetime>> for Range {
	type Error = Error;
	fn try_into(self) -> Result<std::ops::Range<Datetime>, Self::Error> {
		let beg = match self.beg {
			Bound::Unbounded => Datetime::MIN_UTC,
			Bound::Included(beg) => to_datetime(beg)?,
			Bound::Excluded(beg) => Datetime::from(to_datetime(beg)?.0 + Duration::nanoseconds(1)),
		};

		let end = match self.end {
			Bound::Unbounded => Datetime::MAX_UTC,
			Bound::Included(end) => Datetime::from(to_datetime(end)?.0 + Duration::nanoseconds(1)),
			Bound::Excluded(end) => to_datetime(end)?,
		};

		Ok(beg..end)
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

	/// Validate that a Range contains only computed Values
	pub fn validate_computed(&self) -> Result<(), Error> {
		match &self.beg {
			Bound::Included(ref v) | Bound::Excluded(ref v) => v.validate_computed()?,
			Bound::Unbounded => {}
		}
		match &self.end {
			Bound::Included(ref v) | Bound::Excluded(ref v) => v.validate_computed()?,
			Bound::Unbounded => {}
		}

		Ok(())
	}
}

impl PartialOrd for Range {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
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
		fn bound_value(v: &Value) -> Value {
			if v.can_be_range_bound() {
				v.to_owned()
			} else {
				Value::Subquery(Box::new(Subquery::Value(v.to_owned())))
			}
		}

		match &self.beg {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(v) => write!(f, "{}", bound_value(v)),
			Bound::Excluded(v) => write!(f, "{}>", bound_value(v)),
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(v) => write!(f, "..{}", bound_value(v)),
			Bound::Included(v) => write!(f, "..={}", bound_value(v)),
		}?;
		Ok(())
	}
}

fn to_i64(v: Value) -> Result<i64, Error> {
	match v {
		Value::Number(Number::Int(v)) => Ok(v),
		v => Err(Error::InvalidRangeValue {
			expected: "int".to_string(),
			found: v.kindof().to_string(),
		}),
	}
}

fn to_datetime(v: Value) -> Result<Datetime, Error> {
	match v {
		Value::Datetime(v) => Ok(v),
		v => Err(Error::InvalidRangeValue {
			expected: "datetime".to_string(),
			found: v.kindof().to_string(),
		}),
	}
}

// Structs needed for revision convertion from old ranges

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Range")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct OldRange {
	pub tb: String,
	pub beg: Bound<Id>,
	pub end: Bound<Id>,
}
