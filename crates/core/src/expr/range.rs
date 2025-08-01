use super::kind::HasKind;
use super::value::{Coerce, CoerceError, CoerceErrorExt as _};
use super::{Array, FlowResult, Id};
use crate::cnf::GENERATION_ALLOCATION_LIMIT;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Value;
use crate::expr::operator::BindingPower;
use crate::syn;
use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt;
use std::ops::Bound;
use std::str::FromStr;
use crate::sql::ToSql;

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
	pub fn can_coerce_to_typed<T: Coerce>(&self) -> bool {
		match self.beg {
			Bound::Included(ref x) | Bound::Excluded(ref x) => {
				if !x.can_coerce_to::<T>() {
					return false;
				}
			}
			Bound::Unbounded => {}
		}

		match self.end {
			Bound::Included(ref x) | Bound::Excluded(ref x) => x.can_coerce_to::<T>(),
			Bound::Unbounded => true,
		}
	}

	pub fn coerce_to_typed<T: Coerce + HasKind>(self) -> Result<TypedRange<T>, CoerceError> {
		let beg = match self.beg {
			Bound::Included(x) => Bound::Included(
				x.coerce_to::<T>().with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Excluded(x) => Bound::Excluded(
				x.coerce_to::<T>().with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match self.end {
			Bound::Included(x) => Bound::Included(
				x.coerce_to::<T>().with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Excluded(x) => Bound::Excluded(
				x.coerce_to::<T>().with_element_of(|| format!("range<{}>", T::kind().to_sql()))?,
			),
			Bound::Unbounded => Bound::Unbounded,
		};

		Ok(TypedRange {
			beg,
			end,
		})
	}

	pub fn slice<'a, T>(&self, s: &'a [T]) -> Option<&'a [T]> {
		let range = self.clone().coerce_to_typed::<i64>().ok()?;
		let r = match range.end {
			Bound::Included(x) => {
				//TODO: Handle negative truncation
				let x = x as usize;
				s.get(..=x)?
			}
			Bound::Excluded(x) => {
				//TODO: Handle negative truncation
				let x = x as usize;
				s.get(..x)?
			}
			Bound::Unbounded => s,
		};

		let r = match range.beg {
			Bound::Included(x) => {
				//TODO: Handle negative truncation
				let x = x as usize;
				r.get(x..)?
			}
			Bound::Excluded(x) => {
				//TODO: Handle negative truncation
				let x = (x as usize).saturating_add(1);
				r.get(x..)?
			}
			Bound::Unbounded => r,
		};
		Some(r)
	}

	pub fn slice_mut<'a, T>(&self, s: &'a mut [T]) -> Option<&'a mut [T]> {
		let range = self.clone().coerce_to_typed::<i64>().ok()?;
		let r = match range.end {
			Bound::Included(x) => {
				//TODO: Handle negative truncation
				let x = x as usize;
				s.get_mut(..=x)?
			}
			Bound::Excluded(x) => {
				//TODO: Handle negative truncation
				let x = x as usize;
				s.get_mut(..x)?
			}
			Bound::Unbounded => s,
		};

		let r = match range.beg {
			Bound::Included(x) => {
				//TODO: Handle negative truncation
				let x = x as usize;
				r.get_mut(x..)?
			}
			Bound::Excluded(x) => {
				//TODO: Handle negative truncation
				let x = (x as usize).saturating_add(1);
				r.get_mut(x..)?
			}
			Bound::Unbounded => r,
		};
		Some(r)
	}
}

/// A range but with specific value types.
#[derive(Clone, Debug)]
pub struct TypedRange<T> {
	pub beg: Bound<T>,
	pub end: Bound<T>,
}

impl TypedRange<i64> {
	/// Turn the typed range into an array, returning None if the size of the array would be too
	/// big.
	pub fn cast_to_array(self) -> Option<Array> {
		match self.size_hint().1 {
			Some(x) if x > *GENERATION_ALLOCATION_LIMIT => return None,
			None => return None,
			_ => {}
		}

		Some(Array(self.map(Value::from).collect()))
	}
}

impl Iterator for TypedRange<i64> {
	type Item = i64;

	fn next(&mut self) -> Option<i64> {
		let next = match self.beg {
			Bound::Included(x) => x,
			Bound::Excluded(x) => x.checked_add(1)?,
			Bound::Unbounded => i64::MIN,
		};
		match self.end {
			Bound::Unbounded => {}
			Bound::Excluded(x) => {
				if next >= x {
					return None;
				}
			}
			Bound::Included(x) => {
				if next > x {
					return None;
				}
			}
		}

		self.beg = match next.checked_add(1) {
			Some(x) => Bound::Included(x),
			None => Bound::Excluded(i64::MAX),
		};

		Some(next)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let end = match self.end {
			Bound::Unbounded => i64::MAX,
			Bound::Excluded(x) => match x.checked_sub(1) {
				Some(x) => x,
				// The upper bound is the lowest number excluded, so the iterator must be zero
				// length.
				None => return (0, Some(0)),
			},
			Bound::Included(x) => x,
		};
		let beg = match self.beg {
			Bound::Unbounded => i64::MIN,
			Bound::Excluded(x) => match x.checked_add(1) {
				Some(x) => x,
				// The lower bound is the highest number excluded, so the iterator must be zero
				// length.
				None => return (0, Some(0)),
			},
			Bound::Included(x) => x,
		};
		// beg and end are now the bounds inclusive.

		if beg > end {
			return (0, Some(0));
		}

		let len = beg.abs_diff(end);

		match usize::try_from(len) {
			Ok(x) => (x, Some(x)),
			Err(_) => (usize::MAX, None),
		}
	}
}

impl<T> From<TypedRange<T>> for Range
where
	Value: From<T>,
{
	fn from(value: TypedRange<T>) -> Self {
		Range {
			beg: value.beg.map(Value::from),
			end: value.end.map(Value::from),
		}
	}
}

impl FromStr for Range {
	type Err = ();
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match syn::range(s) {
			Ok(v) => Ok(v.into()),
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
	) -> FlowResult<Value> {
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
	pub fn validate_computed(&self) -> Result<()> {
		match &self.beg {
			Bound::Included(v) | Bound::Excluded(v) => v.validate_computed()?,
			Bound::Unbounded => {}
		}
		match &self.end {
			Bound::Included(v) | Bound::Excluded(v) => v.validate_computed()?,
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
		match &self.beg {
			Bound::Unbounded => write!(f, ""),
			Bound::Included(v) => {
				// We also () if the binding power is equal. This is because range has no defined
				// associativity. a..b..c is ambigous and could either be (a..b)..c or a..(b..c).
				// The syntax explicitly left this undefined and thus a..b..c without params is a
				// syntax error so we have to () any child range expression.
				if BindingPower::for_value(v) <= BindingPower::Range {
					write!(f, "({})", v)
				} else {
					write!(f, "{}", v)
				}
			}
			Bound::Excluded(v) => {
				if BindingPower::for_value(v) <= BindingPower::Range {
					write!(f, "({})>", v)
				} else {
					write!(f, "{}>", v)
				}
			}
		}?;
		match &self.end {
			Bound::Unbounded => write!(f, ".."),
			Bound::Excluded(v) => {
				if BindingPower::for_value(v) <= BindingPower::Range {
					write!(f, "..({})", v)
				} else {
					write!(f, "..{}", v)
				}
			}
			Bound::Included(v) => {
				if BindingPower::for_value(v) <= BindingPower::Range {
					write!(f, "..=({})", v)
				} else {
					write!(f, "..={}", v)
				}
			}
		}?;
		Ok(())
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
