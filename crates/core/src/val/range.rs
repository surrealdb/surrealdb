use crate::expr::kind::HasKind;
use crate::val::Value;
use crate::val::value::{Coerce, CoerceError};
use revision::revisioned;
use std::ops::Bound;

use super::value::CoerceErrorExt;

/// A range of surrealql values,
///
/// Can be any kind of values, "a"..1 is allowed.
#[revisioned(revision = 1)]
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct Range {
	pub start: Bound<Value>,
	pub end: Bound<Value>,
}

impl Range {
	pub fn can_coerce_to_typed<T: Coerce>(&self) -> bool {
		match self.start {
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
		let start = match self.start {
			Bound::Included(x) => {
				Bound::Included(T::coerce(x).with_element_of(|| format!("range<{}>", T::kind()))?)
			}
			Bound::Excluded(x) => {
				Bound::Excluded(T::coerce(x).with_element_of(|| format!("range<{}>", T::kind()))?)
			}
			Bound::Unbounded => Bound::Unbounded,
		};
		let end = match self.end {
			Bound::Included(x) => {
				Bound::Included(T::coerce(x).with_element_of(|| format!("range<{}>", T::kind()))?)
			}
			Bound::Excluded(x) => {
				Bound::Excluded(T::coerce(x).with_element_of(|| format!("range<{}>", T::kind()))?)
			}
			Bound::Unbounded => Bound::Unbounded,
		};
		Ok(TypedRange {
			start,
			end,
		})
	}
}

/// A range of a specific type, can be converted back into a general range and coerced from a
/// general range.
#[derive(Debug, Eq, PartialEq, Clone)]
pub struct TypedRange<T> {
	pub start: Bound<T>,
	pub end: Bound<T>,
}

impl TypedRange<i64> {
	/// Returns an iterator over this range.
	pub fn iter(self) -> IntegerRangeIter {
		let cur = match self.start {
			Bound::Included(x) => x,
			Bound::Excluded(x) => match x.checked_add(1) {
				Some(x) => x,
				// i64::MAX is excluded so the iterator will never return anything.
				None => {
					return IntegerRangeIter {
						cur: i64::MAX,
						end: Some(i64::MIN),
					};
				}
			},
			Bound::Unbounded => i64::MIN,
		};

		match self.end {
			Bound::Included(x) => IntegerRangeIter {
				cur,
				end: x.checked_add(1),
			},
			Bound::Excluded(x) => IntegerRangeIter {
				cur,
				end: Some(x),
			},
			Bound::Unbounded => IntegerRangeIter {
				cur,
				end: None,
			},
		}
	}

	pub fn slice<'a, T>(&self, s: &'a [T]) -> Option<&'a [T]> {
		let r = match self.end {
			Bound::Included(x) => s.get(..=(x as usize))?,
			Bound::Excluded(x) => s.get(..(x as usize))?,
			Bound::Unbounded => s,
		};
		match self.start {
			Bound::Included(x) => r.get((x as usize)..),
			Bound::Excluded(x) => {
				let x = (x as usize).checked_add(1)?;
				r.get(x..)
			}
			Bound::Unbounded => Some(r),
		}
	}

	pub fn slice_mut<'a, T>(&self, s: &'a mut [T]) -> Option<&'a mut [T]> {
		let r = match self.end {
			Bound::Included(x) => s.get_mut(..=(x as usize))?,
			Bound::Excluded(x) => s.get_mut(..(x as usize))?,
			Bound::Unbounded => s,
		};
		match self.start {
			Bound::Included(x) => r.get_mut((x as usize)..),
			Bound::Excluded(x) => {
				let x = (x as usize).checked_add(1)?;
				r.get_mut(x..)
			}
			Bound::Unbounded => Some(r),
		}
	}
}

impl<T> From<TypedRange<T>> for Range
where
	Value: From<T>,
{
	fn from(value: TypedRange<T>) -> Self {
		Range {
			start: value.start.map(From::from),
			end: value.end.map(From::from),
		}
	}
}

/// Iterator over TypedRange<i64>.
pub struct IntegerRangeIter {
	cur: i64,
	// Signifies the end of the iterator.
	// The iterator will stop returning if self.cur >= self.end
	// If end is None then i64::MAX is included.
	end: Option<i64>,
}

impl Iterator for IntegerRangeIter {
	type Item = i64;

	fn next(&mut self) -> Option<i64> {
		let cur = self.cur;
		if let Some(end) = self.end {
			if cur >= end {
				return None;
			}
		}
		if let Some(x) = cur.checked_add(1) {
			self.cur = x
		} else {
			// we have reached i64::MAX so after this we need to avoid returning anything.
			self.end = Some(i64::MIN)
		}

		Some(cur)
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		let len = if let Some(x) = self.end {
			if self.cur >= x {
				return (0, Some(0));
			}
			self.cur.abs_diff(x) - 1
		} else {
			self.cur.abs_diff(i64::MAX)
		};
		// handling if u64::MAX > usize::MAX
		let upper: Option<usize> = len.try_into().ok();
		(upper.unwrap_or(usize::MAX), upper)
	}
}
