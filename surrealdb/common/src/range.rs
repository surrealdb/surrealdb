//! A range parameterised by a concrete element type.
//!
//! Lives below the value layer because the sql AST embeds `TypedRange<i64>`
//! in `Mock::Range`, and the parser constructs one directly.

use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};

/// A range of a specific type, can be converted back into a general range and
/// coerced from a general range.
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct TypedRange<T> {
	pub start: Bound<T>,
	pub end: Bound<T>,
}

impl<T: PartialOrd> PartialOrd for TypedRange<T> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		fn compare_bounds<T: PartialOrd>(a: &Bound<T>, b: &Bound<T>) -> Option<Ordering> {
			match a {
				Bound::Unbounded => match b {
					Bound::Unbounded => Some(Ordering::Equal),
					_ => Some(Ordering::Less),
				},
				Bound::Included(a) => match b {
					Bound::Unbounded => Some(Ordering::Greater),
					Bound::Included(b) => a.partial_cmp(b),
					Bound::Excluded(_) => Some(Ordering::Less),
				},
				Bound::Excluded(a) => match b {
					Bound::Excluded(b) => a.partial_cmp(b),
					_ => Some(Ordering::Greater),
				},
			}
		}

		match compare_bounds(&self.start, &other.start) {
			Some(Ordering::Equal) => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl<T: Clone> TypedRange<T> {
	pub fn from_range<R: RangeBounds<T>>(r: R) -> Self {
		TypedRange {
			start: r.start_bound().map(|x| x.clone()),
			end: r.end_bound().map(|x| x.clone()),
		}
	}
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

	/// Returns the length of this range, or `None` when the range is unbounded
	/// on either side or its length does not fit in a `usize`.
	#[allow(clippy::len_without_is_empty)]
	pub fn len(&self) -> Option<usize> {
		let end = match self.end {
			Bound::Unbounded => return None,
			Bound::Included(x) => x,
			Bound::Excluded(x) => match x.checked_sub(1) {
				Some(x) => x,
				None => return Some(0),
			},
		};

		let start = match self.start {
			Bound::Unbounded => return None,
			Bound::Included(x) => x,
			Bound::Excluded(x) => match x.checked_add(1) {
				Some(x) => x,
				None => return Some(0),
			},
		};

		if start > end {
			return Some(0);
		}

		usize::try_from(start.abs_diff(end)).ok()
	}
}

/// Iterator over `TypedRange<i64>`.
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
		if let Some(end) = self.end
			&& cur >= end
		{
			return None;
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
