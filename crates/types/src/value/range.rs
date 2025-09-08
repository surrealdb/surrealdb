use std::cmp::Ordering;
use std::ops::{Bound, RangeBounds};

use serde::{Deserialize, Serialize};

use crate::{SurrealValue, Value};

/// Represents a range of values in SurrealDB
///
/// A range defines an interval between two values with inclusive or exclusive bounds.
/// This is commonly used for range queries and comparisons.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct Range {
	/// The lower bound of the range
	pub start: Bound<Value>,
	/// The upper bound of the range
	pub end: Bound<Value>,
}

impl Range {
	/// Creates a new range with specified start and ending bounds.
	pub const fn new(start: Bound<Value>, end: Bound<Value>) -> Self {
		Range {
			start,
			end,
		}
	}

	/// Returns a range with no bounds.
	pub const fn unbounded() -> Self {
		Range {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}
}

impl<T: SurrealValue> From<std::ops::Range<T>> for Range {
	fn from(range: std::ops::Range<T>) -> Self {
		Range {
			start: Bound::Included(range.start.into_value()),
			end: Bound::Excluded(range.end.into_value()),
		}
	}
}

impl<T: SurrealValue> From<std::ops::RangeInclusive<T>> for Range {
	fn from(range: std::ops::RangeInclusive<T>) -> Self {
		let (start, end) = range.into_inner();
		Range {
			start: Bound::Included(start.into_value()),
			end: Bound::Included(end.into_value()),
		}
	}
}

impl<T: SurrealValue> From<std::ops::RangeFrom<T>> for Range {
	fn from(range: std::ops::RangeFrom<T>) -> Self {
		Range {
			start: Bound::Included(range.start.into_value()),
			end: Bound::Unbounded,
		}
	}
}

impl<T: SurrealValue> From<std::ops::RangeTo<T>> for Range {
	fn from(range: std::ops::RangeTo<T>) -> Self {
		Range {
			start: Bound::Unbounded,
			end: Bound::Excluded(range.end.into_value()),
		}
	}
}

impl From<std::ops::RangeFull> for Range {
	fn from(_: std::ops::RangeFull) -> Self {
		Range {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}
}

impl RangeBounds<Value> for Range {
	fn start_bound(&self) -> Bound<&Value> {
		self.start.as_ref()
	}

	fn end_bound(&self) -> Bound<&Value> {
		self.end.as_ref()
	}

	fn contains<U>(&self, item: &U) -> bool
	where
		U: ?Sized + PartialOrd<Value>,
		Value: PartialOrd<U>,
	{
		(match self.start_bound() {
			Bound::Unbounded => true,
			Bound::Included(start) => start <= item,
			Bound::Excluded(start) => start < item,
		}) && (match self.end_bound() {
			Bound::Unbounded => true,
			Bound::Included(end) => item <= end,
			Bound::Excluded(end) => item < end,
		})
	}
}

impl PartialOrd for Range {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Range {
	fn cmp(&self, other: &Self) -> Ordering {
		fn compare_bounds(a: &Bound<Value>, b: &Bound<Value>) -> Ordering {
			match a {
				Bound::Unbounded => match b {
					Bound::Unbounded => Ordering::Equal,
					_ => Ordering::Less,
				},
				Bound::Included(a) => match b {
					Bound::Unbounded => Ordering::Greater,
					Bound::Included(b) => a.cmp(b),
					Bound::Excluded(_) => Ordering::Less,
				},
				Bound::Excluded(a) => match b {
					Bound::Excluded(b) => a.cmp(b),
					_ => Ordering::Greater,
				},
			}
		}
		match compare_bounds(&self.start, &other.start) {
			Ordering::Equal => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}
