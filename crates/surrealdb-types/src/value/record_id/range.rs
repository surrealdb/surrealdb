use std::cmp::Ordering;
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::{Range, RecordIdKey};

/// Represents a range of record identifier keys in SurrealDB
///
/// This type is used for range queries on record identifiers,
/// allowing queries like "find all records with IDs between X and Y".
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RecordIdKeyRange {
	/// The lower bound of the range
	pub start: Bound<RecordIdKey>,
	/// The upper bound of the range
	pub end: Bound<RecordIdKey>,
}

impl RecordIdKeyRange {
	/// Converts this range into a `Range` value.
	pub fn into_value_range(self) -> Range {
		Range {
			start: self.start.map(RecordIdKey::into_value),
			end: self.end.map(RecordIdKey::into_value),
		}
	}

	/// Converts a `Range` value into a `RecordIdKeyRange`.
	pub fn from_value_range(range: Range) -> Option<Self> {
		let start = match range.start {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_value(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_value(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match range.end {
			Bound::Included(x) => Bound::Included(RecordIdKey::from_value(x)?),
			Bound::Excluded(x) => Bound::Excluded(RecordIdKey::from_value(x)?),
			Bound::Unbounded => Bound::Unbounded,
		};

		Some(RecordIdKeyRange {
			start,
			end,
		})
	}
}

impl PartialOrd for RecordIdKeyRange {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for RecordIdKeyRange {
	fn cmp(&self, other: &Self) -> Ordering {
		fn compare_bounds(a: &Bound<RecordIdKey>, b: &Bound<RecordIdKey>) -> Ordering {
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
		match compare_bounds(&self.start, &other.end) {
			Ordering::Equal => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl PartialEq<Range> for RecordIdKeyRange {
	fn eq(&self, other: &Range) -> bool {
		(match self.start {
			Bound::Included(ref a) => {
				if let Bound::Included(ref b) = other.start {
					a == b
				} else {
					false
				}
			}
			Bound::Excluded(ref a) => {
				if let Bound::Excluded(ref b) = other.start {
					a == b
				} else {
					false
				}
			}
			Bound::Unbounded => matches!(other.start, Bound::Unbounded),
		}) && (match self.end {
			Bound::Included(ref a) => {
				if let Bound::Included(ref b) = other.end {
					a == b
				} else {
					false
				}
			}
			Bound::Excluded(ref a) => {
				if let Bound::Excluded(ref b) = other.end {
					a == b
				} else {
					false
				}
			}
			Bound::Unbounded => matches!(other.end, Bound::Unbounded),
		})
	}
}
