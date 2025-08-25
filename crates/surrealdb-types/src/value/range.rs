use std::cmp::Ordering;
use std::fmt::Display;
use std::ops::Bound;

use serde::{Deserialize, Serialize};

use crate::Value;

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
		match compare_bounds(&self.start, &other.end) {
			Ordering::Equal => compare_bounds(&self.end, &other.end),
			x => x,
		}
	}
}

impl Display for Range {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self.start {
			Bound::Unbounded => (),
			Bound::Included(start) => write!(f, "{}", start)?,
			Bound::Excluded(start) => write!(f, "{}>", start)?,
		}

		write!(f, "..")?;

		match &self.end {
			Bound::Unbounded => (),
			Bound::Included(end) => write!(f, "={}", end)?,
			Bound::Excluded(end) => write!(f, "{}", end)?,
		}

		Ok(())
	}
}
