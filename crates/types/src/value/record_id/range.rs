use std::cmp::Ordering;
use std::ops::{Bound, RangeFrom, RangeFull, RangeTo, RangeToInclusive};

use serde::{Deserialize, Serialize};

use crate as surrealdb_types;
use crate::sql::{SqlFormat, ToSql};
use crate::{Kind, Range, RecordIdKey, SurrealValue, Value, kind};

/// Represents a range of record identifier keys in SurrealDB
///
/// This type is used for range queries on record identifiers,
/// allowing queries like "find all records with IDs between X and Y".

#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RecordIdKeyRange {
	/// The lower bound of the range
	pub start: Bound<RecordIdKey>,
	/// The upper bound of the range
	pub end: Bound<RecordIdKey>,
}

impl SurrealValue for RecordIdKeyRange {
	fn kind_of() -> Kind {
		kind!(range)
	}

	fn is_value(value: &Value) -> bool {
		if let Value::Range(r) = value {
			(match &r.start {
				Bound::Unbounded => true,
				Bound::Included(x) => RecordIdKey::is_value(x),
				Bound::Excluded(x) => RecordIdKey::is_value(x),
			} && match &r.end {
				Bound::Unbounded => true,
				Bound::Included(x) => RecordIdKey::is_value(x),
				Bound::Excluded(x) => RecordIdKey::is_value(x),
			})
		} else {
			false
		}
	}

	fn into_value(self) -> Value {
		Value::Range(Box::new(Range {
			start: self.start.map(RecordIdKey::into_value),
			end: self.end.map(RecordIdKey::into_value),
		}))
	}

	fn from_value(value: Value) -> anyhow::Result<Self> {
		if let Value::Range(r) = value {
			Ok(RecordIdKeyRange {
				start: match r.start {
					Bound::Unbounded => Bound::Unbounded,
					Bound::Included(x) => {
						Bound::Included(RecordIdKey::from_value(x).map_err(|e| {
							anyhow::anyhow!("Failed to convert Bound value to record id key: {e}")
						})?)
					}
					Bound::Excluded(x) => {
						Bound::Excluded(RecordIdKey::from_value(x).map_err(|e| {
							anyhow::anyhow!("Failed to convert Bound value to record id key: {e}")
						})?)
					}
				},
				end: match r.end {
					Bound::Unbounded => Bound::Unbounded,
					Bound::Included(x) => {
						Bound::Included(RecordIdKey::from_value(x).map_err(|e| {
							anyhow::anyhow!("Failed to convert Bound value to record id key: {e}")
						})?)
					}
					Bound::Excluded(x) => {
						Bound::Excluded(RecordIdKey::from_value(x).map_err(|e| {
							anyhow::anyhow!("Failed to convert Bound value to record id key: {e}")
						})?)
					}
				},
			})
		} else {
			Err(anyhow::anyhow!("Failed to convert to RecordIdKeyRange"))
		}
	}
}

impl RecordIdKeyRange {
	/// Returns the start bound of the range.
	pub fn start(&self) -> Bound<&RecordIdKey> {
		self.start.as_ref()
	}

	/// Returns the upper bound of the range.
	pub fn end(&self) -> Bound<&RecordIdKey> {
		self.end.as_ref()
	}

	/// Converts this range into the inner bounds.
	pub fn into_inner(self) -> (Bound<RecordIdKey>, Bound<RecordIdKey>) {
		(self.start, self.end)
	}

	/// Converts this range into a `Range` value.
	pub fn into_value_range(self) -> Range {
		Range {
			start: self.start.map(RecordIdKey::into_value),
			end: self.end.map(RecordIdKey::into_value),
		}
	}

	/// Converts a `Range` value into a `RecordIdKeyRange`.
	pub fn from_value_range(range: Range) -> anyhow::Result<Self> {
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

		Ok(RecordIdKeyRange {
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

impl ToSql for RecordIdKeyRange {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		match &self.start {
			Bound::Unbounded => {}
			Bound::Included(v) => {
				v.fmt_sql(f, fmt);
			}
			Bound::Excluded(v) => {
				f.push('>');
				v.fmt_sql(f, fmt)
			}
		};

		f.push_str("..");

		match &self.end {
			Bound::Unbounded => {}
			Bound::Included(v) => {
				f.push('=');
				v.fmt_sql(f, fmt);
			}
			Bound::Excluded(v) => {
				v.fmt_sql(f, fmt);
			}
		};
	}
}

impl From<RangeFull> for RecordIdKeyRange {
	fn from(_: RangeFull) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Unbounded,
		}
	}
}

impl<T: Into<RecordIdKey>> From<std::ops::Range<T>> for RecordIdKeyRange {
	fn from(range: std::ops::Range<T>) -> Self {
		Self {
			start: Bound::Included(range.start.into()),
			end: Bound::Excluded(range.end.into()),
		}
	}
}

impl<T: Into<RecordIdKey>> From<std::ops::RangeInclusive<T>> for RecordIdKeyRange {
	fn from(range: std::ops::RangeInclusive<T>) -> Self {
		let (start, end) = range.into_inner();
		Self {
			start: Bound::Included(start.into()),
			end: Bound::Included(end.into()),
		}
	}
}

impl<T: Into<RecordIdKey>> From<RangeTo<T>> for RecordIdKeyRange {
	fn from(range: RangeTo<T>) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Excluded(range.end.into()),
		}
	}
}

impl<T: Into<RecordIdKey>> From<RangeToInclusive<T>> for RecordIdKeyRange {
	fn from(range: RangeToInclusive<T>) -> Self {
		Self {
			start: Bound::Unbounded,
			end: Bound::Included(range.end.into()),
		}
	}
}

impl<T: Into<RecordIdKey>> From<RangeFrom<T>> for RecordIdKeyRange {
	fn from(range: RangeFrom<T>) -> Self {
		Self {
			start: Bound::Included(range.start.into()),
			end: Bound::Unbounded,
		}
	}
}

impl<T: Into<RecordIdKey>> From<(Bound<T>, Bound<T>)> for RecordIdKeyRange {
	fn from((start, end): (Bound<T>, Bound<T>)) -> Self {
		Self {
			start: match start {
				Bound::Included(x) => Bound::Included(x.into()),
				Bound::Excluded(x) => Bound::Excluded(x.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
			end: match end {
				Bound::Included(x) => Bound::Included(x.into()),
				Bound::Excluded(x) => Bound::Excluded(x.into()),
				Bound::Unbounded => Bound::Unbounded,
			},
		}
	}
}
