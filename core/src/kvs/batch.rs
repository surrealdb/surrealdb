use super::Key;
use super::Val;
use super::Version;
use std::ops::Range;

/// A batch scan result returned from the [`Transaction::batch`] or [`Transactor::batch`] functions.
#[derive(Debug)]
pub struct Batch {
	pub next: Option<Range<Key>>,
	pub values: Vec<(Key, Val)>,
	pub versioned_values: Vec<(Key, Val, Version, bool)>,
}

impl Batch {
	/// Create a new batch scan result.
	pub fn new(next: Option<Range<Key>>, values: Vec<(Key, Val)>) -> Self {
		Self {
			next,
			values,
			versioned_values: vec![],
		}
	}

	/// Create a new batch scan result with versioned values.
	pub fn new_versioned(
		next: Option<Range<Key>>,
		versioned_values: Vec<(Key, Val, Version, bool)>,
	) -> Self {
		Self {
			next,
			values: vec![],
			versioned_values,
		}
	}
}
