use std::ops::Range;

use super::Key;

/// A batch scan result returned from the [`Transaction::batch`] or
/// [`Transactor::batch`] functions.
#[derive(Debug)]
pub struct Batch<T> {
	pub next: Option<Range<Key>>,
	pub result: Vec<T>,
}

impl<T> Batch<T> {
	/// Create a new batch scan result.
	pub fn new(next: Option<Range<Key>>, result: Vec<T>) -> Self {
		Self {
			next,
			result,
		}
	}
}
