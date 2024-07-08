use super::Key;
use super::Val;
use std::ops::Range;

/// A batch scan result returned from the [`Transaction::batch`] or [`Transactor::batch`] functions.
#[derive(Debug)]
pub struct Batch {
	pub next: Option<Range<Key>>,
	pub values: Vec<(Key, Val)>,
}
