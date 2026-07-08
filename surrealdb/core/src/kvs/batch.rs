use crate::key::KeyRange;

/// A batch scan result returned from the [`Transaction::batch`] or
/// [`Transactor::batch`] functions.
#[derive(Debug)]
pub struct Batch<T> {
	pub next: Option<KeyRange<'static>>,
	pub result: Vec<T>,
}

impl<T> Batch<T> {
	/// Create a new batch scan result.
	pub fn new(next: Option<KeyRange<'static>>, result: Vec<T>) -> Self {
		Self {
			next,
			result,
		}
	}
}
