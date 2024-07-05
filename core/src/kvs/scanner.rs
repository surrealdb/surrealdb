use super::tx::Transaction;
use super::Key;
use super::Val;
use crate::err::Error;
use futures::stream::Stream;
use futures::Future;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) struct Scanner<'a> {
	/// The store which started this range scan
	store: &'a Transaction,
	/// The number of keys to fetch at once
	batch: u32,
	// The key range for this range scan
	range: Range<Key>,
	// The results from the last range scan
	results: VecDeque<(Key, Val)>,
}

impl<'a> Scanner<'a> {
	pub fn new(store: &'a Transaction, batch: u32, range: Range<Key>) -> Self {
		Scanner {
			store,
			batch,
			range,
			results: VecDeque::new(),
		}
	}
}

impl<'a> Stream for Scanner<'a> {
	type Item = Result<(Key, Val), Error>;
	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<(Key, Val), Error>>> {
		// If we have results, return the first one
		if let Some(v) = self.results.pop_front() {
			return Poll::Ready(Some(Ok(v)));
		}
		// Set the max number of results to fetch
		let num = std::cmp::min(1000, self.batch);
		// Clone the range to use when scanning
		let range = self.range.clone();
		// Prepare a future to scan for results
		let mut scanner = Box::pin(self.store.scan(range, num));
		// Try to resolve the future
		match scanner.as_mut().poll(cx) {
			// The future has now completed fully
			Poll::Ready(result) => match result {
				// The range was fetched successfully
				Ok(v) => match v.is_empty() {
					// There are no more results to stream
					true => {
						// Mark this stream as complete
						Poll::Ready(None)
					}
					// There are results which need streaming
					false => {
						// Release the mutable scanner reference
						drop(scanner);
						// Get the last element of the results
						let last = v.last().unwrap();
						// Start the next scan from the last result
						self.range.start = last.0.clone();
						// Ensure we don't see the last result again
						self.range.start.push(0xff);
						// Store the fetched range results
						self.results.extend(v.into_iter());
						// Remove the first result to return
						let item = self.results.pop_front().unwrap();
						// Return the first result
						Poll::Ready(Some(Ok(item)))
					}
				},
				// Return the received error
				Err(error) => Poll::Ready(Some(Err(error))),
			},
			// The future has not yet completed
			Poll::Pending => Poll::Pending,
		}
	}
}
