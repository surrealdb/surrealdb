use super::tx::Transaction;
use super::Key;
use super::Val;
use crate::cnf::MAX_STREAM_BATCH_SIZE;
use crate::err::Error;
use futures::stream::Stream;
use futures::Future;
use futures::FutureExt;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

pub(super) struct Scanner<'a, I> {
	/// The store which started this range scan
	store: &'a Transaction,
	/// The number of keys to fetch at once
	batch: u32,
	// The key range for this range scan
	range: Range<Key>,
	// The results from the last range scan
	results: VecDeque<I>,
	#[allow(clippy::type_complexity)]
	/// The currently running future to be polled
	#[cfg(not(target_arch = "wasm32"))]
	future: Option<Pin<Box<dyn Future<Output = Result<Vec<I>, Error>> + 'a + Send>>>,
	#[cfg(target_arch = "wasm32")]
	future: Option<Pin<Box<dyn Future<Output = Result<Vec<I>, Error>> + 'a>>>,
	/// Whether this stream should try to fetch more
	exhausted: bool,
	/// Version as timestamp, 0 means latest.
	version: Option<u64>,
}

impl<'a, I> Scanner<'a, I> {
	pub fn new(
		store: &'a Transaction,
		batch: u32,
		range: Range<Key>,
		version: Option<u64>,
	) -> Self {
		Scanner {
			store,
			batch,
			range,
			future: None,
			results: VecDeque::new(),
			exhausted: false,
			version,
		}
	}
}

impl Stream for Scanner<'_, (Key, Val)> {
	type Item = Result<(Key, Val), Error>;
	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut Context,
	) -> Poll<Option<Result<(Key, Val), Error>>> {
		// If we have results, return the first one
		if let Some(v) = self.results.pop_front() {
			return Poll::Ready(Some(Ok(v)));
		}
		// If we won't fetch more results then exit
		if self.exhausted {
			return Poll::Ready(None);
		}
		// Check if there is no pending future task
		if self.future.is_none() {
			// Set the max number of results to fetch
			let num = std::cmp::min(*MAX_STREAM_BATCH_SIZE, self.batch);
			// Clone the range to use when scanning
			let range = self.range.clone();
			// Prepare a future to scan for results
			self.future = Some(Box::pin(self.store.scan(range, num, self.version)));
		}
		// Try to resolve the future
		match self.future.as_mut().unwrap().poll_unpin(cx) {
			// The future has now completed fully
			Poll::Ready(result) => {
				// Drop the completed asynchronous future
				self.future = None;
				// Check the result of the finished future
				match result {
					// The range was fetched successfully
					Ok(v) => match v.is_empty() {
						// There are no more results to stream
						true => {
							// Mark this stream as complete
							Poll::Ready(None)
						}
						// There are results which need streaming
						false => {
							// We fetched the last elements in the range
							if v.len() < self.batch as usize {
								self.exhausted = true;
							}
							// Get the last element of the results
							let last = v.last().ok_or_else(|| {
								fail!("Expected the last key-value pair to not be none")
							})?;
							// Start the next scan from the last result
							self.range.start.clone_from(&last.0);
							// Ensure we don't see the last result again
							self.range.start.push(0xff);
							// Store the fetched range results
							self.results.extend(v);
							// Remove the first result to return
							let item = self.results.pop_front().unwrap();
							// Return the first result
							Poll::Ready(Some(Ok(item)))
						}
					},
					// Return the received error
					Err(error) => Poll::Ready(Some(Err(error))),
				}
			}
			// The future has not yet completed
			Poll::Pending => Poll::Pending,
		}
	}
}

impl Stream for Scanner<'_, Key> {
	type Item = Result<Key, Error>;
	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Key, Error>>> {
		// If we have results, return the first one
		if let Some(v) = self.results.pop_front() {
			return Poll::Ready(Some(Ok(v)));
		}
		// If we won't fetch more results then exit
		if self.exhausted {
			return Poll::Ready(None);
		}
		// Check if there is no pending future task
		if self.future.is_none() {
			// Set the max number of results to fetch
			let num = std::cmp::min(*MAX_STREAM_BATCH_SIZE, self.batch);
			// Clone the range to use when scanning
			let range = self.range.clone();
			// Prepare a future to scan for results
			self.future = Some(Box::pin(self.store.keys(range, num, self.version)));
		}
		// Try to resolve the future
		match self.future.as_mut().unwrap().poll_unpin(cx) {
			// The future has now completed fully
			Poll::Ready(result) => {
				// Drop the completed asynchronous future
				self.future = None;
				// Check the result of the finished future
				match result {
					// The range was fetched successfully
					Ok(v) => match v.is_empty() {
						// There are no more results to stream
						true => {
							// Mark this stream as complete
							Poll::Ready(None)
						}
						// There are results which need streaming
						false => {
							// We fetched the last elements in the range
							if v.len() < self.batch as usize {
								self.exhausted = true;
							}
							// Get the last element of the results
							let last = v.last().ok_or_else(|| {
								fail!("Expected the last key-value pair to not be none")
							})?;
							// Start the next scan from the last result
							self.range.start.clone_from(last);
							// Ensure we don't see the last result again
							self.range.start.push(0xff);
							// Store the fetched range results
							self.results.extend(v);
							// Remove the first result to return
							let item = self.results.pop_front().unwrap();
							// Return the first result
							Poll::Ready(Some(Ok(item)))
						}
					},
					// Return the received error
					Err(error) => Poll::Ready(Some(Err(error))),
				}
			}
			// The future has not yet completed
			Poll::Pending => Poll::Pending,
		}
	}
}
