use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;
use futures::{Future, FutureExt};

use super::tr::Transactor;
use super::{Key, Result, Val};
use crate::cnf::MAX_BATCH_SIZE;

#[cfg(not(target_family = "wasm"))]
type FutureResult<'a, I> = Pin<Box<dyn Future<Output = Result<Vec<I>>> + 'a + Send>>;

#[cfg(target_family = "wasm")]
type FutureResult<'a, I> = Pin<Box<dyn Future<Output = Result<Vec<I>>> + 'a>>;

/// The direction of a scan.
pub enum Direction {
	Forward,
	Backward,
}

/// A scanner for fetching a tream of keys or key-value pairs from a range.
pub struct Scanner<'a, I> {
	/// The store which started this range scan
	store: &'a Transactor,
	/// The current adaptive batch size for fetching
	current_batch_size: usize,
	/// The key range for this range scan
	range: Range<Key>,
	/// The results from the last range scan
	results: VecDeque<I>,
	/// The currently running future to be polled
	future: Option<FutureResult<'a, I>>,
	/// Whether prefetching is enabled for this scanner
	prefetch: bool,
	/// The prefetch future for the next batch
	prefetch_future: Option<FutureResult<'a, I>>,
	/// The prefetched results ready to be swapped in
	prefetch_results: VecDeque<I>,
	/// The initial count of results to track 50% threshold for prefetching
	active_result_count: usize,
	/// Whether this stream should try to fetch more
	exhausted: bool,
	/// Version as timestamp, 0 means latest.
	version: Option<u64>,
	/// An optional maximum number of keys to scan
	limit: Option<usize>,
	/// The scan direction
	dir: Direction,
}

impl<'a, I> Scanner<'a, I> {
	pub fn new(
		store: &'a Transactor,
		range: Range<Key>,
		version: Option<u64>,
		limit: Option<usize>,
		dir: Direction,
		prefetch: bool,
	) -> Self {
		// Check if the range is valid (start must be less than end)
		let exhausted = range.start >= range.end;
		// Initialize the scanner
		Scanner {
			store,
			current_batch_size: 100,
			range,
			future: None,
			prefetch_future: None,
			results: VecDeque::with_capacity(100),
			prefetch_results: VecDeque::with_capacity(100),
			active_result_count: 0,
			prefetch,
			exhausted,
			version,
			limit,
			dir,
		}
	}

	fn next_poll<S, K>(&mut self, cx: &mut Context, scan: S, key: K) -> Poll<Option<Result<I>>>
	where
		S: Fn(Range<Key>, usize) -> FutureResult<'a, I>,
		K: Fn(&I) -> &Key,
	{
		// Check if we have prefetched results ready to use when main buffer is empty
		if self.results.is_empty() && !self.prefetch_results.is_empty() {
			// Swap the prefetch buffer into the main buffer to avoid copying
			std::mem::swap(&mut self.results, &mut self.prefetch_results);
			// Update the active result count the this prefetched results
			self.active_result_count = self.results.len();
		}

		// If we have results in the main buffer, return the first one
		if let Some(v) = self.results.pop_front() {
			// Check if we should start prefetching the next batch (50% threshold)
			let should_prefetch = self.prefetch
				&& !self.exhausted
				&& self.prefetch_future.is_none()
				&& self.active_result_count > 0
				&& self.results.len() <= self.active_result_count / 2
				&& self.limit.is_none_or(|l| l > 0);
			// Perform a prefetch if the conditions are met
			if should_prefetch {
				// Compute the next batch size (double it, capped at MAX_BATCH_SIZE and limit)
				let batch_size = match self.limit {
					Some(l) => (self.current_batch_size * 2).min(*MAX_BATCH_SIZE).min(l),
					None => (self.current_batch_size * 2).min(*MAX_BATCH_SIZE),
				};
				// Start prefetching the next batch
				self.prefetch_future = Some(scan(self.range.clone(), batch_size));
			}
			// Return the first result
			return Poll::Ready(Some(Ok(v)));
		}

		// If we won't fetch more results then exit
		if self.exhausted {
			return Poll::Ready(None);
		}

		// Check if we have a prefetch future that's ready
		if let Some(future) = &mut self.prefetch_future {
			match future.poll_unpin(cx) {
				Poll::Ready(result) => {
					// Drop the completed prefetch future
					self.prefetch_future = None;
					// Process the prefetch result
					match result {
						Ok(v) if !v.is_empty() => {
							// Update limit
							if let Some(l) = &mut self.limit {
								*l = l.saturating_sub(v.len());
								if *l == 0 {
									self.exhausted = true;
								}
							}
							// Check if we got less than requested (end of range)
							if v.len() < self.current_batch_size {
								self.exhausted = true;
							}
							// Update the range for the next batch
							let last = v.last().expect("last key-value pair to not be none");
							// Update the range for the next batch
							match self.dir {
								Direction::Forward => {
									self.range.start.clone_from(key(last));
									self.range.start.push(0xff);
								}
								Direction::Backward => {
									self.range.end.clone_from(key(last));
								}
							}
							// Store the prefetched results
							self.prefetch_results.extend(v);
							// Store the latest batch size
							self.current_batch_size =
								(self.current_batch_size * 2).min(*MAX_BATCH_SIZE);
							// Swap prefetch into main and return first item
							std::mem::swap(&mut self.results, &mut self.prefetch_results);
							// Remove the first result to return
							let item = self.results.pop_front().expect("results should have items");
							// Update the active result count
							self.active_result_count = self.results.len();
							// Return the first result
							return Poll::Ready(Some(Ok(item)));
						}
						Ok(_) => {
							// Empty result means we've reached the end
							self.exhausted = true;
							// Return that there are no more results
							return Poll::Ready(None);
						}
						Err(error) => return Poll::Ready(Some(Err(error))),
					}
				}
				Poll::Pending => {
					// Prefetch is still running, continue to check main future
				}
			}
		}

		// Check if there is no pending main future task
		// to avoid fetching the same range twice
		if self.future.is_none() && self.prefetch_future.is_none() {
			// Compute the next batch size (double it, capped at MAX_BATCH_SIZE and limit)
			let batch_size = match self.limit {
				Some(l) => (self.current_batch_size * 2).min(*MAX_BATCH_SIZE).min(l),
				None => (self.current_batch_size * 2).min(*MAX_BATCH_SIZE),
			};
			// Prepare a future to scan for results
			self.future = Some(scan(self.range.clone(), batch_size));
		}

		// If we have no main future (because prefetch is pending), wait for prefetch
		if self.future.is_none() {
			return Poll::Pending;
		}

		// Try to resolve the main future
		match self.future.as_mut().expect("future should be set").poll_unpin(cx) {
			Poll::Ready(result) => {
				// Drop the completed asynchronous future
				self.future = None;
				// Check the result of the finished future
				match result {
					Ok(v) if !v.is_empty() => {
						// Update limit
						if let Some(l) = &mut self.limit {
							*l = l.saturating_sub(v.len());
							if *l == 0 {
								self.exhausted = true;
							}
						}
						// Check if we fetched less than requested
						if v.len() < self.current_batch_size {
							self.exhausted = true;
						}
						// Get the last element to update range
						let last = v.last().expect("last key-value pair to not be none");
						// Update the range for the next batch
						match self.dir {
							Direction::Forward => {
								// Start the next scan from the last result
								self.range.start.clone_from(key(last));
								// Ensure we don't see the last result again
								self.range.start.push(0xff);
							}
							Direction::Backward => {
								// Start the next scan from the last result
								self.range.end.clone_from(key(last));
							}
						}
						// Store the fetched results
						self.results.extend(v);
						// Store the latest batch size
						self.current_batch_size =
							(self.current_batch_size * 2).min(*MAX_BATCH_SIZE);
						// Remove the first result to return
						let item = self.results.pop_front().expect("results should have items");
						// Update the active result count
						self.active_result_count = self.results.len();
						// Return the first result
						Poll::Ready(Some(Ok(item)))
					}
					Ok(_) => {
						// Empty result, stream is complete
						self.exhausted = true;
						// Return that the stream is complete
						Poll::Ready(None)
					}
					Err(error) => Poll::Ready(Some(Err(error))),
				}
			}
			// The main future is still running, continue to check it
			Poll::Pending => Poll::Pending,
		}
	}
}

impl Stream for Scanner<'_, Key> {
	type Item = Result<Key>;
	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Key>>> {
		let (store, version) = (self.store, self.version);
		match self.dir {
			Direction::Forward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.keys(range, batch, version)),
				|v| v,
			),
			Direction::Backward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.keysr(range, batch, version)),
				|v| v,
			),
		}
	}
}

impl Stream for Scanner<'_, (Key, Val)> {
	type Item = Result<(Key, Val)>;
	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(Key, Val)>>> {
		let (store, version) = (self.store, self.version);
		match self.dir {
			Direction::Forward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.scan(range, batch, version)),
				|v| &v.0,
			),
			Direction::Backward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.scanr(range, batch, version)),
				|v| &v.0,
			),
		}
	}
}
