use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::Result;
use futures::stream::Stream;
use futures::{Future, FutureExt};

use super::tx::Transaction;
use super::{Key, Val};
use crate::err::Error;
use crate::idx::planner::ScanDirection;

#[cfg(not(target_family = "wasm"))]
type FutureResult<'a, I> = Pin<Box<dyn Future<Output = Result<Vec<I>>> + 'a + Send>>;

#[cfg(target_family = "wasm")]
type FutureResult<'a, I> = Pin<Box<dyn Future<Output = Result<Vec<I>>> + 'a>>;

pub(super) struct Scanner<'a, I> {
	/// The store which started this range scan
	store: &'a Transaction,
	/// The number of keys to fetch at once
	batch: u32,
	/// The key range for this range scan
	range: Range<Key>,
	/// The results from the last range scan
	results: VecDeque<I>,
	/// The currently running future to be polled
	future: Option<FutureResult<'a, I>>,
	/// Whether this stream should try to fetch more
	exhausted: bool,
	/// Version as timestamp, 0 means latest.
	version: Option<u64>,
	/// An optional maximum number of keys to scan
	limit: Option<usize>,
	/// The scan direction
	sc: ScanDirection,
}

impl<'a, I> Scanner<'a, I> {
	pub fn new(
		store: &'a Transaction,
		batch: u32,
		range: Range<Key>,
		version: Option<u64>,
		limit: Option<usize>,
		sc: ScanDirection,
	) -> Self {
		Scanner {
			store,
			batch,
			range,
			future: None,
			results: VecDeque::new(),
			exhausted: false,
			version,
			limit,
			sc,
		}
	}

	fn next_poll<S, K>(&mut self, cx: &mut Context, scan: S, key: K) -> Poll<Option<Result<I>>>
	where
		S: Fn(Range<Key>, u32) -> FutureResult<'a, I>,
		K: Fn(&I) -> &Key,
	{
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
			// Clone the range to use when scanning
			let range = self.range.clone();
			// Compute the batch size. It can't be more what is left to collect
			let batch = self
				.limit
				.map(|l| (self.batch as usize).min(l) as u32)
				.unwrap_or_else(|| self.batch);
			// Prepare a future to scan for results
			self.future = Some(scan(range, batch));
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
						// There are results that need streaming
						false => {
							if let Some(l) = &mut self.limit {
								*l -= v.len();
							}
							// We fetched the last elements in the range
							if v.len() < self.batch as usize {
								self.exhausted = true;
							}
							// Get the last element of the results
							let last = v.last().ok_or_else(|| {
								Error::unreachable(
									"Expected the last key-value pair to not be none",
								)
							})?;
							match self.sc {
								ScanDirection::Forward => {
									// Start the next scan from the last result
									self.range.start.clone_from(key(last));
									// Ensure we don't see the last result again
									self.range.start.push(0xff);
								}
								#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
								ScanDirection::Backward => {
									// Start the next scan from the last result
									self.range.end.clone_from(key(last));
								}
							};
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

impl Stream for Scanner<'_, (Key, Val)> {
	type Item = Result<(Key, Val)>;
	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<(Key, Val)>>> {
		let (store, version) = (self.store, self.version);
		match self.sc {
			ScanDirection::Forward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.scan(range, batch, version)),
				|v| &v.0,
			),
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			ScanDirection::Backward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.scanr(range, batch, version)),
				|v| &v.0,
			),
		}
	}
}

impl Stream for Scanner<'_, Key> {
	type Item = Result<Key>;
	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Key>>> {
		let (store, version) = (self.store, self.version);
		match self.sc {
			ScanDirection::Forward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.keys(range, batch, version)),
				|v| v,
			),
			#[cfg(any(feature = "kv-rocksdb", feature = "kv-tikv"))]
			ScanDirection::Backward => self.next_poll(
				cx,
				move |range, batch| Box::pin(store.keysr(range, batch, version)),
				|v| v,
			),
		}
	}
}
