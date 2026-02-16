use std::ops::Range;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::stream::Stream;
use futures::{Future, FutureExt};

use super::api::ScanLimit;
use super::tr::Transactor;
use super::{Key, Result, Val};
use crate::cnf::NORMAL_FETCH_SIZE;

#[cfg(not(target_family = "wasm"))]
type FutureResult<'a, I> = Pin<Box<dyn Future<Output = Result<Vec<I>>> + 'a + Send>>;

#[cfg(target_family = "wasm")]
type FutureResult<'a, I> = Pin<Box<dyn Future<Output = Result<Vec<I>>> + 'a>>;

/// The direction of a scan.
#[derive(Clone, Copy)]
pub enum Direction {
	Forward,
	Backward,
}

/// A batch scanner that streams batches of keys or key-value pairs from a range.
///
/// The scanner fetches data in batches with configurable behavior:
/// - Initial batch: fetches up to NORMAL_FETCH_SIZE items (default 500)
/// - Subsequent batches: fetches up to 4 MiB of data
/// - Prefetching: optionally prefetches the next batch concurrently
///
/// Use the builder methods to configure the scanner before using it as a stream:
/// ```ignore
/// let scanner = Scanner::<Key>::new(&tx, range, limit, Direction::Forward)
///     .version(1234567890)
///     .initial_batch_size(ScanLimit::Count(50))
///     .subsequent_batch_size(ScanLimit::Bytes(8 * 1024 * 1024))
///     .prefetch(false);
/// ```
pub struct Scanner<'a, I> {
	/// The store which started this range scan
	store: &'a Transactor,
	/// The key range for this range scan
	range: Range<Key>,
	/// The currently running future to be polled
	future: Option<FutureResult<'a, I>>,
	/// A prefetched result ready to be returned
	prefetched: Option<Result<Vec<I>>>,
	/// Whether this is the first batch (uses count-based limit)
	first_batch: bool,
	/// Whether this stream should try to fetch more
	exhausted: bool,
	/// An optional maximum number of keys to scan
	limit: Option<usize>,
	/// The number of entries to skip (applied to first batch only)
	skip: u32,
	/// The scan direction
	dir: Direction,
	/// Version as timestamp, 0 means latest.
	version: Option<u64>,
	/// Whether to enable prefetching of the next batch
	enable_prefetch: bool,
	/// The initial batch size (default: NORMAL_FETCH_SIZE, typically 500 items)
	initial_batch_size: ScanLimit,
	/// The subsequent batch size (default: 16 MiB bytes)
	subsequent_batch_size: ScanLimit,
}

impl<'a, I> Scanner<'a, I> {
	/// Creates a new Scanner with default configuration.
	pub fn new(
		store: &'a Transactor,
		range: Range<Key>,
		limit: Option<usize>,
		dir: Direction,
	) -> Self {
		// Check if the range is exhausted
		let exhausted = range.start >= range.end;
		// Initialize the scanner with defaults.
		// The initial batch size uses NORMAL_FETCH_SIZE (default 500) to
		// avoid under-fetching on the first round-trip, which is especially
		// important for remote backends like TiKV where each scan is a
		// network call.
		Scanner {
			store,
			range,
			limit,
			dir,
			skip: 0,
			exhausted,
			future: None,
			prefetched: None,
			first_batch: true,
			version: None,
			enable_prefetch: false,
			initial_batch_size: ScanLimit::Count(*NORMAL_FETCH_SIZE),
			subsequent_batch_size: ScanLimit::Bytes(4 * 1024 * 1024),
		}
	}

	/// Set the number of entries to skip (applied to first batch only)
	pub fn skip(mut self, skip: u32) -> Self {
		self.skip = skip;
		self
	}

	/// Set the version timestamp for the scan.
	///
	/// When set, the scanner will read data as it existed at the specified version.
	pub fn version(mut self, version: u64) -> Self {
		self.version = Some(version);
		self
	}

	/// Enable or disable background prefetching.
	///
	/// When enabled, the scanner will start fetching the next batch while the current
	/// batch is being processed, improving throughput at the cost of additional resources.
	/// Default: false
	pub fn prefetch(mut self, enabled: bool) -> Self {
		self.enable_prefetch = enabled;
		self
	}

	/// Set the initial batch size for the first batch.
	///
	/// The first batch fetched will contain up to this many items or bytes of data.
	/// Default: NORMAL_FETCH_SIZE (500 items)
	pub fn initial_batch_size(mut self, size: ScanLimit) -> Self {
		self.initial_batch_size = size;
		self
	}

	/// Set the subsequent batch size for subsequent batches.
	///
	/// After the first batch, subsequent batches will fetch up to this many items or bytes of data.
	/// Default: 4 MiB
	pub fn subsequent_batch_size(mut self, size: ScanLimit) -> Self {
		self.subsequent_batch_size = size;
		self
	}

	/// Updates the range for the next batch based on the last key fetched.
	#[inline]
	fn update_range(&mut self, last_key: &Key) {
		match self.dir {
			Direction::Forward => {
				self.range.start.clone_from(last_key);
				self.range.start.push(0xff);
			}
			Direction::Backward => {
				self.range.end.clone_from(last_key);
			}
		}
	}

	/// Calculate the scan limit for the next batch.
	#[inline]
	fn next_scan_limit(&self) -> ScanLimit {
		// Check if this is the first batch
		let batch_size = if self.first_batch {
			self.initial_batch_size
		} else {
			self.subsequent_batch_size
		};
		// Apply the limit to the batch size
		match batch_size {
			ScanLimit::Count(c) => match self.limit {
				Some(l) => ScanLimit::Count(c.min(l as u32)),
				None => ScanLimit::Count(c),
			},
			ScanLimit::Bytes(b) => match self.limit {
				Some(l) => ScanLimit::BytesOrCount(b, l as u32),
				None => ScanLimit::Bytes(b),
			},
			ScanLimit::BytesOrCount(b, c) => match self.limit {
				Some(l) => ScanLimit::BytesOrCount(b, c.min(l as u32)),
				None => ScanLimit::BytesOrCount(b, c),
			},
		}
	}

	#[inline]
	fn start_prefetch<S>(&mut self, cx: &mut Context, scan: S)
	where
		S: Fn(Range<Key>, ScanLimit, u32) -> FutureResult<'a, I>,
	{
		if self.enable_prefetch && !self.exhausted {
			// Calculate the limit for the next batch
			let limit = self.next_scan_limit();
			// Get the skip value for the first batch
			let skip = self.skip;
			// Setup the next range scan
			let mut future = scan(self.range.clone(), limit, skip);
			// Poll the future to kick off I/O
			match future.poll_unpin(cx) {
				Poll::Pending => {
					// I/O started, store for later
					self.future = Some(future);
				}
				Poll::Ready(result) => {
					// We received a result immediately
					self.prefetched = Some(result);
				}
			}
		}
	}

	/// Process a completed fetch result, updating internal state.
	/// Returns the batch if successful, or None if the stream is exhausted.
	fn process_result<K>(&mut self, result: Result<Vec<I>>, key: &K) -> Poll<Option<Result<Vec<I>>>>
	where
		K: Fn(&I) -> &Key,
	{
		match result {
			// There were some results returned
			Ok(batch) if !batch.is_empty() => {
				// Update limit
				if let Some(l) = &mut self.limit {
					*l = l.saturating_sub(batch.len());
					if *l == 0 {
						self.exhausted = true;
					}
				}
				// Fetch the limiter for the next batch
				let limiter = if self.first_batch {
					self.initial_batch_size
				} else {
					self.subsequent_batch_size
				};
				// Check if the batch is exhausted
				if let ScanLimit::Count(l) = limiter
					&& batch.len() < l as usize
				{
					self.exhausted = true;
				}
				// Get the last key to update range for the next batch
				let last = batch.last().expect("batch should not be empty");
				let last_key = key(last);
				// Update the range for the next batch
				self.update_range(last_key);
				// Mark that we've fetched the first batch
				self.first_batch = false;
				// Reset skip after the first batch
				self.skip = 0;
				// Return the batch
				Poll::Ready(Some(Ok(batch)))
			}
			// There were no results returned
			Ok(_) => {
				// Empty result means we've reached the end
				self.exhausted = true;
				// Return no more results
				Poll::Ready(None)
			}
			// We received an error
			Err(error) => {
				// An error means we've reached the end
				self.exhausted = true;
				// Return the error
				Poll::Ready(Some(Err(error)))
			}
		}
	}

	fn next_poll<S, K>(&mut self, cx: &mut Context, scan: S, key: K) -> Poll<Option<Result<Vec<I>>>>
	where
		S: Fn(Range<Key>, ScanLimit, u32) -> FutureResult<'a, I>,
		K: Fn(&I) -> &Key,
	{
		// Return early if exhausted
		if self.exhausted {
			return Poll::Ready(None);
		}
		// Check if we have a prefetched result ready
		if let Some(result) = self.prefetched.take() {
			// Process the last fetches result batch
			let poll = self.process_result(result, &key);
			// If prefetch is enabled, start the next scan
			self.start_prefetch(cx, &scan);
			// Return the result
			return poll;
		}
		// Calculate the limit for this fetch
		let limit = self.next_scan_limit();
		// Get the skip value (only applies to first batch)
		let skip = self.skip;
		// Fetch or start a new fetch if none is pending
		let future = self.future.get_or_insert_with(|| scan(self.range.clone(), limit, skip));
		// Try to resolve the main future
		match future.poll_unpin(cx) {
			// The future is pending
			Poll::Pending => Poll::Pending,
			// The future is ready
			Poll::Ready(result) => {
				// Drop the completed future
				self.future = None;
				// Process the last fetches result batch
				let poll = self.process_result(result, &key);
				// If prefetch is enabled, start the next scan
				self.start_prefetch(cx, &scan);
				// Return the result
				poll
			}
		}
	}
}

impl Stream for Scanner<'_, Key> {
	type Item = Result<Vec<Key>>;
	fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Result<Vec<Key>>>> {
		let (store, version) = (self.store, self.version);
		match self.dir {
			Direction::Forward => self.next_poll(
				cx,
				move |range, limit, skip| Box::pin(store.keys(range, limit, skip, version)),
				|v| v,
			),
			Direction::Backward => self.next_poll(
				cx,
				move |range, limit, skip| Box::pin(store.keysr(range, limit, skip, version)),
				|v| v,
			),
		}
	}
}

impl Stream for Scanner<'_, (Key, Val)> {
	type Item = Result<Vec<(Key, Val)>>;
	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut Context,
	) -> Poll<Option<Result<Vec<(Key, Val)>>>> {
		let (store, version) = (self.store, self.version);
		match self.dir {
			Direction::Forward => self.next_poll(
				cx,
				move |range, limit, skip| Box::pin(store.scan(range, limit, skip, version)),
				|v| &v.0,
			),
			Direction::Backward => self.next_poll(
				cx,
				move |range, limit, skip| Box::pin(store.scanr(range, limit, skip, version)),
				|v| &v.0,
			),
		}
	}
}
