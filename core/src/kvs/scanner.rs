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

pub(super) enum Scanner<'a> {
	// The initial state of this scanner
	Begin {
		/// The store which started this range scan
		store: &'a Transaction,
		/// The number of keys to fetch at once
		batch: u32,
		// The key range for this range scan
		range: Range<Key>,
	},
	// The state when a future has completed
	Ready {
		/// The store which started this range scan
		store: &'a Transaction,
		/// The number of keys to fetch at once
		batch: u32,
		// The key range for this range scan
		range: Range<Key>,
		// The results from the last range scan
		results: VecDeque<(Key, Val)>,
	},
	// The state for when a future is being polled
	Pending {
		/// The store which started this range scan
		store: &'a Transaction,
		/// The number of keys to fetch at once
		batch: u32,
		// The key range for this range scan
		range: Range<Key>,
		// The currently awaiting range scan future
		future: Pin<Box<dyn Future<Output = Result<Vec<(Key, Val)>, Error>> + 'a>>,
	},
	// This scanner is complete
	Complete,
	// Used internally
	Internal,
}

impl<'a> Stream for Scanner<'a> {
	type Item = Result<(Key, Val), Error>;
	fn poll_next(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<(Key, Val), Error>>> {
		// Take ownership of the pointed
		let this = std::mem::replace(&mut *self, Self::Internal);
		// Check the current scanner state
		match this {
			// The initial state of this scanner
			Self::Begin {
				store,
				batch,
				range,
			} => {
				// Set the max number of results to fetch
				let num = std::cmp::min(1000, batch);
				// Set the next state of the scanner
				self.set(Self::Pending {
					store,
					batch,
					range: range.clone(),
					future: Box::pin(store.scan(range, num)),
				});
				// Mark this async stream as pending
				Poll::Pending
			}
			// The future has finished and we have some results
			Self::Ready {
				store,
				batch,
				mut range,
				mut results,
			} => match results.pop_front() {
				// We still have results, so return a result
				Some(v) => {
					// Set the next state of the scanner
					self.set(Self::Ready {
						store,
						batch,
						range,
						results,
					});
					// Return the first result
					Poll::Ready(Some(Ok(v)))
				}
				// No more results so let's fetch some more
				None => {
					range.end.push(0x00);
					// Set the max number of results to fetch
					let num = std::cmp::min(1000, batch);
					// Set the next state of the scanner
					self.set(Self::Pending {
						store,
						batch,
						range: range.clone(),
						future: Box::pin(store.scan(range, num)),
					});
					// Mark this async stream as pending
					Poll::Pending
				}
			},
			// We are waiting for a future to resolve
			Self::Pending {
				store,
				batch,
				range,
				mut future,
			} => match future.as_mut().poll(cx) {
				// The future has not yet completed
				Poll::Pending => {
					// Set the next state of the scanner
					self.set(Self::Pending {
						store,
						batch,
						range,
						future,
					});
					// Mark this async stream as pending
					Poll::Pending
				}
				// The future has now completed fully
				Poll::Ready(v) => match v {
					// There was an error with the range fetch
					Err(e) => {
						// Mark this scanner as complete
						self.set(Self::Complete);
						// Return the received error
						Poll::Ready(Some(Err(e)))
					}
					// The range was fetched successfully
					Ok(v) => match v.is_empty() {
						// There are no more results to stream
						true => {
							// Mark this scanner as complete
							self.set(Self::Complete);
							// Mark this stream as complete
							Poll::Ready(None)
						}
						// There are results which need streaming
						false => {
							// Store the fetched range results
							let mut results = VecDeque::from(v);
							// Remove the first result to return
							let item = results.pop_front().unwrap();
							// Set the next state of the scanner
							self.set(Self::Ready {
								store,
								batch,
								range,
								results,
							});
							// Return the first result
							Poll::Ready(Some(Ok(item)))
						}
					},
				},
			},
			// This range scan is completed
			Self::Complete => {
				// Mark this scanner as complete
				self.set(Self::Complete);
				// Mark this stream as complete
				Poll::Ready(None)
			}
			// This state should never occur
			Self::Internal => unreachable!(),
		}
	}
}
