use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use rocksdb::{OptimisticTransactionDB, Options};
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::kvs::err::{Error, Result};

/// Shared state between producers (transactions) and consumer (batcher)
struct SharedState {
	/// Buffer of pending commit requests
	buffer: Mutex<Vec<CommitRequest>>,
	/// Condition variable to wake the batcher thread
	condvar: Condvar,
	/// Flag indicating the coordinator is shutting down
	shutdown: Mutex<bool>,
}

/// Coordinator for batching transaction commits together
pub struct CommitCoordinator {
	/// Shared state for communication with the batcher
	shared: Arc<SharedState>,
}

/// A request to commit a transaction
struct CommitRequest {
	/// The transaction to commit
	txn: rocksdb::Transaction<'static, OptimisticTransactionDB>,
	/// The channel to send the result of the commit
	channel: oneshot::Sender<Result<()>>,
}

/// The background batcher that processes commit requests
struct CommitBatcher {
	/// Shared state for receiving commit requests
	shared: Arc<SharedState>,
	/// Reference to the database for explicit WAL flushing
	db: Pin<Arc<OptimisticTransactionDB>>,
	/// Maximum time to wait for collecting a batch
	timeout: Duration,
	/// Maximum number of transactions per batch
	batch_size: usize,
	/// Minimum number of concurrent transactions before using timeout
	min_siblings: usize,
}

impl Drop for CommitCoordinator {
	fn drop(&mut self) {
		self.shutdown();
	}
}

impl CommitCoordinator {
	/// Create a new commit coordinator and spawn the background batcher task
	pub fn new(
		opts: &mut Options,
		db: Pin<Arc<OptimisticTransactionDB>>,
		timeout: u64,
		batch_size: usize,
		min_siblings: usize,
	) -> Self {
		// Enable manual WAL flushing
		opts.set_manual_wal_flush(true);
		// Create shared state with pre-allocated buffer
		let shared = Arc::new(SharedState {
			buffer: Mutex::new(Vec::with_capacity(batch_size)),
			condvar: Condvar::new(),
			shutdown: Mutex::new(false),
		});
		// Create a new commit batcher
		let batcher = CommitBatcher {
			shared: shared.clone(),
			db,
			batch_size,
			min_siblings,
			timeout: Duration::from_nanos(timeout),
		};
		// Spawn the background task
		tokio::spawn(async move {
			batcher.run().await;
		});
		// Return the commit coordinator
		Self {
			shared,
		}
	}

	/// Signal shutdown to the batcher thread
	fn shutdown(&self) {
		*self.shared.shutdown.lock() = true;
		self.shared.condvar.notify_all();
	}

	/// Submit a transaction for grouped commit
	pub async fn commit(
		&self,
		txn: rocksdb::Transaction<'static, OptimisticTransactionDB>,
	) -> Result<()> {
		// Create a new oneshot response channel
		let (tx, rx) = oneshot::channel();
		// Create a new commit request
		let request = CommitRequest {
			txn,
			channel: tx,
		};
		// Add to shared buffer and notify batcher
		{
			let mut buffer = self.shared.buffer.lock();
			// Check if shutting down
			if *self.shared.shutdown.lock() {
				return Err(Error::Transaction("commit coordinator is shutting down".into()));
			}
			buffer.push(request);
			// Notify the batcher that work is available
			self.shared.condvar.notify_one();
		}
		// Wait for the transaction to commit
		rx.await
			.map_err(|_| Error::Transaction("commit coordinator response channel closed".into()))?
	}
}

impl CommitBatcher {
	/// Run the background batcher loop
	async fn run(self) {
		// Pre-allocate batch vector once
		let mut batch = Vec::with_capacity(self.batch_size);
		// Loop continuously until shutdown
		loop {
			// Wait for work to be available
			{
				let mut buffer = self.shared.buffer.lock();
				// Wait for items or shutdown
				while buffer.is_empty() && !*self.shared.shutdown.lock() {
					self.shared.condvar.wait(&mut buffer);
				}
				// Check for shutdown
				if *self.shared.shutdown.lock() && buffer.is_empty() {
					break;
				}
				// Take the right amount of items from the buffer
				let take = buffer.len().min(self.batch_size);
				// Drain the buffer items into the batch
				batch.extend(buffer.drain(..take));
			}
			// If we have fewer items than batch_size, consider waiting for more
			let should_wait = batch.len() < self.batch_size && {
				// Lock the buffer to check the number of items
				let buffer = self.shared.buffer.lock();
				// Check if we have enough items to start a batch
				buffer.len() >= self.min_siblings.saturating_sub(batch.len())
			};
			// If we should wait, collect more requests with timeout
			if should_wait {
				// Calculate the timeout deadline
				let deadline = Instant::now() + self.timeout;
				// Collect requests until timeout or batch is full
				while batch.len() < self.batch_size {
					// Get the current instant
					let now = Instant::now();
					// Check if deadline is reached
					if now >= deadline {
						break;
					}
					// Calculate the remaining time
					let wait = deadline - now;
					// Wait on condvar with timeout
					let mut buffer = self.shared.buffer.lock();
					// Wait for items or timeout
					if self.shared.condvar.wait_for(&mut buffer, wait.into()).timed_out() {
						break;
					}
					// Take any new items
					let take = (self.batch_size - batch.len()).min(buffer.len());
					// Drain the buffer items into the batch
					if take > 0 {
						batch.extend(buffer.drain(..take));
					}
				}
			}
			// Drain any additional immediately available transactions
			if batch.len() < self.batch_size {
				// Lock the buffer to check the number of items
				let mut buffer = self.shared.buffer.lock();
				// Calculate the remaining number of slots in the batch
				let take = (self.batch_size - batch.len()).min(buffer.len());
				// Drain the buffer items into the batch
				if take > 0 {
					batch.extend(buffer.drain(..take));
				}
			}
			// Commit as a batch with single fsync
			affinitypool::spawn_local(|| {
				// Create a vector to store the results
				let mut results = Vec::with_capacity(batch.len());
				// Commit each transaction and store the result
				for request in batch.drain(..) {
					let result = request.txn.commit().map_err(Into::into);
					results.push((request.channel, result));
				}
				// Perform a single WAL flush and disk sync for all commits
				if let Err(e) = self.db.flush_wal(true) {
					let err = e.to_string();
					for (_, result) in &mut results {
						if result.is_ok() {
							*result = Err(Error::Transaction(err.clone()));
						}
					}
				}
				// Send results back to all waiters
				for (channel, result) in results {
					let _ = channel.send(result);
				}
			})
			.await;
		}
	}
}
