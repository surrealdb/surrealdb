use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rocksdb::{OptimisticTransactionDB, Options};
use tokio::sync::oneshot;

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
	/// Threshold for deciding whether to wait for more transactions
	wait_threshold: usize,
	/// Maximum number of transactions in a single batch
	max_batch_size: usize,
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
		wait_threshold: usize,
		max_batch_size: usize,
	) -> Result<Self> {
		// Enable manual WAL flushing
		opts.set_manual_wal_flush(true);
		// Create shared state with pre-allocated buffer
		let shared = Arc::new(SharedState {
			buffer: Mutex::new(Vec::with_capacity(max_batch_size)),
			condvar: Condvar::new(),
			shutdown: Mutex::new(false),
		});
		// Create a new commit batcher
		let batcher = CommitBatcher {
			shared: shared.clone(),
			db,
			wait_threshold,
			max_batch_size,
			timeout: Duration::from_nanos(timeout),
		};
		// Spawn the background thread
		let res = thread::Builder::new().name("rocksdb-commit-coordinator".to_string()).spawn(
			move || {
				batcher.run();
			},
		);
		// Catch any thread spawning errors
		if res.is_err() {
			return Err(Error::Datastore(
				"failed to spawn RocksDB commit coordinator thread".to_string(),
			));
		}
		// Return the commit coordinator
		Ok(Self {
			shared,
		})
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
	///
	/// Behavior:
	/// - Wakes when transactions arrive
	/// - If few transactions (below `wait_threshold`): commits immediately (low latency)
	/// - If some transactions (above `wait_threshold`): waits up to `timeout` (better batching)
	/// - If many transactions (up to `max_batch_size`): commits immediately (high throughput)
	/// - Batches capped at `max_batch_size` to prevent unbounded growth
	fn run(self) {
		// Pre-allocate batch vector once
		let mut batch = Vec::with_capacity(self.max_batch_size);
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
				// Initially drain up to max_batch_size items
				let take = buffer.len().min(self.max_batch_size);
				// Drain the buffer items into the batch
				batch.extend(buffer.drain(..take));
			}
			// We wait if batched transactions is above threshold
			let should_wait = batch.len() > self.wait_threshold;
			// We wait if batched transactions is below max batch size
			let should_wait = should_wait && batch.len() < self.max_batch_size;
			// If we should wait, collect more requests with timeout
			if should_wait {
				// Calculate the timeout deadline
				let deadline = Instant::now() + self.timeout;
				// Wait for more items until timeout
				loop {
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
					if self.shared.condvar.wait_for(&mut buffer, wait).timed_out() {
						break;
					}
					// Take available items up to the maximum batch size
					if !buffer.is_empty() {
						// Get the total transactions in the batch
						let total = batch.len();
						// Get the number of pending transactions
						let extra = buffer.len();
						// Calculate the number of transactions to take
						let take = self.max_batch_size.saturating_sub(total).min(extra);
						// Drain any pending items up to the maximum batch size
						batch.extend(buffer.drain(..take));
					}
					// Break if we've reached maximum batch size
					if batch.len() >= self.max_batch_size {
						break;
					}
				}
			}
			// Check if we have batch capacity remaining
			if batch.len() < self.max_batch_size {
				// Drain any pending items up to the maximum batch size
				let mut buffer = self.shared.buffer.lock();
				// Check if there are any pending items
				if !buffer.is_empty() {
					// Get the total transactions in the batch
					let total = batch.len();
					// Get the number of pending transactions
					let extra = buffer.len();
					// Calculate the number of transactions to take
					let take = self.max_batch_size.saturating_sub(total).min(extra);
					// Drain any pending items up to the maximum batch size
					batch.extend(buffer.drain(..take));
				}
			}
			// Commit as a batch with single fsync to the WAL and disk
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
		}
	}
}
