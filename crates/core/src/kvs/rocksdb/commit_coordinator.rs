use std::pin::Pin;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use rocksdb::{OptimisticTransactionDB, Options};
use tokio::sync::oneshot::{self, Sender};

use super::{TARGET, cnf};
use crate::kvs::err::{Error, Result};

/// Shared state for producer-consumer communication between transaction submitters and the batcher.
///
/// This structure implements the synchronization primitives for a multi-producer, single-consumer
/// pattern where multiple threads can submit transactions for commit while a single background
/// thread (the [`CommitBatcher`]) processes them in batches.
///
/// # Communication Protocol
///
/// **Producers** ([`CommitCoordinator::commit`]):
/// 1. Lock the `buffer` mutex
/// 2. Check the `shutdown` flag to ensure the coordinator is still running
/// 3. Push a [`CommitRequest`] into the buffer
/// 4. Signal the `condvar` to wake the batcher
/// 5. Release the lock and await the response channel
///
/// **Consumer** ([`CommitBatcher::run`]):
/// 1. Lock the `buffer` mutex
/// 2. Wait on the `condvar` until the buffer is non-empty or shutdown is signaled
/// 3. Drain transactions from the buffer (up to `max_batch_size`)
/// 4. Release the lock and process the batch
/// 5. Send results back through each request's response channel
///
/// # Thread Safety
///
/// All fields are protected by appropriate synchronization primitives (`Mutex` and `Condvar`)
/// to ensure safe concurrent access from multiple threads. The condition variable prevents
/// busy-waiting and ensures efficient wake-up semantics.
struct SharedState {
	/// Buffer of pending commit requests awaiting batch processing
	buffer: Mutex<Vec<CommitRequest>>,
	/// Condition variable to wake the batcher thread when work arrives
	condvar: Condvar,
	/// Flag indicating the coordinator is shutting down; checked by both producers and consumer
	shutdown: Mutex<bool>,
}

/// A request to commit a transaction.
///
/// This structure encapsulates a RocksDB transaction along with a response channel.
/// When the transaction is committed by the background batcher, the result is sent
/// back through the channel to the waiting caller.
struct CommitRequest {
	/// The transaction to commit
	txn: rocksdb::Transaction<'static, OptimisticTransactionDB>,
	/// The channel to send the result of the commit
	channel: Sender<Result<()>>,
}

/// Coordinator for batching transaction commits together with adaptive grouping.
///
/// This coordinator collects multiple transaction commits and processes them in batches
/// to reduce the overhead of disk synchronization operations. When synced writes are enabled,
/// each batch is committed with a single `fsync` to the Write-Ahead Log (WAL) and disk,
/// significantly improving throughput while maintaining durability guarantees.
///
/// # Adaptive Batching Strategy
///
/// The coordinator employs an adaptive batching algorithm that balances latency and throughput:
///
/// - **Low load** (< `wait_threshold`): Commits immediately for low latency
/// - **Moderate load** (≥ `wait_threshold`, < `max_batch_size`): Waits up to `timeout` to collect
///   more transactions for better batching efficiency
/// - **High load** (≥ `max_batch_size`): Commits immediately to maintain high throughput
///
/// # Configuration
///
/// Batching behavior is controlled by environment variables:
/// - `SURREAL_ROCKSDB_GROUPED_COMMIT_TIMEOUT`: Maximum wait time for collecting a batch
///   (nanoseconds)
/// - `SURREAL_ROCKSDB_GROUPED_COMMIT_WAIT_THRESHOLD`: Transaction count to trigger waiting
/// - `SURREAL_ROCKSDB_GROUPED_COMMIT_MAX_BATCH_SIZE`: Maximum transactions per batch
///
/// # Durability
///
/// When enabled, this coordinator provides full durability guarantees. All committed transactions
/// are fully persisted to disk and will survive system crashes or power failures. This is achieved
/// by explicitly flushing the WAL to disk after each batch.
pub struct CommitCoordinator {
	/// Shared state for communication with the batcher
	shared: Arc<SharedState>,
	/// Handle to the background batcher thread
	handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl CommitCoordinator {
	/// Pre-configure the commit coordinator
	pub(super) fn configure(opts: &mut Options) -> Result<bool> {
		// If the user has enabled both synced transaction writes and background flushing,
		// we return an error because the two features are incompatible. When sync is enabled,
		// the transaction commits are always batched together, written to WAL, and then
		// flushed to disk. This means that the background flushing is redundant.
		if *cnf::SYNC_DATA && *cnf::ROCKSDB_BACKGROUND_FLUSH {
			Err(Error::Datastore(
				"Synced transaction writes and background flushing are incompatible".to_string(),
			))
		}
		// If the user has enabled synced transaction writes and disabled background flushing,
		// we enable grouped commit. This means that the transaction commits are batched
		// together, written to WAL, and then flushed to disk. This ensures that transactions
		// are grouped together and flushed to disk in a single operation, reducing the impact
		// of disk syncing for each individual transaction. In this mode, when a transaction is
		// committed, the data is fully durable and will not be lost in the event of a system crash.
		else if *cnf::SYNC_DATA {
			// Log the batched group commit configuration options
			info!(target: TARGET, "Grouped commit: enabled (timeout={}, wait_threshold={}, max_batch_size={})",
				*cnf::ROCKSDB_GROUPED_COMMIT_TIMEOUT,
				*cnf::ROCKSDB_GROUPED_COMMIT_WAIT_THRESHOLD,
				*cnf::ROCKSDB_GROUPED_COMMIT_MAX_BATCH_SIZE,
			);
			// Enable manual WAL flushing
			opts.set_manual_wal_flush(true);
			// Continue
			Ok(true)
		}
		// If the user has disabled both synced transaction writes and background flushing,
		// we defer to the operating system buffers for disk sync. This means that the transaction
		// commits are written to WAL on commit, but are then flushed to disk by the operating
		// system at an unspecified time. In the event of a system crash, data may be lost if the
		// operating system has not yet flushed and synced the data to disk.
		else {
			// Log that the batched commit coordinator is disabled
			info!(target: TARGET, "Batched commit coordinator: disabled");
			// Continue
			Ok(false)
		}
	}
	/// Create a new commit coordinator
	pub fn new(db: Pin<Arc<OptimisticTransactionDB>>) -> Result<Self> {
		// Get the batched commit configuration options
		let timeout = *cnf::ROCKSDB_GROUPED_COMMIT_TIMEOUT;
		let wait_threshold = *cnf::ROCKSDB_GROUPED_COMMIT_WAIT_THRESHOLD;
		let max_batch_size = *cnf::ROCKSDB_GROUPED_COMMIT_MAX_BATCH_SIZE;
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
		// Spawn the background commit coordinator thread
		let handle = thread::Builder::new()
			.name("rocksdb-commit-coordinator".to_string())
			.spawn(move || {
				batcher.run();
			})
			.map_err(|_| {
				Error::Datastore("failed to spawn RocksDB commit coordinator thread".to_string())
			})?;
		// Create a new commit coordinator
		Ok(Self {
			shared,
			handle: Mutex::new(Some(handle)),
		})
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

	/// Shutdown the commit coordinator
	pub fn shutdown(&self) -> Result<()> {
		// Signal shutdown
		*self.shared.shutdown.lock() = true;
		// Notify the batcher
		self.shared.condvar.notify_all();
		// Wait for thread to finish
		if let Some(handle) = self.handle.lock().take() {
			let _ = handle.join();
		}
		// All good
		Ok(())
	}
}

/// Background worker thread that processes commit requests in batches.
///
/// The `CommitBatcher` runs in a dedicated thread and implements the core batching logic
/// for grouped transaction commits. It continuously receives commit requests from the
/// [`CommitCoordinator`], accumulates them into batches, and performs a single WAL flush
/// for the entire batch to minimize disk synchronization overhead.
///
/// # Thread Safety
///
/// The batcher communicates with producer threads (submitting transactions) through
/// a shared buffer protected by a mutex and condition variable. This allows multiple
/// concurrent transactions to be queued while the batcher processes batches atomically.
///
/// # Batching Algorithm
///
/// The batcher implements an adaptive strategy based on the current transaction load:
///
/// 1. **Wait for work**: The batcher sleeps on a condition variable until transactions arrive
/// 2. **Collect initial batch**: Drains up to `max_batch_size` transactions from the buffer
/// 3. **Adaptive waiting**:
///    - If batch size < `wait_threshold`: Process immediately (optimize for latency)
///    - If batch size ≥ `wait_threshold` and < `max_batch_size`: Wait up to `timeout` for more
///      transactions to arrive (optimize for throughput)
///    - If batch size ≥ `max_batch_size`: Process immediately (prevent unbounded growth)
/// 4. **Commit batch**: Commit all transactions individually (optimistic locking validation)
/// 5. **Flush WAL**: Perform a single `flush_wal(true)` to sync all commits to disk
/// 6. **Send results**: Notify all waiting callers through their response channels
///
/// # Shutdown
///
/// The batcher monitors the shutdown flag and gracefully terminates after processing
/// all remaining transactions in the buffer.
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
