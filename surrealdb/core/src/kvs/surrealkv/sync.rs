//! WAL synchronization components for SurrealKV.
//!
//! This module provides background WAL flushing and grouped commit coordination
//! for the SurrealKV storage engine.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use surrealkv::Tree;
use tokio::sync::oneshot::{self, Sender};

use super::TARGET;
use crate::kvs::err::{Error, Result};

// ============================================================================
// BackgroundFlusher
// ============================================================================

/// Background flusher for periodically syncing the Write-Ahead Log (WAL) to disk.
///
/// This component manages a dedicated background thread that periodically flushes and syncs
/// SurrealKV's Write-Ahead Log to persistent storage at configurable intervals. It provides
/// a trade-off between write performance and durability guarantees.
///
/// ## Configuration
///
/// Background flushing is configured via the `sync` query parameter on the connection string:
/// - `surrealkv:///path?sync=200ms` -- flush every 200 milliseconds
/// - `surrealkv:///path?sync=1s` -- flush every second
///
/// ## Durability
///
/// When background flushing is enabled:
/// - Transaction commits are written to the WAL in memory buffers
/// - The background thread periodically flushes these buffers to disk
/// - **Data committed between flushes may be lost** in the event of a system crash or power failure
/// - Write operations are faster due to reduced fsync overhead
///
/// When enabled, this component provides loose durability guarantees. All committed transactions
/// are persisted to disk, but the operating system may buffer the data in memory, and it may be
/// lost in the event of a system crash or power failure. Full durability is only guaranteed
/// once the data is flushed to disk at the specified interval.
pub(super) struct BackgroundFlusher {
	/// Shutdown flag
	shutdown: Arc<AtomicBool>,
	/// Thread handle
	handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl BackgroundFlusher {
	/// Create a new background flusher.
	pub fn new(db: Tree, interval: Duration) -> Result<Self> {
		// Create a new shutdown flag
		let shutdown = Arc::new(AtomicBool::new(false));
		// Clone the shutdown flag
		let finished = shutdown.clone();
		// Spawn the background flusher thread
		let handle = thread::Builder::new()
			.name("surrealkv-background-flusher".to_string())
			.spawn(move || {
				loop {
					// Wait for the specified interval
					thread::sleep(interval);
					// Check shutdown flag again after sleep
					if finished.load(Ordering::Relaxed) {
						break;
					}
					// Flush the WAL to disk periodically
					if let Err(err) = db.flush_wal(true) {
						error!(target: TARGET, "Failed to flush WAL: {err}");
					}
				}
			})
			.map_err(|_| {
				Error::Datastore("failed to spawn SurrealKV background flush thread".to_string())
			})?;
		// Create a new background flusher
		Ok(Self {
			shutdown,
			handle: Mutex::new(Some(handle)),
		})
	}

	/// Shutdown the background flusher.
	pub fn shutdown(&self) -> Result<()> {
		// Signal shutdown
		self.shutdown.store(true, Ordering::Relaxed);
		// Wait for thread to finish
		if let Some(handle) = self.handle.lock().take() {
			let _ = handle.join();
		}
		// All good
		Ok(())
	}
}

// ============================================================================
// CommitCoordinator
// ============================================================================

/// Shared state for producer-consumer communication between transaction submitters and the batcher.
///
/// This structure implements the synchronization primitives for a multi-producer, single-consumer
/// pattern where multiple threads can wait for WAL synchronization while a single background
/// thread (the [`CommitBatcher`]) performs grouped fsync operations.
///
/// # Communication Protocol
///
/// **Producers** ([`CommitCoordinator::wait_for_sync`]):
/// 1. Commit their SurrealKV transaction on the caller thread
/// 2. Lock the `buffer` mutex
/// 3. Check the `shutdown` flag to ensure the coordinator is still running
/// 4. Push a [`SyncRequest`] into the buffer
/// 5. Signal the `condvar` to wake the batcher
/// 6. Release the lock and await the response channel
///
/// **Consumer** ([`CommitBatcher::run`]):
/// 1. Lock the `buffer` mutex
/// 2. Wait on the `condvar` until the buffer is non-empty or shutdown is signaled
/// 3. Drain sync requests from the buffer (up to `max_batch_size`)
/// 4. Release the lock
/// 5. Perform a single `flush_wal(true)` for all waiters
/// 6. Send results back through each request's response channel
///
/// # Thread Safety
///
/// All fields are protected by appropriate synchronization primitives (`Mutex` and `Condvar`)
/// to ensure safe concurrent access from multiple threads. The condition variable prevents
/// busy-waiting and ensures efficient wake-up semantics.
struct SharedState {
	/// Shutdown flag
	shutdown: Arc<AtomicBool>,
	/// Buffer of pending sync requests awaiting batch processing
	buffer: Mutex<Vec<SyncRequest>>,
	/// Condition variable to wake the batcher thread when work arrives
	condvar: Condvar,
}

/// A request to wait for WAL synchronization.
///
/// This structure encapsulates a response channel that will be notified once the
/// WAL has been flushed to disk. Transactions are committed on the caller thread,
/// and this request only participates in the grouped fsync operation.
struct SyncRequest {
	/// The channel to send the result of the WAL flush
	channel: Sender<Result<()>>,
}

/// Coordinator for batching WAL synchronization with adaptive grouping.
///
/// This coordinator allows multiple threads to commit their SurrealKV transactions in parallel
/// on their own threads, while batching the expensive `fsync` operations. When synced writes
/// are enabled, multiple waiters are grouped together and woken up after a single `flush_wal(true)`
/// operation, significantly improving throughput while maintaining durability guarantees.
///
/// ## Configuration
///
/// Grouped commit is configured via the `sync` query parameter on the connection string:
/// - `surrealkv:///path?sync=every` -- groups transaction commits together and wait for sync
///
/// Batching behavior is controlled by environment variables:
/// - `SURREAL_SURREALKV_GROUPED_COMMIT_TIMEOUT`: Maximum wait time for collecting a batch
///   (nanoseconds)
/// - `SURREAL_SURREALKV_GROUPED_COMMIT_WAIT_THRESHOLD`: Waiter count to trigger waiting
/// - `SURREAL_SURREALKV_GROUPED_COMMIT_MAX_BATCH_SIZE`: Maximum waiters per batch
///
/// # Design Philosophy
///
/// Unlike traditional grouped commit implementations that serialize all commit operations,
/// this coordinator:
/// - Allows **parallel commits**: Each thread commits its SurrealKV transaction independently
/// - Groups **only the fsync**: Multiple threads wait together for a single WAL flush
/// - Maximizes **CPU parallelism**: No single-threaded commit bottleneck
///
/// # Adaptive Batching
///
/// The coordinator employs an adaptive batching algorithm that balances latency and throughput:
///
/// - **Low load** (< `wait_threshold`): Flushes immediately for low latency
/// - **Moderate load** (>= `wait_threshold`, < `max_batch_size`): Waits up to `timeout` to collect
///   more waiters for better batching efficiency
/// - **High load** (>= `max_batch_size`): Flushes immediately to maintain high throughput
///
/// # Durability
///
/// When enabled, this coordinator provides full durability guarantees. All committed transactions
/// are fully persisted to disk and will survive system crashes or power failures. This is achieved
/// by explicitly flushing the WAL to disk after each batch.
pub(super) struct CommitCoordinator {
	/// Shared state for communication with the batcher
	shared: Arc<SharedState>,
	/// Handle to the background batcher thread
	handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl CommitCoordinator {
	/// Create a new commit coordinator.
	pub fn new(
		db: Tree,
		timeout: u64,
		wait_threshold: usize,
		max_batch_size: usize,
	) -> Result<Self> {
		// Log the batched group commit configuration options
		info!(target: TARGET, "Grouped commit: enabled (timeout={}ns, wait_threshold={}, max_batch_size={})",
			timeout,
			wait_threshold,
			max_batch_size,
		);
		// Create shared state with pre-allocated buffer
		let shared = Arc::new(SharedState {
			shutdown: Arc::new(AtomicBool::new(false)),
			buffer: Mutex::new(Vec::with_capacity(max_batch_size)),
			condvar: Condvar::new(),
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
			.name("surrealkv-commit-coordinator".to_string())
			.spawn(move || {
				batcher.run();
			})
			.map_err(|_| {
				Error::Datastore("failed to spawn SurrealKV commit coordinator thread".to_string())
			})?;
		// Create a new commit coordinator
		Ok(Self {
			shared,
			handle: Mutex::new(Some(handle)),
		})
	}

	/// Wait for the next grouped WAL flush.
	///
	/// This should be called after the transaction has been committed on the caller thread.
	/// The caller will block until the background batcher performs a `flush_wal(true)` operation,
	/// ensuring that the transaction is durably persisted to disk.
	pub async fn wait_for_sync(&self) -> Result<()> {
		// Create a new oneshot response channel
		let (tx, rx) = oneshot::channel();
		// Create a new sync request
		let request = SyncRequest {
			channel: tx,
		};
		// Add to shared buffer and notify batcher
		let should_notify = {
			// Lock the buffer
			let mut buffer = self.shared.buffer.lock();
			// Check if buffer is currently empty
			let was_empty = buffer.is_empty();
			// Add the request to the buffer
			buffer.push(request);
			// Only notify if the batcher was waiting
			was_empty
		};
		// Notify the batcher that work is available
		if should_notify {
			self.shared.condvar.notify_one();
		}
		// Wait for the WAL flush to complete
		rx.await
			.map_err(|_| Error::Transaction("commit coordinator response channel closed".into()))?
	}

	/// Shutdown the commit coordinator.
	pub fn shutdown(&self) -> Result<()> {
		// Signal shutdown
		self.shared.shutdown.store(true, Ordering::Release);
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

/// Background worker thread that performs grouped WAL flushes.
///
/// The `CommitBatcher` runs in a dedicated thread and implements the core batching logic
/// for grouped WAL synchronization. It continuously receives sync requests from the
/// [`CommitCoordinator`], accumulates them into batches, and performs a single WAL flush
/// for the entire batch to minimize disk synchronization overhead.
///
/// # Important Design Note
///
/// This batcher does **not** commit transactions. Each caller thread commits its own
/// SurrealKV transaction before calling `wait_for_sync()`. This allows transaction commits
/// to happen in parallel across all CPU cores, while only the expensive fsync operation
/// is serialized and batched.
///
/// # Batching Algorithm
///
/// The batcher implements an adaptive strategy based on the current waiter load:
///
/// 1. **Wait for work**: The batcher sleeps on a condition variable until waiters arrive
/// 2. **Collect initial batch**: Drains up to `max_batch_size` sync requests from the buffer
/// 3. **Adaptive waiting**:
///    - If batch size < `wait_threshold`: Flush immediately (optimize for latency)
///    - If batch size >= `wait_threshold` and < `max_batch_size`: Wait up to `timeout` for more
///      waiters to arrive (optimize for throughput)
///    - If batch size >= `max_batch_size`: Flush immediately (prevent unbounded growth)
/// 4. **Flush WAL**: Perform a single `flush_wal(true)` to sync all commits to disk
/// 5. **Send results**: Notify all waiting callers through their response channels
///
/// # Shutdown
///
/// The batcher monitors the shutdown flag and gracefully terminates after processing
/// all remaining sync requests in the buffer.
struct CommitBatcher {
	/// Shared state for receiving commit requests
	shared: Arc<SharedState>,
	/// Reference to the database for explicit WAL flushing
	db: Tree,
	/// Maximum time to wait for collecting a batch
	timeout: Duration,
	/// Threshold for deciding whether to wait for more transactions
	wait_threshold: usize,
	/// Maximum number of transactions in a single batch
	max_batch_size: usize,
}

impl CommitBatcher {
	/// Run the background batcher loop.
	///
	/// Behavior:
	/// - Wakes when sync requests arrive
	/// - If few waiters (below `wait_threshold`): flushes immediately (low latency)
	/// - If some waiters (above `wait_threshold`): waits up to `timeout` (better batching)
	/// - If many waiters (up to `max_batch_size`): flushes immediately (high throughput)
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
				loop {
					// Check if we have work to do
					if !buffer.is_empty() {
						break;
					}
					// Wait for notification
					self.shared.condvar.wait(&mut buffer);
					// Check shutdown flag before continuing
					if self.shared.shutdown.load(Ordering::Acquire) {
						return;
					}
				}
				// Initially drain up to max_batch_size items
				let take = buffer.len().min(self.max_batch_size);
				// Drain the buffer items into the batch
				batch.extend(buffer.drain(..take));
			}
			// We wait if batch is above threshold and below max size
			let should_wait =
				batch.len() >= self.wait_threshold && batch.len() < self.max_batch_size;
			// If we should wait, collect more requests with timeout
			if should_wait {
				// Calculate the timeout deadline
				let deadline = Instant::now() + self.timeout;
				// Wait for more items until timeout
				loop {
					// Wait on condvar with timeout
					let mut buffer = self.shared.buffer.lock();
					// Get the current instant
					let now = Instant::now();
					// Calculate the remaining time
					let wait = deadline.saturating_duration_since(now);
					// Check if deadline is reached
					if wait.is_zero() {
						break;
					}
					// Wait for items or timeout
					if self.shared.condvar.wait_for(&mut buffer, wait).timed_out() {
						break;
					}
					// Take available items up to the maximum batch size
					if !buffer.is_empty() {
						// Calculate the number of transactions to take
						let take = (self.max_batch_size - batch.len()).min(buffer.len());
						// Drain any pending items up to the maximum batch size
						batch.extend(buffer.drain(..take));
					}
					// Break if we've reached maximum batch size
					if batch.len() >= self.max_batch_size {
						break;
					}
				}
			}
			// Perform a single WAL flush and disk sync for all commits
			let flush_result = self.db.flush_wal(true);
			// Send the result to all waiters
			if let Err(e) = flush_result {
				// Convert error to a string
				let err = e.to_string();
				// Send the error to all waiters
				for request in batch.drain(..) {
					let _ = request.channel.send(Err(Error::Transaction(err.clone())));
				}
			} else {
				// Send success to all waiters
				for request in batch.drain(..) {
					let _ = request.channel.send(Ok(()));
				}
			}
		}
	}
}
