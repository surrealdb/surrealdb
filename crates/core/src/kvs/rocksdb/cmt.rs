use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rocksdb::OptimisticTransactionDB;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::kvs::err::{Error, Result};

/// Coordinator for batching transaction commits together
pub struct CommitCoordinator {
	sender: mpsc::Sender<CommitRequest>,
}

/// A request to commit a transaction
struct CommitRequest {
	txn: rocksdb::Transaction<'static, OptimisticTransactionDB>,
	response: oneshot::Sender<Result<()>>,
}

/// The background batcher that processes commit requests
struct CommitBatcher {
	/// Channel receiver for incoming commit requests
	receiver: mpsc::Receiver<CommitRequest>,
	/// Reference to the database for explicit WAL flushing
	db: Pin<Arc<OptimisticTransactionDB>>,
	/// Maximum time to wait for collecting a batch
	timeout: Duration,
	/// Maximum number of transactions per batch
	batch_size: usize,
	/// Minimum number of concurrent transactions before using timeout
	min_siblings: usize,
}

impl CommitCoordinator {
	/// Create a new commit coordinator and spawn the background batcher task
	pub fn new(
		db: Pin<Arc<OptimisticTransactionDB>>,
		timeout: u64,
		batch_size: usize,
		min_siblings: usize,
	) -> Self {
		// Create an unbounded channel
		let (sender, receiver) = mpsc::channel(batch_size * 4);
		// Create a new commit batcher
		let batcher = CommitBatcher {
			db,
			receiver,
			batch_size,
			min_siblings,
			timeout: Duration::from_micros(timeout),
		};
		// Spawn the background task
		tokio::spawn(async move {
			batcher.run().await;
		});

		Self {
			sender,
		}
	}

	/// Submit a transaction for grouped commit
	pub async fn commit(
		&self,
		txn: rocksdb::Transaction<'static, OptimisticTransactionDB>,
	) -> Result<()> {
		let (tx, rx) = oneshot::channel();
		let request = CommitRequest {
			txn,
			response: tx,
		};

		// Send the commit request to the batcher
		self.sender
			.send(request)
			.await
			.map_err(|_| Error::Internal("commit coordinator channel closed".into()))?;

		// Wait for the response
		rx.await
			.map_err(|_| Error::Internal("commit coordinator response channel closed".into()))?
	}
}

impl CommitBatcher {
	/// Run the background batcher loop
	async fn run(mut self) {
		loop {
			// Wait for the first request
			let Some(first_request) = self.receiver.recv().await else {
				// Channel closed, exit
				break;
			};
			// Start collecting a batch
			let mut batch = vec![first_request];
			// Peek at queue depth to decide if we should wait
			let should_wait = self.receiver.len() >= self.min_siblings - 1;
			// If we should wait, collect more requests with timeout
			if should_wait {
				// Start the timer
				let deadline = Instant::now() + self.timeout;
				// Collect requests until timeout or batch is full
				while batch.len() < self.batch_size {
					// Get the current instant
					let now = Instant::now();
					// Check if deadline is reached
					if now >= deadline {
						break;
					}
					// Try to receive more requests within the remaining time
					match tokio::time::timeout_at(deadline.into(), self.receiver.recv()).await {
						Ok(Some(request)) => batch.push(request),
						Ok(None) | Err(_) => break,
					};
				}
			} else {
				// Don't wait long, but quickly drain immediately available transactions
				while batch.len() < self.batch_size {
					match self.receiver.try_recv() {
						Ok(request) => batch.push(request),
						Err(_) => break,
					}
				}
			}
			// Commit as a batch with single fsync
			affinitypool::spawn_local(|| {
				// Create a vector to store the results
				let mut results = Vec::with_capacity(batch.len());
				// Commit each transaction and store the result
				for request in batch {
					let result = request.txn.commit().map_err(Into::into);
					results.push((request.response, result));
				}
				// Ensure the WAL is flushed and synced
				let _ = self.db.flush_wal(true);
				// Send responses back to all waiters
				for (response, result) in results {
					let _ = response.send(result);
				}
			})
			.await;
		}
	}
}
