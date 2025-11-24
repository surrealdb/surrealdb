use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use rocksdb::{OptimisticTransactionDB, Options};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::kvs::err::{Error, Result};

/// Coordinator for batching transaction commits together
pub struct CommitCoordinator {
	/// The sender for commit requests
	sender: mpsc::Sender<CommitRequest>,
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
		opts: &mut Options,
		db: Pin<Arc<OptimisticTransactionDB>>,
		timeout: u64,
		batch_size: usize,
		min_siblings: usize,
	) -> Self {
		// Create an unbounded channel
		let (sender, receiver) = mpsc::channel(batch_size * 4);
		// Enable manual WAL flushing
		opts.set_manual_wal_flush(true);
		// Create a new commit batcher
		let batcher = CommitBatcher {
			db,
			receiver,
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
			sender,
		}
	}

	/// Submit a transaction for grouped commit
	pub async fn commit(
		&self,
		txn: rocksdb::Transaction<'static, OptimisticTransactionDB>,
	) -> Result<()> {
		// Create a new oneshot channel
		let (tx, rx) = oneshot::channel();
		// Create a new commit request
		let request = CommitRequest {
			txn,
			channel: tx,
		};
		// Send the commit request to the batcher
		self.sender
			.send(request)
			.await
			.map_err(|_| Error::Transaction("commit coordinator channel closed".into()))?;
		// Wait for the transaction to commit
		rx.await
			.map_err(|_| Error::Transaction("commit coordinator response channel closed".into()))?
	}
}

impl CommitBatcher {
	/// Run the background batcher loop
	async fn run(mut self) {
		// Pre-allocate batch vector once
		let mut batch = Vec::with_capacity(self.batch_size);
		// Loop continuously until the channel is closed
		loop {
			// Immediately drain any requests that are already queued
			let total = self.receiver.recv_many(&mut batch, self.batch_size).await;
			// If channel is closed and no items received, exit
			if total == 0 {
				break;
			}
			// If we still don't have enough, check if we should wait for more
			let wait =
				batch.len() < self.batch_size && self.receiver.len() >= self.min_siblings - 1;
			// If we should wait, collect more requests with timeout
			if wait {
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
			}
			// Drain any additional immediately available transactions
			if batch.len() < self.batch_size && self.receiver.len() > 0 {
				// Calculate the remaining number of slots in the batch
				let remaining = self.batch_size - batch.len();
				// Drain the remaining slots in the batch
				self.receiver.recv_many(&mut batch, remaining).await;
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
