use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use rocksdb::{OptimisticTransactionDB, Options};

use super::TARGET;
use crate::kvs::config::{RocksDbConfig, SyncMode};
use crate::kvs::err::{Error, Result};

/// Background flusher for periodically syncing the Write-Ahead Log (WAL) to disk.
///
/// This component manages a dedicated background thread that periodically flushes and syncs
/// RocksDB's Write-Ahead Log to persistent storage at configurable intervals. It provides
/// a trade-off between write performance and durability guarantees.
///
/// ## Configuration
///
/// Background flushing is configured via the `sync` query parameter on the connection string:
/// - `rocksdb:///path?sync=200ms` -- flush every 200 milliseconds
/// - `rocksdb:///path?sync=1s` -- flush every second
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
/// lost in the event of a system crash or power failure. Full durability is only guaranteed if
/// the operating once the data is flushed to disk at the specified interval.
pub struct BackgroundFlusher {
	/// Shutdown flag
	shutdown: Arc<AtomicBool>,
	/// Thread handle
	handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl BackgroundFlusher {
	/// Pre-configure RocksDB options for periodic background flushing.
	pub(super) fn configure(opts: &mut Options, config: &RocksDbConfig) {
		// Don't configure if the sync mode is not every
		let SyncMode::Interval(interval) = config.sync_mode else {
			return;
		};
		// Log the sync mode specifically
		info!(target: TARGET, "Sync mode: background syncing on interval");
		// Log the background write-ahead-log flushing interval
		info!(target: TARGET, "Background write-ahead-log flushing: enabled (interval={}ms)",
			interval.as_millis(),
		);
		// Set incremental asynchronous bytes per sync to 512KiB
		opts.set_wal_bytes_per_sync(512 * 1024);
		// Enable manual WAL flush
		opts.set_manual_wal_flush(true);
	}

	/// Create and new background flusher
	pub fn new(db: Pin<Arc<OptimisticTransactionDB>>, interval: Duration) -> Result<Self> {
		// Create a new shutdown flag
		let shutdown = Arc::new(AtomicBool::new(false));
		// Clone the shutdown flag
		let finished = shutdown.clone();
		// Spawn the background flusher thread
		let handle = thread::Builder::new()
			.name("rocksdb-background-flusher".to_string())
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
						error!("Failed to flush WAL: {err}");
					}
				}
			})
			.map_err(|_| {
				Error::Datastore("failed to spawn RocksDB background flush thread".to_string())
			})?;
		// Create a new background flusher
		Ok(Self {
			shutdown,
			handle: Mutex::new(Some(handle)),
		})
	}

	/// Shutdown the background flusher
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
