use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use rocksdb::{OptimisticTransactionDB, Options};

use super::{TARGET, cnf};
use crate::kvs::err::{Error, Result};

/// Background flusher for periodically syncing the Write-Ahead Log (WAL) to disk.
///
/// This component manages a dedicated background thread that periodically flushes and syncs
/// RocksDB's Write-Ahead Log to persistent storage at configurable intervals. It provides
/// a trade-off between write performance and durability guarantees.
///
/// ## Durability Trade-offs
///
/// When background flushing is enabled:
/// - Transaction commits are written to the WAL in memory buffers
/// - The background thread periodically flushes these buffers to disk
/// - **Data committed between flushes may be lost** in the event of a system crash or power failure
/// - Write operations are faster due to reduced fsync overhead
///
/// ## Configuration
///
/// Background flushing is controlled by two environment variables:
/// - `SURREAL_ROCKSDB_BACKGROUND_FLUSH`: Enable/disable background flushing
/// - `SURREAL_ROCKSDB_BACKGROUND_FLUSH_INTERVAL`: Flush interval in nanoseconds
///
/// ## Compatibility
///
/// This feature is **incompatible** with synced transaction writes (`SURREAL_SYNC_DATA=true`).
/// When sync is enabled, transaction commits are already batched and immediately flushed
/// to disk, making background flushing redundant and potentially conflicting.
pub struct BackgroundFlusher {
	/// Shutdown flag
	shutdown: Arc<AtomicBool>,
	/// Thread handle
	handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl BackgroundFlusher {
	// Pre-configure the commit coordinator
	pub(super) fn configure(opts: &mut Options) -> Result<bool> {
		// If the user has enabled both synced transaction writes and background flushing,
		// we return an error because the two features are incompatible. When background
		// flushing is enabled, the transaction commits are written to WAL in memory, and then
		// flushed to disk by the background thread.
		if *cnf::SYNC_DATA && *cnf::ROCKSDB_BACKGROUND_FLUSH {
			Err(Error::Datastore(
				"Synced transaction writes and background flushing are incompatible".to_string(),
			))
		}
		// If the user has enabled background flushing, we wait for a periodic background thread
		// to flush the WAL to disk, and wait for the data to be synced to disk. This means that
		// the transaction commits are written to WAL in memory, and in the event of a system
		// crash, data committed before the periodic background flush will be lost.
		else if *cnf::ROCKSDB_BACKGROUND_FLUSH {
			// Log the background write-ahead-log flushing interval
			info!(target: TARGET, "Background write-ahead-log flushing: enabled (interval={}ns)",
				*cnf::ROCKSDB_BACKGROUND_FLUSH_INTERVAL,
			);
			// Set incremental asynchronous bytes per sync to 512KiB
			opts.set_wal_bytes_per_sync(512 * 1024);
			// Enable manual WAL flush
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
			// Log that the background flusher is disabled
			info!(target: TARGET, "Background write-ahead-log flushing: disabled");
			// Continue
			Ok(false)
		}
	}

	/// Create and start a background flusher
	pub fn new(db: Pin<Arc<OptimisticTransactionDB>>) -> Result<Self> {
		// Get the background flusher configuration options
		let interval = *cnf::ROCKSDB_BACKGROUND_FLUSH_INTERVAL;
		// Convert the interval to a duration
		let duration = Duration::from_nanos(interval);
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
					thread::sleep(duration);
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
