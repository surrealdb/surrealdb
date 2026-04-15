use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use surrealkv::Tree;

use super::TARGET;
use crate::kvs::err::{Error, Result};

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
	/// Shared state for signalling shutdown to the background thread
	notify: Arc<ShutdownSignal>,
	/// Thread handle
	handle: Mutex<Option<thread::JoinHandle<()>>>,
}

/// Shared state for signalling shutdown to the background thread
struct ShutdownSignal {
	flag: AtomicBool,
	condvar: Condvar,
	mutex: Mutex<()>,
}

impl BackgroundFlusher {
	/// Create a new background flusher.
	pub fn new(db: Tree, interval: Duration) -> Result<Self> {
		// Create a new shutdown notifier
		let notify = Arc::new(ShutdownSignal {
			flag: AtomicBool::new(false),
			condvar: Condvar::new(),
			mutex: Mutex::new(()),
		});
		// Clone the shutdown notifier
		let signal = notify.clone();
		// Spawn the background flusher thread
		let handle = thread::Builder::new()
			.name("surrealkv-background-flusher".to_string())
			.spawn(move || {
				loop {
					// Wait for the specified interval
					let mut guard = signal.mutex.lock();
					signal.condvar.wait_for(&mut guard, interval);
					drop(guard);
					// Check shutdown flag again after sleep
					if signal.flag.load(Ordering::Relaxed) {
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
			notify,
			handle: Mutex::new(Some(handle)),
		})
	}

	/// Shutdown the background flusher.
	pub fn shutdown(&self) -> Result<()> {
		// Signal shutdown
		self.notify.flag.store(true, Ordering::Relaxed);
		// Notify the background flusher thread
		self.notify.condvar.notify_one();
		// Wait for the background flusher thread to finish
		if let Some(handle) = self.handle.lock().take() {
			let _ = handle.join();
		}
		Ok(())
	}
}
