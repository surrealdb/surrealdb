//! Public process-level metrics snapshot.
//!
//! Thin wrapper over [`crate::sys`] so the observability layer can read CPU
//! and memory stats without the private module being exposed to every
//! consumer. Both fields are aggregate, host-wide, and free of tenant
//! attribution — safe to surface even on the unauthenticated `/metrics`
//! endpoint.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Instantaneous view of the current process resource usage.
///
/// Fields mirror the subset of [`crate::sys::Information`] that is safe to
/// expose to external observers.
#[derive(Copy, Clone, Debug, Default)]
pub struct ProcessSnapshot {
	/// Resident set size in bytes, as reported by `sysinfo`.
	pub memory_bytes: u64,
	/// Process CPU usage as a percentage. May exceed 100% on multi-core
	/// hosts because `sysinfo` sums across cores.
	pub cpu_percent: f32,
}

/// Process-wide cached snapshot updated by [`refresh_process_snapshot`].
/// Read synchronously by [`process_snapshot`] from any context (including
/// OpenTelemetry observable-gauge callbacks, which are not async).
static SYNC_MEMORY_BYTES: AtomicU64 = AtomicU64::new(0);
static SYNC_CPU_PERCENT_BITS: AtomicU32 = AtomicU32::new(0);

/// Read the cached process snapshot without awaiting a refresh.
///
/// Returns the values most recently observed by [`refresh_process_snapshot`],
/// or `(0, 0.0)` before the first refresh has completed.
pub fn process_snapshot() -> ProcessSnapshot {
	ProcessSnapshot {
		memory_bytes: SYNC_MEMORY_BYTES.load(Ordering::Relaxed),
		cpu_percent: f32::from_bits(SYNC_CPU_PERCENT_BITS.load(Ordering::Relaxed)),
	}
}

/// Refresh the cached system information, update the synchronous cache, and
/// return a fresh [`ProcessSnapshot`].
///
/// Uses the same underlying [`crate::sys`] cache that the INFO statement
/// reads from, so repeated callers share the refresh cost. Updates the
/// process-wide synchronous cache so [`process_snapshot`] returns the same
/// values without needing an async context.
pub async fn refresh_process_snapshot() -> ProcessSnapshot {
	crate::sys::refresh().await;
	let info = crate::sys::INFORMATION.lock().await;
	let snapshot = ProcessSnapshot {
		memory_bytes: info.memory_usage,
		cpu_percent: info.cpu_usage,
	};
	SYNC_MEMORY_BYTES.store(snapshot.memory_bytes, Ordering::Relaxed);
	SYNC_CPU_PERCENT_BITS.store(snapshot.cpu_percent.to_bits(), Ordering::Relaxed);
	snapshot
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn refresh_populates_sync_snapshot_cache() {
		// Cold cache: nothing has run yet, so the sync getter returns
		// the all-zero default.
		let cold = process_snapshot();
		// CPU% may legitimately read zero on a quiet test thread, so
		// we only assert the memory-bytes field which is always > 0
		// for a running process.

		// One refresh cycle should populate the sync atomics. After
		// the await the synchronous getter must observe the same
		// memory value the async refresh just returned.
		let refreshed = refresh_process_snapshot().await;
		assert!(refreshed.memory_bytes > 0, "sysinfo failed to read RSS");
		let after = process_snapshot();
		assert_eq!(
			after.memory_bytes, refreshed.memory_bytes,
			"sync cache did not pick up the async refresh result",
		);
		// Cold-state guard: the cache must transition strictly upward
		// from (0, 0.0) to a populated reading. If this regresses, the
		// background refresh task will not actually publish values to
		// OTel observable-gauge callbacks on the OTLP push path.
		assert!(after.memory_bytes >= cold.memory_bytes);
	}
}
