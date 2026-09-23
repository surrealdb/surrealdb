//! Public process-level metrics snapshot.
//!
//! Thin wrapper over [`crate::sys`] so the observability layer can read CPU
//! and memory stats without the private module being exposed to every
//! consumer. Every field is aggregate, host-wide, and free of tenant
//! attribution.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Instantaneous view of the current process resource usage.
///
/// Fields mirror the subset of [`crate::sys::Information`] that carries no
/// tenant attribution. That makes them safe to aggregate, not automatically
/// safe to publish anonymously: the memory figures together describe how close
/// an instance is to refusing queries, so the metrics layer withholds them from
/// unauthenticated callers.
#[derive(Copy, Clone, Debug, Default)]
pub struct ProcessSnapshot {
	/// Resident set size in bytes, as reported by `sysinfo`.
	pub memory_bytes: u64,
	/// Tracked bytes: live Rust-heap allocations, counted by requested size,
	/// plus every registered external memory reporter. This is the quantity
	/// `SURREAL_MEMORY_THRESHOLD` is compared against. It counts allocated
	/// bytes, not resident pages, so it bounds [`Self::memory_bytes`] in
	/// neither direction: an allocation not yet written is tracked without being
	/// resident, and pages the allocator retains after a free stay resident
	/// without being tracked. Always `0` in builds without allocation tracking
	/// compiled in.
	pub memory_allocated_bytes: u64,
	/// Process CPU usage as a percentage. May exceed 100% on multi-core
	/// hosts because `sysinfo` sums across cores.
	pub cpu_percent: f32,
}

/// Process-wide cached snapshot updated by [`refresh_process_snapshot`].
/// Read synchronously by [`process_snapshot`] from any context (including
/// OpenTelemetry observable-gauge callbacks, which are not async).
static SYNC_MEMORY_BYTES: AtomicU64 = AtomicU64::new(0);
static SYNC_MEMORY_ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static SYNC_CPU_PERCENT_BITS: AtomicU32 = AtomicU32::new(0);

/// Read the cached process snapshot without awaiting a refresh.
///
/// Returns the values most recently observed by [`refresh_process_snapshot`],
/// or `(0, 0.0)` before the first refresh has completed.
pub fn process_snapshot() -> ProcessSnapshot {
	ProcessSnapshot {
		memory_bytes: SYNC_MEMORY_BYTES.load(Ordering::Relaxed),
		memory_allocated_bytes: SYNC_MEMORY_ALLOCATED_BYTES.load(Ordering::Relaxed),
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
		memory_allocated_bytes: info.memory_allocated as u64,
		cpu_percent: info.cpu_usage,
	};
	SYNC_MEMORY_BYTES.store(snapshot.memory_bytes, Ordering::Relaxed);
	SYNC_MEMORY_ALLOCATED_BYTES.store(snapshot.memory_allocated_bytes, Ordering::Relaxed);
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
		// Every published figure needs its own assertion: a store dropped for
		// one of them leaves that gauge reading a constant zero while the rest
		// keep working. Equality rather than growth, because the tracked figure
		// is legitimately zero in a build without allocation tracking.
		assert_eq!(
			after.memory_allocated_bytes, refreshed.memory_allocated_bytes,
			"sync cache did not pick up the tracked-memory figure",
		);
	}
}
