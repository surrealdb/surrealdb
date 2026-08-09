//! Public process-level metrics snapshot.
//!
//! Thin wrapper over [`crate::sys`] so the observability layer can read CPU
//! and memory stats without the private module being exposed to every
//! consumer. Both fields are aggregate, host-wide, and free of tenant
//! attribution — safe to surface even on the unauthenticated `/metrics`
//! endpoint.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};

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

/// Whether some holder currently has the process-wide refresh claim.
static PROCESS_REFRESH_CLAIMED: AtomicBool = AtomicBool::new(false);

/// One holder's claim on refreshing the process snapshot on a schedule.
///
/// [`refresh_process_snapshot`] reads a single `sysinfo` handle per process and
/// `sysinfo` derives CPU percentage as a delta since that handle's previous
/// refresh, so the reading is only meaningful when one scheduler owns the
/// cadence. Every datastore schedules the refresh — the datastore that has this
/// claim performs it and the rest skip — which is what keeps the reading
/// independent of how many datastores a process builds.
///
/// The claim is sticky: a holder that has taken it keeps it across passes, so
/// the delta window stays one refresh interval rather than alternating between
/// holders. It is released when the last clone of a holder is dropped, so it
/// passes to another datastore when the owning task ends — including when that
/// task is dropped rather than returning.
#[derive(Clone)]
pub(crate) struct RefreshClaim(Arc<Holder>);

struct Holder {
	/// The claim being competed for; process-wide in production.
	slot: &'static AtomicBool,
	/// Whether this holder is the one that took `slot`. Read and written only
	/// by the task that owns the holder, so it carries no ordering of its own —
	/// the claim itself is published through `slot`.
	held: AtomicBool,
}

impl Drop for Holder {
	fn drop(&mut self) {
		if *self.held.get_mut() {
			self.slot.store(false, Ordering::Release);
		}
	}
}

impl RefreshClaim {
	/// A holder competing for the process-wide claim, not yet holding it.
	pub(crate) fn process() -> Self {
		Self::on(&PROCESS_REFRESH_CLAIMED)
	}

	fn on(slot: &'static AtomicBool) -> Self {
		Self(Arc::new(Holder {
			slot,
			held: AtomicBool::new(false),
		}))
	}

	/// Whether this holder may refresh, taking the claim when it is free.
	///
	/// Call it on every pass: a holder that is refused now takes over once the
	/// current holder is dropped, so the refresh does not stop with whichever
	/// datastore happened to start first.
	pub(crate) fn take(&self) -> bool {
		if self.0.held.load(Ordering::Relaxed) {
			return true;
		}
		let taken =
			self.0.slot.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok();
		if taken {
			self.0.held.store(true, Ordering::Relaxed);
		}
		taken
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	/// The claim admits one holder at a time, keeps it across passes, and hands
	/// it on once that holder is gone.
	///
	/// Competes for a slot of its own rather than the process-wide one, which
	/// every datastore built anywhere in this test binary is also competing for.
	#[test]
	fn claim_admits_one_holder_and_hands_over_on_drop() {
		static SLOT: AtomicBool = AtomicBool::new(false);
		let first = RefreshClaim::on(&SLOT);
		let second = RefreshClaim::on(&SLOT);

		assert!(first.take(), "an unclaimed slot must admit the first holder");
		assert!(!second.take(), "a second holder must be refused while the first holds the claim");
		assert!(first.take(), "the holder keeps its claim across passes");

		// A clone is the same holder, so the claim survives until the last one
		// is dropped — the scheduler hands a clone to every pass.
		let clone = first.clone();
		drop(first);
		assert!(!second.take(), "the claim must outlive a clone of its holder");

		drop(clone);
		assert!(second.take(), "the claim must pass on once its holder is gone");
	}

	#[tokio::test]
	async fn refresh_populates_sync_snapshot_cache() {
		// One refresh cycle must publish into the sync atomics: after the await
		// the synchronous getter has to observe the same memory value the async
		// refresh just returned. This is what the OTel observable-gauge
		// callbacks read on the OTLP push path, and they have no async context
		// of their own — if the hand-off regresses they report nothing.
		//
		// The two samples are compared for equality rather than for growth. The
		// cache is process-wide, so a reading taken before the refresh is not
		// guaranteed to be the zeroed default — anything else in this binary
		// that built a datastore has already populated it — and two real RSS
		// samples are not ordered.
		let refreshed = refresh_process_snapshot().await;
		assert!(refreshed.memory_bytes > 0, "sysinfo failed to read RSS");
		let after = process_snapshot();
		assert_eq!(
			after.memory_bytes, refreshed.memory_bytes,
			"sync cache did not pick up the async refresh result",
		);
	}
}
