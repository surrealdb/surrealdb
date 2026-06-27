//! Memory backpressure valve for indexed writes.
//!
//! Under sustained indexed-write load the un-compacted index-delta backlog can outrun the single
//! background index compactor and drive resident memory to OOM: reads/queries then scan the whole
//! set of pending updates, so RSS grows proportional to the *write backlog* rather than the dataset
//! (see surrealdb#7337). When resident memory exceeds `SURREAL_RSS_BACKPRESSURE_BYTES`, indexed
//! writes are paced here so the compactor gets time to drain instead of the process OOM-killing.
//!
//! Fail-open and zero-overhead by default: an unset/zero ceiling, or an unreadable `/proc`, results
//! in no delay. Linux only (the ceiling is the cgroup/container RSS in practice).
use std::sync::LazyLock;
use std::time::Duration;

use tokio::time::sleep;

/// Resident-memory ceiling in bytes above which indexed writes are paced. `0` (default) disables.
static RSS_BACKPRESSURE_BYTES: LazyLock<u64> =
	LazyLock::new(|| std::env::var("SURREAL_RSS_BACKPRESSURE_BYTES").ok().and_then(|s| s.trim().parse().ok()).unwrap_or(0));

/// How long (milliseconds) to pace a single record's indexed writes while over the ceiling.
static RSS_BACKPRESSURE_SLEEP_MS: LazyLock<u64> =
	LazyLock::new(|| std::env::var("SURREAL_RSS_BACKPRESSURE_SLEEP_MS").ok().and_then(|s| s.trim().parse().ok()).unwrap_or(50));

#[cfg(target_os = "linux")]
fn resident_bytes() -> u64 {
	// /proc/self/statm fields (in pages): size resident shared text lib data dt.
	// Field index 1 is the resident set size.
	std::fs::read_to_string("/proc/self/statm")
		.ok()
		.and_then(|s| s.split_whitespace().nth(1).and_then(|p| p.parse::<u64>().ok()))
		.map(|pages| pages.saturating_mul(4096))
		.unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn resident_bytes() -> u64 {
	0
}

/// Pace a record's indexed writes when resident memory is over the configured ceiling, giving the
/// index compactor time to drain the pending-delta backlog. Bounded, fail-open, and a no-op unless
/// `SURREAL_RSS_BACKPRESSURE_BYTES` is set.
pub(crate) async fn pace_indexed_write() {
	let ceiling = *RSS_BACKPRESSURE_BYTES;
	if ceiling == 0 {
		return;
	}
	if resident_bytes() > ceiling {
		sleep(Duration::from_millis(*RSS_BACKPRESSURE_SLEEP_MS)).await;
	}
}
