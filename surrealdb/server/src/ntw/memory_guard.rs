//! The transport-level memory guard and the latch `/health` reads.
//!
//! Every RPC transport asks [`admit`] before dispatching a request. When
//! tracked memory is beyond `SURREAL_MEMORY_THRESHOLD`, `admit` counts the
//! refusal (see [`surrealdb_core::mem::record_refusal`]), logs it, and returns
//! the figures so the transport can refuse the request.
//!
//! Each answer `admit` gives is also an observation of the guard's state,
//! which [`LATCH`] records: the latch is *engaged* from the first observation
//! beyond the threshold until the next observation below it. `/health` reads
//! how long the latch has been engaged and fails once that reaches
//! `SURREAL_MEMORY_THRESHOLD_UNHEALTHY_AFTER` (see
//! [`crate::cnf::MEMORY_THRESHOLD_UNHEALTHY_AFTER`]). Memory held by caches
//! that are never released keeps tracked memory beyond the threshold
//! indefinitely, so a process whose guard stays engaged refuses every request
//! until it restarts; failing `/health` lets a liveness probe do that restart.
//!
//! The health probe observes the guard too, so the latch follows the real
//! state even when no requests arrive, and any single observation below the
//! threshold, from a request or a probe, disengages it.

use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use surrealdb_core::mem::{ALLOC, RefusalSite, record_refusal};
use web_time::Instant;

/// The process-wide latch fed by [`admit`] and read by `/health`.
pub(crate) static LATCH: LazyLock<Latch> = LazyLock::new(|| Latch::new(Instant::now()));

/// Decide whether a request arriving on `site` may be dispatched.
///
/// Returns `None` when it may. Returns `Some((allocated, threshold))` when
/// tracked memory is beyond the threshold, after counting and logging the
/// refusal; the caller must then refuse the request.
///
/// A refusal short-circuits before dispatch, so no statement or RPC metric
/// moves. The log line carries the transport and both byte counts, taken from
/// the check that refused, which is what tells "the threshold is set too low"
/// apart from "this instance is genuinely out of memory".
pub(crate) fn admit(site: RefusalSite) -> Option<(usize, usize)> {
	let over = ALLOC.beyond_threshold_by();
	LATCH.observe(over.is_some(), Instant::now());
	if let Some((allocated, threshold)) = over {
		record_refusal(site);
		warn!(
			transport = site.as_label(),
			memory_allocated = allocated,
			memory_threshold = threshold,
			"Refusing request: tracked memory is beyond SURREAL_MEMORY_THRESHOLD"
		);
	}
	over
}

/// Observe the guard without refusing anything, and return how long it has
/// been engaged, or `None` when it is not engaged.
pub(crate) fn probe() -> Option<Duration> {
	let now = Instant::now();
	LATCH.observe(ALLOC.is_beyond_threshold(), now);
	LATCH.engaged_for(now)
}

/// Tracks how long the memory guard has been engaged without a break.
///
/// Lock-free so that observing it on every request costs one relaxed load
/// while the guard is disengaged.
pub(crate) struct Latch {
	/// The instant offsets are measured from.
	origin: Instant,
	/// `0` while disengaged; otherwise one more than the number of nanoseconds
	/// between [`Self::origin`] and the first observation of this engagement.
	since: AtomicU64,
}

impl Latch {
	pub(crate) fn new(origin: Instant) -> Self {
		Self {
			origin,
			since: AtomicU64::new(0),
		}
	}

	/// Record one observation of the guard, taken at `now`.
	///
	/// An observation beyond the threshold engages the latch if it is not
	/// already engaged, and leaves the start of an existing engagement
	/// untouched. An observation below the threshold disengages it, unless
	/// the current engagement started after `now`: observations race, and one
	/// taken before an engagement began says nothing about that engagement.
	pub(crate) fn observe(&self, beyond: bool, now: Instant) {
		let stamp =
			(now.saturating_duration_since(self.origin).as_nanos() as u64).saturating_add(1);
		if beyond {
			// Losing the race means another observation engaged it first,
			// which is the earlier start this engagement should keep.
			let _ = self.since.compare_exchange(0, stamp, Ordering::Relaxed, Ordering::Relaxed);
			return;
		}
		let mut current = self.since.load(Ordering::Relaxed);
		// Clear only an engagement that began at or before this observation,
		// and only if it is still the one that was read: a newer engagement
		// installed in between must survive.
		while current != 0 && current <= stamp {
			match self.since.compare_exchange_weak(current, 0, Ordering::Relaxed, Ordering::Relaxed)
			{
				Ok(_) => return,
				Err(actual) => current = actual,
			}
		}
	}

	/// How long the latch has been engaged at `now`, or `None` when it is not.
	pub(crate) fn engaged_for(&self, now: Instant) -> Option<Duration> {
		match self.since.load(Ordering::Relaxed) {
			0 => None,
			stamp => {
				let started = self.origin + Duration::from_nanos(stamp - 1);
				Some(now.saturating_duration_since(started))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn secs(n: u64) -> Duration {
		Duration::from_secs(n)
	}

	#[test]
	fn a_fresh_latch_is_disengaged() {
		let origin = Instant::now();
		let latch = Latch::new(origin);
		assert_eq!(latch.engaged_for(origin + secs(100)), None);
	}

	#[test]
	fn engagement_is_measured_from_the_first_observation_beyond() {
		let origin = Instant::now();
		let latch = Latch::new(origin);
		latch.observe(true, origin + secs(10));
		latch.observe(true, origin + secs(40));
		assert_eq!(latch.engaged_for(origin + secs(70)), Some(secs(60)));
	}

	#[test]
	fn one_observation_below_disengages_and_restarts_the_clock() {
		let origin = Instant::now();
		let latch = Latch::new(origin);
		latch.observe(true, origin + secs(10));
		latch.observe(false, origin + secs(50));
		assert_eq!(latch.engaged_for(origin + secs(55)), None);
		latch.observe(true, origin + secs(60));
		assert_eq!(latch.engaged_for(origin + secs(90)), Some(secs(30)));
	}

	/// An observation below the threshold that was taken before the current
	/// engagement began, and arrives late, leaves that engagement in place.
	#[test]
	fn a_stale_observation_below_does_not_clear_a_newer_engagement() {
		let origin = Instant::now();
		let latch = Latch::new(origin);
		latch.observe(true, origin + secs(20));
		latch.observe(false, origin + secs(10));
		assert_eq!(latch.engaged_for(origin + secs(80)), Some(secs(60)));
	}

	/// Many threads observing at once, every one of them beyond the
	/// threshold, engage the latch once, from one of their own stamps.
	#[test]
	fn concurrent_observations_beyond_engage_once() {
		let origin = Instant::now();
		let latch = Latch::new(origin);
		std::thread::scope(|scope| {
			for i in 1..=16u64 {
				let latch = &latch;
				scope.spawn(move || latch.observe(true, origin + secs(i)));
			}
		});
		let engaged = latch.engaged_for(origin + secs(100)).expect("engaged");
		assert!(engaged >= secs(84) && engaged <= secs(99), "{engaged:?}");
	}

	#[test]
	fn engaging_at_the_origin_is_distinguishable_from_disengaged() {
		let origin = Instant::now();
		let latch = Latch::new(origin);
		latch.observe(true, origin);
		assert_eq!(latch.engaged_for(origin + secs(5)), Some(secs(5)));
	}

	/// Without allocation tracking, or with no threshold configured, nothing
	/// is ever beyond the threshold: requests are admitted and the probe
	/// reports the guard disengaged.
	#[test]
	fn an_unconfigured_guard_admits_everything() {
		if surrealdb_core::mem::memory_threshold() != 0 {
			return;
		}
		assert_eq!(admit(RefusalSite::Http), None);
		assert_eq!(probe(), None);
	}
}
