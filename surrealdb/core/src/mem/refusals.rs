//! Process-wide counts of work refused by the memory guard.
//!
//! Every place that turns work away because tracked memory is beyond
//! `SURREAL_MEMORY_THRESHOLD` calls [`record_refusal`] with the
//! [`RefusalSite`] it guards. The counts are monotonic for the life of the
//! process and are read by the server's metrics layer, which publishes them as
//! the `surrealdb.memory_guard.refusals` counter with a `site` attribute.
//!
//! The counters live here rather than in the metrics layer so that refusals
//! raised inside query execution, which has no access to the server's
//! instruments, are counted alongside the ones raised at the transport.

use std::sync::atomic::{AtomicU64, Ordering};

/// Where the memory guard refused work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalSite {
	/// A WebSocket RPC message, refused before dispatch.
	Websocket,
	/// An HTTP `/rpc` request, refused before dispatch.
	Http,
	/// A running query, failed with `QueryBeyondMemoryThreshold` part way
	/// through execution.
	Query,
}

impl RefusalSite {
	/// Every site, in a fixed order.
	pub const ALL: [RefusalSite; 3] =
		[RefusalSite::Websocket, RefusalSite::Http, RefusalSite::Query];

	/// The stable label published as the metric's `site` attribute.
	pub const fn as_label(self) -> &'static str {
		match self {
			RefusalSite::Websocket => "websocket",
			RefusalSite::Http => "http",
			RefusalSite::Query => "query",
		}
	}

	const fn index(self) -> usize {
		match self {
			RefusalSite::Websocket => 0,
			RefusalSite::Http => 1,
			RefusalSite::Query => 2,
		}
	}
}

static REFUSALS: [AtomicU64; RefusalSite::ALL.len()] =
	[const { AtomicU64::new(0) }; RefusalSite::ALL.len()];

// Every site indexes its own counter.
const _: () = {
	let mut i = 0;
	while i < RefusalSite::ALL.len() {
		assert!(RefusalSite::ALL[i].index() == i);
		i += 1;
	}
};

/// Count one refusal at `site`.
///
/// Called only on the refusal path, so it adds nothing to work the guard lets
/// through.
#[inline]
pub fn record_refusal(site: RefusalSite) {
	REFUSALS[site.index()].fetch_add(1, Ordering::Relaxed);
}

/// The number of refusals counted at `site` since the process started.
pub fn refusal_count(site: RefusalSite) -> u64 {
	REFUSALS[site.index()].load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn each_site_counts_independently() {
		// The counters are process-wide and other tests may refuse work
		// concurrently, so assert on deltas of at least what this test adds.
		let before = RefusalSite::ALL.map(refusal_count);
		record_refusal(RefusalSite::Http);
		record_refusal(RefusalSite::Http);
		record_refusal(RefusalSite::Query);
		let after = RefusalSite::ALL.map(refusal_count);
		assert!(after[RefusalSite::Http.index()] >= before[RefusalSite::Http.index()] + 2);
		assert!(after[RefusalSite::Query.index()] > before[RefusalSite::Query.index()]);
	}

	#[test]
	fn labels_are_stable() {
		let labels = RefusalSite::ALL.map(RefusalSite::as_label);
		assert_eq!(labels, ["websocket", "http", "query"]);
	}
}
