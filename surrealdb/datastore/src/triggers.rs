//! Wake-ups a committing transaction fires.
//!
//! Both are notified after a commit makes the relevant work visible, never
//! before. They are bundled behind one `Arc` rather than carried as separate
//! fields because `TransactionFactory` is held by value inside
//! `IndexBuildReservationRelease`, and `Transaction` is held by value inside
//! large async futures: adding a second pointer to either grew those futures
//! enough to overflow the test stack in debug builds. One `Arc` keeps both
//! types the size they were.

use std::collections::HashSet;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::values::graph::GraphFoldScope;

/// How many read-observed hot adjacency scopes are held before further
/// observations are dropped. Dropping is harmless: the scope stays hot, so
/// a later traversal re-observes it, and deletions reach the fold queue
/// through the durable commit-time path instead.
const GRAPH_FOLD_OBSERVATION_CAP: usize = 4096;

/// The set of post-commit wake-ups shared by a datastore and its transactions.
#[derive(Default)]
pub struct CommitTriggers {
	/// Async event processing, notified when a commit queues events.
	pub async_event: Notify,
	/// Index compaction, notified when a commit queues compaction work so the
	/// compactor runs on the write instead of waiting out its interval.
	pub index_compaction: Notify,
	/// Graph adjacency folding, notified when a commit queues fold work or a
	/// read observes a scope over the fold threshold.
	pub graph_fold: Notify,
	/// Adjacency scopes a read found carrying enough unfolded keys to be
	/// worth packing. In memory because reads cannot write a queue entry;
	/// the fold task drains this set into the durable queue on its pass.
	graph_fold_observations: Mutex<HashSet<GraphFoldScope>>,
}

impl CommitTriggers {
	pub fn new() -> Self {
		Self::default()
	}

	/// Record a hot adjacency scope observed by a read, waking the fold task.
	pub fn observe_graph_fold(&self, scope: GraphFoldScope) {
		{
			let mut observations = self.graph_fold_observations.lock();
			if observations.len() >= GRAPH_FOLD_OBSERVATION_CAP {
				return;
			}
			observations.insert(scope);
		}
		self.graph_fold.notify_one();
	}

	/// Drain the read-observed scopes for the fold task's pass.
	pub fn drain_graph_fold_observations(&self) -> Vec<GraphFoldScope> {
		self.graph_fold_observations.lock().drain().collect()
	}
}
