//! Wake-ups a committing transaction fires, and the count of its datastore's
//! catalog commits ([`CatalogEpoch`]).
//!
//! The wake-ups are notified after a commit makes the relevant work visible,
//! never before. They are bundled behind one `Arc` rather than carried as separate
//! fields because `TransactionFactory` is held by value inside
//! `IndexBuildReservationRelease`, and `Transaction` is held by value inside
//! large async futures: adding a second pointer to either grew those futures
//! enough to overflow the test stack in debug builds. One `Arc` keeps both
//! types the size they were.

use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::values::graph::GraphFoldScope;

/// How many read-observed hot adjacency scopes are held before further
/// observations are dropped. Dropping is harmless: the scope stays hot, so
/// a later traversal re-observes it, and deletions reach the fold queue
/// through the durable commit-time path instead.
const GRAPH_FOLD_OBSERVATION_CAP: usize = 4096;

/// How many commits of transactions that stamped a catalog version have
/// started and finished through one datastore.
///
/// Where the datastore is the store's sole writer
/// ([`surrealdb_kvs::Transactable::sole_writer`]), the catalog a transaction
/// sees is named by the count of such commits finished before it opened, as
/// long as none was under way when it opened and none has started since
/// ([`crate::tx::Transaction::catalog_epoch`]): every catalog write stamps a
/// version, so a commit that changes the catalog is one of these.
#[derive(Default)]
pub(crate) struct CatalogEpoch {
	started: AtomicU64,
	finished: AtomicU64,
}

impl CatalogEpoch {
	/// The count of finished catalog commits, where none is under way.
	///
	/// `finished` is read first: a commit under way when it is read has
	/// started by the time `started` is, so the two differ.
	pub(crate) fn settled(&self) -> Option<u64> {
		let finished = self.finished.load(Ordering::SeqCst);
		let started = self.started.load(Ordering::SeqCst);
		(started == finished).then_some(finished)
	}

	/// Count a catalog commit as under way until the guard is dropped, which
	/// the caller does once the commit has returned, whether or not it took.
	pub(crate) fn committing(&self) -> CatalogCommit<'_> {
		self.started.fetch_add(1, Ordering::SeqCst);
		CatalogCommit(self)
	}
}

/// A catalog commit under way; see [`CatalogEpoch::committing`].
pub(crate) struct CatalogCommit<'a>(&'a CatalogEpoch);

impl Drop for CatalogCommit<'_> {
	fn drop(&mut self) {
		self.0.finished.fetch_add(1, Ordering::SeqCst);
	}
}

/// The set of post-commit wake-ups shared by a datastore and its transactions.
#[derive(Default)]
pub struct CommitTriggers {
	/// The catalog commits made through the datastore.
	pub(crate) catalog_epoch: CatalogEpoch,
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
