//! Wake-ups a committing transaction fires.
//!
//! Both are notified after a commit makes the relevant work visible, never
//! before. They are bundled behind one `Arc` rather than carried as separate
//! fields because `TransactionFactory` is held by value inside
//! `IndexBuildReservationRelease`, and `Transaction` is held by value inside
//! large async futures: adding a second pointer to either grew those futures
//! enough to overflow the test stack in debug builds. One `Arc` keeps both
//! types the size they were.

use tokio::sync::Notify;

/// The set of post-commit wake-ups shared by a datastore and its transactions.
#[derive(Default)]
pub struct CommitTriggers {
	/// Async event processing, notified when a commit queues events.
	pub async_event: Notify,
	/// Index compaction, notified when a commit queues compaction work so the
	/// compactor runs on the write instead of waiting out its interval.
	pub index_compaction: Notify,
}

impl CommitTriggers {
	pub fn new() -> Self {
		Self::default()
	}
}
