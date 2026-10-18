//! Work a layer above the datastore owes once a transaction's outcome is known.
//!
//! Some effects cannot be staged inside the transaction that causes them, because
//! they are not transactional: stopping a process-local index builder, telling a
//! live subscriber its subscription is gone, deleting durable state that a
//! *different* transaction already committed. Run them too early and a rollback
//! leaves the effect behind with nothing to undo it.
//!
//! The transaction is the only thing that knows when its outcome is settled, so it
//! holds the queue; but what to run belongs to the layer that asked. These two
//! traits are that split. Registering is cheap and rare - a DDL statement or a
//! `KILL`, never a row - so the boxing costs nothing that matters.

use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Weak};
use std::time::Duration;

use anyhow::Result;
use tokio::sync::Mutex;
use web_time::Instant;

/// Work owed once a transaction has committed.
///
/// Infallible from the transaction's point of view: the commit has already
/// happened and cannot be taken back, so a failure here is logged rather than
/// surfaced. Discarded when the transaction is cancelled or its commit fails,
/// because the change the action answers to never became durable.
///
/// An action registered inside a save point scope answers to the writes of that scope, so a
/// rollback of the scope discards it ([`Self::answers_to_scope`]).
pub trait CommitAction: Send + Sync + 'static {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>>;

	/// Whether a rollback of the save point scope this action was registered in discards it.
	///
	/// True unless the action is one half of a pair registered on both outcomes
	/// ([`RollbackAction`]), which answers to the end of the transaction rather than to any write:
	/// the rollback half runs only when the transaction does not commit, so the commit half has to
	/// survive a scope's rollback for one half of the pair to run in every case.
	fn answers_to_scope(&self) -> bool {
		true
	}
}

/// Work owed when a transaction does *not* commit.
///
/// Registered by callers that wrote durable state from a separate transaction
/// while this one was still open, so that state is provisional until this one
/// commits. Discarded once the commit succeeds, at which point the state is no
/// longer provisional.
///
/// Fallible, and every registered action is attempted even when one fails: the
/// first error is returned and the rest still run.
///
/// `drain_started_at` is when the transaction began running this queue. An
/// action that has to wait for something — a task to notice it should stop, a
/// lease to lapse — measures its allowance from there rather than from its own
/// start, because the queue is drained one action at a time: a per-action
/// allowance would let a transaction that registered many of them multiply into
/// a close the client reads as a hang.
pub trait RollbackAction: Send + Sync + 'static {
	fn run(
		self: Box<Self>,
		drain_started_at: Instant,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
}

/// Work a statement leaves for the owner of its transaction to await once the
/// transaction has committed.
///
/// For a statement whose result depends on work that must not run inside its
/// transaction: a blocking index build commits writes of its own in unbounded
/// number, and a transaction held open across them ages past what an
/// optimistic storage engine can still validate at commit. Queued only on a
/// transaction whose owner promised to await it (see
/// [`crate::tx::Transaction::with_after_commit_waits`]); a statement on any
/// other transaction does the work inline.
///
/// Fallible, unlike [`CommitAction`]: the failure is the statement's result,
/// reported to the client in place of success. Discarded when the transaction
/// is cancelled or its commit fails, as a commit action is.
///
/// A wait can owe the catalog something once the commit has applied — the
/// removal of a definition whose build failed — so the owner must see the
/// commit and every wait it queued through whatever becomes of its own
/// caller: a caller that stops waiting stops hearing the outcome, not the work.
///
/// `deadline` is the sooner of what is left of the owner's transaction
/// budget and of the query's own deadline: it bounds the wait the way it would
/// have bounded the statement had the work run inline. The wait itself
/// enforces it, rather than the owner dropping it, so that what it owes on a
/// failure still runs when time runs out.
pub trait AfterCommitWait: Send + 'static {
	fn wait(
		self: Box<Self>,
		deadline: Option<AfterCommitDeadline>,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;

	/// Whether the wait can owe the commit something: work that takes back
	/// what the commit applied should the awaited work fail.
	///
	/// An owner must run every such wait once its commit may have applied,
	/// and must not commit at all once its query has ended, since the wait
	/// could then only compensate. A wait that owes nothing — one that only
	/// waits for work the commit left to background maintenance anyway — can
	/// be skipped where nothing hears its outcome, and its owner commits as it
	/// would with nothing queued.
	fn compensates(&self) -> bool {
		true
	}
}

/// The time limit an [`AfterCommitWait`] still answers to.
#[derive(Clone, Copy, Debug)]
pub struct AfterCommitDeadline {
	/// The configured timeout that sets [`Self::at`], which is what a timeout
	/// error reports.
	pub timeout: Duration,
	/// When the wait must have ended.
	pub at: Instant,
}

/// The queue of [`AfterCommitWait`]s for one transaction, owned by the caller
/// that commits it.
///
/// The transaction holds the queue only weakly. What is queued may itself
/// hold the transaction — a statement's context does — so a transaction that
/// owned its queue would keep itself alive whenever an owner abandoned it
/// with work still queued. Dropping the owner's handle drops the queue and
/// everything in it.
#[derive(Clone, Default)]
pub struct AfterCommitWaits(Arc<AfterCommitQueue>);

/// The queue an [`AfterCommitWaits`] shares with its transaction.
pub(crate) type AfterCommitQueue = Mutex<Vec<Box<dyn AfterCommitWait>>>;

impl AfterCommitWaits {
	/// Take what is queued, in the order it was queued.
	///
	/// Called by the owner once its statement has run. Empty once the
	/// transaction was cancelled or its commit failed, because both discard the
	/// queue; an owner that takes the queue before committing runs nothing it
	/// took unless the commit succeeds.
	pub async fn take(&self) -> Vec<Box<dyn AfterCommitWait>> {
		std::mem::take(&mut *self.0.lock().await)
	}

	/// How many waits are queued: an owner that runs several statements in
	/// one transaction reads it before and after each, to tell which statement
	/// queued which wait.
	pub async fn len(&self) -> usize {
		self.0.lock().await.len()
	}

	/// Whether nothing is queued.
	pub async fn is_empty(&self) -> bool {
		self.0.lock().await.is_empty()
	}

	pub(crate) fn downgrade(&self) -> Weak<AfterCommitQueue> {
		Arc::downgrade(&self.0)
	}
}
