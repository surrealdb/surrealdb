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

use anyhow::Result;

/// Work owed once a transaction has committed.
///
/// Infallible from the transaction's point of view: the commit has already
/// happened and cannot be taken back, so a failure here is logged rather than
/// surfaced. Discarded when the transaction is cancelled or its commit fails,
/// because the change the action answers to never became durable.
pub trait CommitAction: Send + Sync + 'static {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = ()> + Send>>;
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
pub trait RollbackAction: Send + Sync + 'static {
	fn run(self: Box<Self>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>;
}
