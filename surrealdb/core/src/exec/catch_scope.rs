#![cfg_attr(
	not(any(feature = "api", feature = "scripting", feature = "surrealism")),
	allow(dead_code)
)]

//! The savepoint around a unit of work whose failure its caller catches.
//!
//! A statement's error does not always end the transaction it ran in. Some
//! callers turn a failure into a value and go on in the same transaction,
//! which then commits:
//!
//! - a `DEFINE API` stage answers a failure with an error response ([`crate::exec::api`]);
//! - a script catches a rejected `surrealdb.query`, or a rejected builtin that writes, such as
//!   `eval::surql` ([`crate::fnc::script`]);
//! - a Surrealism module is handed the error of a call that writes ([`crate::surrealism`]).
//!
//! Each runs the unit it catches behind a [`CatchScope`], and a unit that
//! fails leaves none of its writes behind, however many it made before
//! failing: records, index entries, what its events wrote, the changefeed and
//! live-query entries describing them, provisional sequence draws, and the
//! commit actions and after-commit waits queued for them, as a savepoint rollback undoes them
//! ([`Transaction::rollback_to_save_point`]). A unit that succeeds keeps them.
//!
//! A unit needs a scope when it may write, or when its transaction has defined
//! a sequence: a unit that only reads may still draw from that sequence, and
//! the draw is provisional, taken back by a savepoint rollback
//! ([`CatchScope::needed`]). A draw from a sequence committed earlier is
//! claimed outside the transaction, which no rollback takes back.
//!
//! A statement does not need one. A statement run in a transaction of its own,
//! or in a `BEGIN` block, cancels that transaction when it fails, and one run in
//! a transaction its client owns refuses that transaction's commit once it
//! fails after writing ([`Transaction::poison_failed_statement`]).
//!
//! An aggregate is not a catch boundary either. It reads a group key or an
//! aggregate's argument whose value fails only about the shape of the data as
//! `NONE`, and goes on, but only for a value whose evaluation ran no nested
//! statement — no subquery, block, `fn::` call, embedded script, module or
//! builtin that runs a statement, however deep beneath a closure, a computed
//! field or a lookup (`ExecutionContext::nested_runs`). Any other value's
//! error fails the statement as it does where nothing catches it. What the
//! aggregate catches ran nothing that could write, so it needs no scope.
//!
//! Savepoints are a stack with no handle, so a caller opens a scope only where
//! no other unit runs on the transaction until the scope closes: an API
//! chain and a statement's expressions run their writes one at a time, a
//! script runs a call that needs a scope alone, and so does a Surrealism
//! module.

use std::sync::Arc;

use anyhow::Result;

use crate::kvs::Transaction;

/// A savepoint opened around a unit whose failure its caller catches; see the
/// module documentation.
///
/// A scope dropped while its savepoint is open, or while its release or
/// rollback is still in progress, refuses its transaction's commit
/// ([`Transaction::poison_abandoned_save_point`]): the savepoint stays on the
/// stack for some other scope to close in place of its own.
#[must_use = "a scope must be ended with `end`"]
pub(crate) struct CatchScope {
	/// The transaction holding the open savepoint, until it is closed. `None`
	/// for a scope that opened none.
	open: Option<Arc<Transaction>>,
}

impl CatchScope {
	/// Whether a unit run on `txn` needs a scope: it may write, or `txn` has
	/// defined a sequence the unit may draw from. A unit run with no
	/// transaction, or with one that cannot be written, has nothing to undo.
	///
	/// A caller that orders its units around their savepoints asks this before
	/// it takes the unit's place, so a unit that opens a scope holds the
	/// exclusive one.
	pub(crate) fn needed(txn: Option<&Arc<Transaction>>, may_write: bool) -> bool {
		txn.is_some_and(|txn| txn.writeable() && (may_write || txn.defines_any_sequence()))
	}

	/// Opens a scope on `txn` for a unit that `may_write`, where one is
	/// [`needed`](Self::needed); otherwise the scope opens no savepoint.
	pub(crate) async fn open(txn: Option<&Arc<Transaction>>, may_write: bool) -> Result<Self> {
		let Some(txn) = txn.filter(|_| Self::needed(txn, may_write)) else {
			return Ok(Self {
				open: None,
			});
		};
		txn.new_save_point().await?;
		Ok(Self {
			open: Some(Arc::clone(txn)),
		})
	}

	/// Closes the scope for a unit that `failed` or did not: the savepoint is
	/// rolled back for a failure and released otherwise.
	///
	/// An error closing it is returned, and the caller reports it in place of
	/// the unit's outcome: the transaction's state is then in doubt, and a
	/// failure the storage raised part-way has already refused its commit.
	pub(crate) async fn end(mut self, failed: bool) -> Result<()> {
		let Some(txn) = self.open.as_ref() else {
			return Ok(());
		};
		let closed = if failed {
			txn.rollback_to_save_point().await
		} else {
			txn.release_last_save_point().await
		};
		self.open = None;
		closed
	}
}

impl Drop for CatchScope {
	fn drop(&mut self) {
		if let Some(txn) = &self.open {
			txn.poison_abandoned_save_point();
		}
	}
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::sync::Arc;

	use surrealdb_kvs::TransactionType::Write;

	use super::CatchScope;
	use crate::kvs::Datastore;

	async fn ds() -> anyhow::Result<Arc<Datastore>> {
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await
	}

	#[tokio::test]
	async fn a_scope_dropped_while_open_refuses_the_commit() {
		let ds = ds().await.unwrap();
		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		let scope = CatchScope::open(Some(&txn), true).await.unwrap();
		drop(scope);
		assert!(txn.commit().await.is_err());
	}

	#[tokio::test]
	async fn a_closed_scope_leaves_the_commit_alone() {
		let ds = ds().await.unwrap();
		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		let kept = CatchScope::open(Some(&txn), true).await.unwrap();
		kept.end(false).await.unwrap();
		let undone = CatchScope::open(Some(&txn), true).await.unwrap();
		undone.end(true).await.unwrap();
		txn.commit().await.unwrap();
	}

	#[tokio::test]
	async fn a_unit_that_only_reads_needs_a_scope_once_its_transaction_defines_a_sequence() {
		let ds = ds().await.unwrap();
		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		assert!(!CatchScope::needed(Some(&txn), false));
		assert!(CatchScope::needed(Some(&txn), true));
		txn.sequence_defined_here(
			crate::catalog::NamespaceId(1),
			crate::catalog::DatabaseId(1),
			"sq".to_owned(),
		)
		.await;
		assert!(CatchScope::needed(Some(&txn), false));
		// The scope it opens is a real one: dropped open, it refuses the commit.
		drop(CatchScope::open(Some(&txn), false).await.unwrap());
		assert!(txn.commit().await.is_err());
		let read = Arc::new(ds.transaction(surrealdb_kvs::TransactionType::Read).await.unwrap());
		assert!(!CatchScope::needed(Some(&read), true));
		assert!(!CatchScope::needed(None, true));
		read.cancel().await.unwrap();
	}

	#[tokio::test]
	async fn a_unit_that_cannot_write_opens_no_savepoint() {
		let ds = ds().await.unwrap();
		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		drop(CatchScope::open(Some(&txn), false).await.unwrap());
		drop(CatchScope::open(None, true).await.unwrap());
		txn.commit().await.unwrap();
	}
}
