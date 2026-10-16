//! A blocking index build awaited once the statement that started it has
//! committed, and the compensation for one that fails before its index is
//! online.
//!
//! A standalone blocking `DEFINE INDEX` or `REBUILD INDEX` commits its catalog
//! change first and then waits for its build, so its transaction is not held
//! open across the build's own commits (see
//! [`IndexBuilder::run_awaited_after_commit`]). A new definition published that
//! way can no longer be rolled back, so a build that fails before its index is
//! online has its definition removed again by a transaction of its own.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use anyhow::Result;
use common::time::sleep;
use surrealdb_datastore::close::{AfterCommitDeadline, AfterCommitWait};
use uuid::Uuid;
use web_time::Instant;

#[cfg(doc)]
use super::IndexBuilder;
use super::builder::query_ended;
use super::state::delete_durable_build_artifacts;
use super::{AbortLocalBuild, BuildCompletion, BuildGeneration, IndexBuildPhase};
use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, IndexDefinition, IndexId, NamespaceId, TableDefinition, TableId};
use crate::ctx::FrozenContext;
use crate::ctx::reason::Reason;
use crate::err::EngineError;
use crate::idx::IndexKeyBase;
use crate::kvs::{DatastoreError, Transaction, TransactionType, is_retryable_transaction_conflict};
use crate::val::TableName;

/// A blocking build, awaited once the statement that started it has committed.
///
/// Until the build publishes its index this settles as the statement would
/// have had it awaited the build inline: the query's own deadline and
/// cancellation and the deadline the owner hands the wait still end it with
/// an error. Whatever ends it with an error — those, the build failing, or the
/// build stopping because another statement removed or replaced the index —
/// has `undo` take back what the statement committed, unless a later build of
/// the index has taken it on (see [`RemoveFailedDefinition`]). With no `undo`,
/// the statement is a `REBUILD INDEX`: its build carries on in the background
/// once the statement stops waiting for it, which a timeout or a cancellation
/// reports rather than a statement that did not complete. Once the index is
/// online the statement has succeeded. The index is complete and queryable, so
/// nothing that goes wrong while the build folds the writes queued during it —
/// the build failing (see [`BuildCompletion`]) or the statement running out of
/// time — takes it back.
pub(crate) struct AwaitBuildAfterCommit {
	/// The statement's context, whose deadline and cancellation still apply.
	ctx: FrozenContext,
	completion: BuildCompletion,
	undo: Option<RemoveFailedDefinition>,
}

/// How often a wait after commit rechecks the query's deadline and
/// cancellation, which nothing signals to the build.
const AFTER_COMMIT_DONE_CHECK: Duration = Duration::from_millis(100);

/// Why a wait after commit ended before its build published its index.
enum WaitEnded {
	/// The build failed or was stopped.
	Failed(anyhow::Error),
	/// The query's own deadline ran out, after the given timeout.
	QueryTimedout(Duration),
	/// The deadline the owner handed the wait ran out, after the given timeout.
	DeadlineTimedout(Duration),
	/// The query was cancelled.
	Cancelled,
}

impl std::fmt::Display for WaitEnded {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			WaitEnded::Failed(err) => write!(f, "{err}"),
			WaitEnded::QueryTimedout(d) => write!(f, "the query timed out after {d:?}"),
			WaitEnded::DeadlineTimedout(d) => {
				write!(f, "the transaction timed out after {d:?}")
			}
			WaitEnded::Cancelled => write!(f, "the query was cancelled"),
		}
	}
}

impl WaitEnded {
	/// The error a `DEFINE INDEX` reports, whose definition is taken back: the
	/// statement did not complete.
	fn into_define_error(self) -> anyhow::Error {
		match self {
			WaitEnded::Failed(err) => err,
			WaitEnded::QueryTimedout(d) => EngineError::QueryTimedout(d).into(),
			WaitEnded::DeadlineTimedout(d) => DatastoreError::TransactionTimedout(d.into()).into(),
			WaitEnded::Cancelled => EngineError::QueryCancelled.into(),
		}
	}

	/// The error a `REBUILD INDEX` reports: a timeout or a cancellation leaves
	/// a committed rebuild whose build carries on in the background.
	fn into_rebuild_error(self) -> anyhow::Error {
		match self {
			WaitEnded::Failed(err) => err,
			WaitEnded::QueryTimedout(d) | WaitEnded::DeadlineTimedout(d) => {
				DatastoreError::IndexBuildOutlastedTimeout(d.into()).into()
			}
			WaitEnded::Cancelled => DatastoreError::IndexBuildWaitCancelled.into(),
		}
	}
}

impl AwaitBuildAfterCommit {
	pub(super) fn new(
		ctx: FrozenContext,
		completion: BuildCompletion,
		undo: Option<RemoveFailedDefinition>,
	) -> Self {
		Self {
			ctx,
			completion,
			undo,
		}
	}

	/// Why the query has stopped, if it has: run out of time or cancelled,
	/// never memory pressure (see [`query_ended`]).
	fn query_done(&self) -> Option<WaitEnded> {
		match query_ended(&self.ctx)? {
			Reason::Timedout(d) => Some(WaitEnded::QueryTimedout(d.0)),
			Reason::Canceled => Some(WaitEnded::Cancelled),
		}
	}

	async fn settle(&mut self, deadline: Option<AfterCommitDeadline>) -> Result<(), WaitEnded> {
		loop {
			if let Some(ended) = self.query_done() {
				return self.fail_unless_online(ended);
			}
			let mut slice = match deadline {
				Some(deadline) => {
					let left = deadline.at.saturating_duration_since(Instant::now());
					if left.is_zero() {
						return self
							.fail_unless_online(WaitEnded::DeadlineTimedout(deadline.timeout));
					}
					left.min(AFTER_COMMIT_DONE_CHECK)
				}
				None => AFTER_COMMIT_DONE_CHECK,
			};
			// Wake at the query's deadline rather than up to a slice after it.
			if let Some(left) = self.ctx.timeout() {
				slice = slice.min(left);
			}
			if let Ok(result) = common::time::timeout(slice, &mut self.completion).await {
				if let Err(err) = result {
					// A wait on another node's build ends on the same query
					// deadline and cancellation this wait watches, and reports
					// them as errors of its own. They are this wait ending, not
					// the build failing, and are settled as such.
					if let Some(ended) = self.query_done() {
						return self.fail_unless_online(ended);
					}
					return Err(WaitEnded::Failed(err));
				}
				if self.completion.is_online() {
					return Ok(());
				}
				// A build that ends without publishing its index was stopped:
				// another statement removed or replaced the index while this one
				// waited.
				return Err(self.query_done().unwrap_or_else(|| {
					WaitEnded::Failed(
						DatastoreError::IndexingBuildingCancelled {
							reason: format!(
								"Index {} was removed or replaced before its build finished",
								self.completion.index().name
							),
						}
						.into(),
					)
				}));
			}
		}
	}

	/// End the wait with `ended`, the statement's time running out or its
	/// cancellation, unless the build still running has already published its
	/// index.
	fn fail_unless_online(&self, ended: WaitEnded) -> Result<(), WaitEnded> {
		if self.completion.excused_once_online(&ended) {
			Ok(())
		} else {
			Err(ended)
		}
	}

	/// Settle the wait, and have `undo` take back what the statement committed
	/// when it ends with an error.
	///
	/// An error is the outcome only if the generation the statement awaited is
	/// not online by the durable state, which a publish can reach after this
	/// process stopped looking: the removal reads it under a locked read before
	/// it takes anything back, and a wait with nothing to take back reads it on
	/// its own.
	async fn settle_and_undo(mut self, deadline: Option<AfterCommitDeadline>) -> Result<()> {
		let Err(ended) = self.settle(deadline).await else {
			return Ok(());
		};
		let online = match self.undo.take() {
			Some(undo) => {
				let err = ended.into_define_error();
				match undo.run(self.completion.generation()).await {
					Ok(Undone::Gone) => return Err(err),
					Ok(Undone::Superseded) => {
						let ix = self.completion.index();
						warn!(
							index = %ix.name,
							table = %ix.table_name,
							error = %err,
							"a blocking index build failed after a later build of its index \
							 began; the index stays defined, and that build decides it"
						);
						return Err(err);
					}
					Ok(Undone::Online) => err,
					Err(undo_err) => {
						warn!(
							error = %undo_err,
							"could not remove the definition of an index whose build failed; \
							 the index stays defined with its build state"
						);
						return Err(err);
					}
				}
			}
			None => {
				let err = ended.into_rebuild_error();
				match self.completion.published().await {
					Ok(true) => err,
					Ok(false) => return Err(err),
					Err(read_err) => {
						warn!(
							error = %read_err,
							"could not read back whether an index whose build failed is online"
						);
						return Err(err);
					}
				}
			}
		};
		let ix = self.completion.index();
		warn!(
			index = %ix.name,
			table = %ix.table_name,
			error = %online,
			"a blocking index build its statement saw fail had published its index; the index \
			 is ready"
		);
		Ok(())
	}
}

impl AfterCommitWait for AwaitBuildAfterCommit {
	fn wait(
		self: Box<Self>,
		deadline: Option<AfterCommitDeadline>,
	) -> Pin<Box<dyn Future<Output = Result<()>> + Send>> {
		Box::pin((*self).settle_and_undo(deadline))
	}
}

/// Removes an index definition that committed ahead of its blocking build,
/// once that build has failed without publishing its index.
///
/// Only the definition the statement created is removed — the same index id
/// on the same table — and only while its build state still holds the
/// generation the statement awaited. One that has since been replaced or
/// removed is left alone, since it is no longer the statement's to undo, and
/// so is one a later build has taken on: a `REBUILD INDEX` started once that
/// generation had published or failed builds a generation of its own, and
/// what becomes of the index is that build's to decide. Nor is one whose
/// build published it after all. The statement stops waiting on what its own
/// process saw of the build, and a publish can still land after that: a build
/// outpacing a timeout, a publish whose commit reported an error but applied,
/// or another node taking the build over. The durable build state decides,
/// under a locked read, so a publish or a later build that commits while the
/// removal is staged fails the removal's commit, and the retry reads what it
/// wrote.
pub(crate) struct RemoveFailedDefinition {
	ctx: FrozenContext,
	ns: NamespaceId,
	db: DatabaseId,
	ns_name: String,
	db_name: String,
	tb: TableName,
	table_id: TableId,
	name: String,
	index_id: IndexId,
}

/// How a [`RemoveFailedDefinition`] ended.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Undone {
	/// The statement's definition is gone: removed here, or replaced or
	/// removed since.
	Gone,
	/// A later build of the index has taken it on, so the definition stays,
	/// and what becomes of the index is that build's to decide.
	Superseded,
	/// The build published the index after all, so the definition stays.
	Online,
}

/// What one attempt at a [`RemoveFailedDefinition`] staged in its transaction.
enum Staged {
	/// The removal, which applies if the transaction commits.
	Removal,
	/// Nothing, because the attempt found the removal has nothing to do.
	Nothing(Undone),
}

/// Attempts at the removal, for a removal that keeps losing a retryable
/// conflict to concurrent schema changes on the same table.
const REMOVE_FAILED_DEFINITION_ATTEMPTS: usize = 20;
/// Pause between removal attempts after a retryable conflict.
const REMOVE_FAILED_DEFINITION_RETRY_SLEEP: Duration = Duration::from_millis(50);

impl RemoveFailedDefinition {
	/// The removal of index `ix`, defined on `table` in the namespace and
	/// database named `ns_name` and `db_name`, by the statement running in
	/// `ctx`.
	pub(crate) fn new(
		ctx: FrozenContext,
		(ns_name, db_name): (String, String),
		table: &TableDefinition,
		ix: &IndexDefinition,
	) -> Self {
		Self {
			ctx,
			ns: table.namespace_id,
			db: table.database_id,
			ns_name,
			db_name,
			tb: table.name.clone(),
			table_id: table.table_id,
			name: ix.name.to_string(),
			index_id: ix.index_id,
		}
	}

	/// Remove the definition, if its build state still holds `generation`,
	/// the build generation the statement awaited, unpublished.
	pub(super) async fn run(self, generation: BuildGeneration) -> Result<Undone> {
		let builder = self
			.ctx
			.get_index_builder()
			.ok_or_else(|| EngineError::unreachable("No Index Builder"))?
			.clone();
		let sequences = self.ctx.try_get_sequences()?.clone();
		let mut attempt = 0;
		loop {
			attempt += 1;
			let txn = builder
				.transaction_factory()
				.transaction(TransactionType::Write, sequences.clone())
				.await?;
			let staged = match self.stage(&txn, generation).await {
				Ok(staged) => staged,
				Err(err) => {
					// Staging's error is what the removal reports. The removal
					// has not committed, so a cancel that fails takes back
					// nothing, and its own error would only mask that one.
					let _ = txn.cancel().await;
					return Err(err);
				}
			};
			if let Staged::Nothing(undone) = staged {
				txn.cancel().await?;
				return Ok(undone);
			}
			match txn.commit().await {
				Ok(()) => return Ok(Undone::Gone),
				Err(err)
					if is_retryable_transaction_conflict(&err)
						&& attempt < REMOVE_FAILED_DEFINITION_ATTEMPTS =>
				{
					sleep(REMOVE_FAILED_DEFINITION_RETRY_SLEEP).await;
				}
				Err(err) => return Err(err),
			}
		}
	}

	/// Stage the removal in `txn`, unless there is nothing left for it to do.
	async fn stage(&self, txn: &Transaction, generation: BuildGeneration) -> Result<Staged> {
		let Some(ix) = txn.get_tb_index(self.ns, self.db, &self.tb, &self.name, None).await? else {
			return Ok(Staged::Nothing(Undone::Gone));
		};
		if ix.index_id != self.index_id {
			return Ok(Staged::Nothing(Undone::Gone));
		}
		// Index ids are allocated per table, so a table removed and defined
		// again can hand the same id to an index that is not this one. A table
		// that is gone took its indexes with it.
		let Some(tb) = txn.get_tb(self.ns, self.db, &self.tb, None).await? else {
			return Ok(Staged::Nothing(Undone::Gone));
		};
		if tb.table_id != self.table_id {
			return Ok(Staged::Nothing(Undone::Gone));
		}
		// A locked read, so a publish or a later build that commits after it
		// fails this transaction's commit, and the retry reads what it wrote.
		let ikb = IndexKeyBase::new(self.ns, self.db, self.tb.clone(), self.index_id);
		match txn.getu_key(&ikb.new_bs_key()).await? {
			Some(state) if state.generation != generation => {
				return Ok(Staged::Nothing(Undone::Superseded));
			}
			Some(state) if state.phase == IndexBuildPhase::Online => {
				return Ok(Staged::Nothing(Undone::Online));
			}
			// The awaited generation, unpublished: its build failed, or is
			// still running on another node, and ends with the definition. A
			// definition left with no build state goes too, since it would
			// read as a ready index.
			_ => {}
		}
		remove_index_definition(
			&self.ctx,
			txn,
			(self.ns, self.db),
			(&self.ns_name, &self.db_name),
			&tb,
			&ix,
		)
		.await?;
		Ok(Staged::Removal)
	}
}

/// Remove index definition `ix` of table `tb` in `txn`, as `REMOVE INDEX`
/// does: the process's loaded state evicted and a local build aborted once
/// `txn` commits, the durable build state and its queues deleted, the
/// definition and its id's name lookup deleted and the index's data enqueued
/// for reclaim, the table's shared doc-id space enqueued for reclaim too when
/// this was its last consumer, and the table's index cache stamp refreshed.
///
/// The id pair and the name pair both identify the namespace and database
/// the table lives in. Everything is staged in `txn`, so the removal takes
/// effect only if `txn` commits.
async fn remove_index_definition(
	ctx: &FrozenContext,
	txn: &Transaction,
	(ns, db): (NamespaceId, DatabaseId),
	(ns_name, db_name): (&str, &str),
	tb: &TableDefinition,
	ix: &IndexDefinition,
) -> Result<()> {
	let table_name = &tb.name;
	// Whether this is the table's last index over its shared doc-id space,
	// read before the index goes. The space survives its last index while the
	// table's `graph_doc_ids` marker is set: numeric adjacency blocks still
	// resolve through it.
	let removed_last_doc_id_index = ix.uses_shared_doc_ids()
		&& !tb.graph_doc_ids
		&& !txn
			.all_tb_indexes(ns, db, table_name, None)
			.await?
			.iter()
			.any(|other| other.index_id != ix.index_id && other.uses_shared_doc_ids());
	// The builder task is process memory, so it is aborted only once the
	// removal commits.
	ctx.get_index_stores().index_removed(ns, db, tb, ix).await?;
	if let Some(index_builder) = ctx.get_index_builder() {
		txn.on_commit(AbortLocalBuild::boxed(
			index_builder.clone(),
			ns,
			db,
			table_name.clone(),
			ix.index_id,
		))
		.await;
	}
	let ikb = IndexKeyBase::new(ns, db, table_name.clone(), ix.index_id);
	delete_durable_build_artifacts(txn, &ikb, false).await?;
	txn.del_tb_index_deferred(ns, db, table_name, &ix.name).await?;
	if removed_last_doc_id_index {
		txn.del_tb_doc_ids_deferred(ns, db, table_name).await?;
	}
	// The locked read `replace_tb` makes of the table definition serializes
	// this removal against every other DDL on the table, including a
	// concurrent removal deciding the last-consumer question above.
	txn.replace_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_indexes_ts: Uuid::now_v7(),
			..tb.clone()
		},
	)
	.await?;
	txn.clear_cache();
	Ok(())
}
