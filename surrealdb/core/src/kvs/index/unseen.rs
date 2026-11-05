//! The index a transaction defined, and what it may do with it before it
//! commits.
//!
//! A `DEFINE INDEX` builds in transactions of its own, so the transaction that
//! ran it holds a snapshot that predates what the build wrote, or is ahead of a
//! build that has not started (see [`surrealdb_datastore::unseen_indexes`]).
//! Such an index is recorded on that transaction, and then:
//!
//! - nothing the transaction reads uses it, since it answers for the table as the build saw it, not
//!   as the transaction does (the planner's filter,
//!   [`surrealdb_datastore::index_state::filter_online_indexes`]);
//! - a write that must change an entry the build wrote is refused where it is made, because the
//!   transaction cannot read that entry, and writing its key fails the commit against the build's
//!   own write however often it is retried ([`judge_write`]);
//! - a write that adds a `UNIQUE` entry is checked against the entries the build wrote, which the
//!   transaction's own read of the key cannot show (`Transaction::with_unseen_index`, read by the
//!   `UNIQUE` maintainer).
//!
//! A write that only adds entries conflicts with nothing the build wrote, and
//! a `CONCURRENTLY` build queues the transaction's writes for itself, so
//! neither is restricted.

use std::collections::HashSet;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_datastore::unseen_indexes::Unseen;

use super::{IndexBuilder, IndexMutation};
use crate::catalog::{
	DatabaseDefinition, DatabaseId, Index, IndexDefinition, NamespaceId, TableId,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::idx::Error as IdxError;
use crate::kvs::{Transaction, TransactionType};

impl IndexBuilder {
	/// Record, on `txn`, what it cannot read of the index `ix` of `table` that
	/// it has just defined and built, awaited when `blocking`.
	///
	/// A `CONCURRENTLY` build has not read anything: it waits for `txn` to
	/// close, so `txn` reads the index empty however much the table holds. A
	/// blocking build wrote entries `txn`'s snapshot predates, unless the
	/// store held no record of the table and no entry of the index for it to
	/// touch; `txn` then reads the index as its snapshot has it. For a fresh
	/// blocking definition, any records `txn` created before `DEFINE INDEX`
	/// (absent from the committed store) are indexed directly into `txn` here
	/// and recorded in [`Unseen::Built::created`] so later writes in `txn` can
	/// update or delete them.
	///
	/// The store is read through a view opened after the build, which says
	/// whether it held a record or index entry and is kept only for a `unique`
	/// index, whose writes check their keys through it ([`Unseen::Built`]).
	///
	/// The future is built here and returned boxed, so that a caller on a deep
	/// path holds a pointer to it, not its size.
	#[inline(never)]
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn note_unseen_definition<'a>(
		&'a self,
		ctx: &'a FrozenContext,
		opt: &'a Options,
		txn: &'a Transaction,
		at: (NamespaceId, DatabaseId, TableId),
		ix: &'a Arc<IndexDefinition>,
		fresh: bool,
		blocking: bool,
	) -> Pin<Box<impl Future<Output = Result<()>> + 'a>> {
		Box::pin(self.note_unseen_definition_body(ctx, opt, txn, at, ix, fresh, blocking))
	}

	#[expect(clippy::too_many_arguments)]
	async fn note_unseen_definition_body(
		&self,
		ctx: &FrozenContext,
		opt: &Options,
		txn: &Transaction,
		(ns, db, tb): (NamespaceId, DatabaseId, TableId),
		ix: &Arc<IndexDefinition>,
		fresh: bool,
		blocking: bool,
	) -> Result<()> {
		let table = &ix.table_name;
		let ix_id = ix.index_id;
		let unique = matches!(ix.index, Index::Uniq);
		// An id this transaction drew again, after a save point rolled back the
		// definition that held it, is judged afresh.
		if let Some(stale) = txn.forget_index_unseen(ns, db, table, ix_id) {
			stale.cancel_bare().await?;
		}
		if !blocking {
			txn.mark_index_unseen(ns, db, table, ix_id, Unseen::Queued);
			return Ok(());
		}
		let view = Arc::new(self.tf.transaction(TransactionType::Read, txn.sequences()).await?);
		let res: Result<bool> = async {
			let _ = (ctx, opt, tb);
			let holds_record = view.holds_record(ns, db, table).await?;
			let holds_index = !fresh && view.holds_index(ns, db, table, ix_id).await?;
			if !holds_record && !holds_index {
				return Ok(false);
			}
			let kept_view = if unique {
				Some(Arc::clone(&view))
			} else {
				None
			};
			txn.mark_index_unseen(
				ns,
				db,
				table,
				ix_id,
				Unseen::Built {
					created: Arc::new(HashSet::new()),
					view: kept_view,
				},
			);
			Ok(unique)
		}
		.await;
		match res {
			Ok(true) => Ok(()),
			Ok(false) => {
				view.cancel_bare().await?;
				Ok(())
			}
			Err(err) => {
				// The statement fails with the error, and the view is not
				// left open behind it.
				if let Err(close_err) = view.cancel_bare().await {
					tracing::warn!(
						target: "surrealdb::core::kvs::index",
						"closing the view of an index failed after its probe failed; preserving the probe's error {err}: {close_err}"
					);
				}
				Err(err)
			}
		}
	}
}

/// Whether the transaction that defined `ix` may make the write `mutation`
/// describes, which changes what the index holds for one record.
///
/// A record the transaction created after the definition holds what it wrote
/// itself, which it reads and may change. Every other record of the table was
/// there when the index was built, and the build wrote for it whatever its
/// values were: an entry, a document mapping, a pending vector update. Changing
/// any of that fails the commit against the build's own write, so the one write
/// the transaction can make to such a record is none. A `COUNT` index keeps
/// deltas, never anything of a record, so none of its writes touches what the
/// build wrote.
///
/// A record the transaction wrote before the definition is neither kind: the
/// build read the table as last committed, so it never saw the record and wrote
/// nothing for it. Nothing here tells it from a record the build indexed, so it
/// is judged as one and its change is refused, naming an entry that is not
/// there. A definition over a table the transaction has written records of
/// builds an index that does not hold them; this check, which judges the writes
/// that follow a definition, does not make that definition sound.
///
/// A record stays remembered as the transaction's own until the transaction
/// ends, deleted or not: the registry does not roll back with the storage, and a
/// save point rollback brings back a record the transaction deleted. An id the
/// transaction wrote is never a record the build wrote, so remembering one the
/// transaction no longer has answers for nothing.
///
/// Answers `Ok` for an index the transaction can read.
pub(super) fn judge_write(
	tx: &Transaction,
	db: &DatabaseDefinition,
	ix: &IndexDefinition,
	mutation: &IndexMutation<'_>,
) -> Result<()> {
	// A speculation judges against its own copy of the registry, which ends
	// with it, so what it remembers never reaches its transaction's.
	let judged = tx.with_unseen_index(
		db.namespace_id,
		db.database_id,
		&ix.table_name,
		ix.index_id,
		|unseen| {
			let Unseen::Built {
				created,
				..
			} = unseen
			else {
				return Ok(());
			};
			if matches!(ix.index, Index::Count(_)) {
				return Ok(());
			}
			let rid = mutation.rid;
			if mutation.old_values.is_none() {
				Arc::make_mut(created).insert(rid.clone());
				return Ok(());
			}
			if created.contains(rid) {
				return Ok(());
			}
			Err(IdxError::EntryOfUnseenBuild {
				index: ix.name.to_string(),
				record: rid.clone(),
			})
		},
	);
	match judged {
		Some(Err(refusal)) => Err(refusal.into()),
		Some(Ok(())) | None => Ok(()),
	}
}
