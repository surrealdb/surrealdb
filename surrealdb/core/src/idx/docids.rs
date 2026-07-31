//! Table-level document-ID (surrogate) space shared by all indexes on a table.
//!
//! Every index family used to own a *private* compact-ID space: full-text search
//! allocated per-index `DocId`s, HNSW and DiskANN each kept their own allocator
//! plus a recycle pool. The same record therefore had a different `u64` identity
//! in every index, which makes cross-index candidate composition impossible
//! without falling back to full [`RecordId`](crate::val::RecordId) comparison.
//!
//! [`TableDocIds`] replaces those private spaces with **one monotonic `u64`
//! space per table**, shared by every index on that table. It maintains the two
//! table-scoped mappings
//!
//! - `/*{ns}*{db}*{tb}!di{record_id}` → [`DocId`] (see [`crate::key::table::di`]),
//! - `/*{ns}*{db}*{tb}!dd{doc_id}` → [`RecordIdKey`] (see [`crate::key::table::dd`]),
//!
//! and allocates fresh ids through the distributed-sequence batching engine
//! ([`crate::kvs::sequences`], the `TableDocIds` domain).
//!
//! # Invariants
//!
//! - **One id per record, shared across indexes.** [`resolve_or_assign`] is idempotent: the first
//!   index to touch a record assigns the id and commits the mapping; every other index (and any
//!   later compaction) reads the same committed id. Assignment is therefore order-independent
//!   across indexes.
//! - **Monotonic, never recycled.** [`remove`] deletes the mapping but never returns the id to a
//!   pool. A delete followed by a re-create of the same record key yields a *new* id, which keeps
//!   bitmap/cache coherence intact (stale cache entries are rejected by generation checks). `u64`
//!   does not exhaust in practice.
//! - **Removal is centralised.** Because the space is shared, no single index may drop a record's
//!   mapping — a sibling index (or a pending vector compaction) might still reference the id. The
//!   mapping is removed once, at record purge, after every index has released the record.
//!
//! [`resolve_or_assign`]: TableDocIds::resolve_or_assign
//! [`remove`]: TableDocIds::remove
//! [`RecordIdKey`]: crate::val::RecordIdKey

use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::FrozenContext;
use crate::err::Error;
use crate::key::schema::{
	DocKeyKey, DocKeyPrefix, DocLookupKey, DocLookupPrefix, DocPendingPrefix,
};
use crate::kvs::{Error as KvsError, Transaction};
use crate::val::{RecordIdKey, TableName};

/// A compact, internal document identifier for a record within a table's shared
/// doc-ID space. Allocated monotonically and never reused.
pub type DocId = u64;

/// The table-level document-ID allocator and record ↔ doc-ID mapping store.
///
/// Cheap to construct (holds only the table scope); all state lives in the
/// key-value store and the distributed sequence, so instances are disposable and
/// carry no in-memory allocation cursor.
pub(crate) struct TableDocIds {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
}

impl TableDocIds {
	/// Creates a doc-ID store for the given table scope.
	pub(crate) fn new(ns: NamespaceId, db: DatabaseId, tb: TableName) -> Self {
		Self {
			ns,
			db,
			tb,
		}
	}

	/// Returns the doc-ID mapped to `id`, or `None` if the record has no id yet.
	pub(crate) async fn get_doc_id(
		&self,
		tx: &Transaction,
		id: &RecordIdKey,
	) -> Result<Option<DocId>> {
		let key = DocLookupKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), Cow::Borrowed(id));
		tx.get_key(&key, None).await
	}

	/// Returns the existing doc-ID for `id`, or allocates, persists, and returns a
	/// new one.
	///
	/// Idempotent: concurrent callers on different indexes converge on the same id
	/// because the `!di` mapping is read first and only a missing mapping triggers
	/// a sequence allocation. The new id is drawn from the table-scoped
	/// distributed sequence (batched via `config.idx.table_doc_ids_batch_size`) and
	/// both the forward (`!di`) and reverse (`!dd`) mappings are written.
	///
	/// The forward mapping is claimed with a conditional create (`putc`) rather
	/// than a blind `set`. When two doc-ID-consuming indexes first touch the same
	/// record concurrently (e.g. concurrent `DEFINE`/`REBUILD` of full-text /
	/// HNSW / DiskANN), each observes no `!di` and allocates a *different* id from
	/// the shared sequence. On last-writer-wins backends (TiKV) a blind `set`
	/// would let the later writer overwrite the forward mapping — leaving one
	/// record with two doc-IDs and breaking the cross-index identity scans rely
	/// on. `putc` reads `!di` before writing, which arms the write-conflict check,
	/// so only one writer wins; the loser's commit is rejected and its caller
	/// retries, at which point the fast path returns the committed id (making the
	/// operation idempotent). The abandoned speculative id is never reused, which
	/// is fine — the space is monotonic and `u64` does not exhaust (see the module
	/// invariants).
	pub(crate) async fn resolve_or_assign(
		&self,
		ctx: &FrozenContext,
		id: &RecordIdKey,
	) -> Result<DocId> {
		let tx = ctx.tx();
		let di = DocLookupKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), Cow::Borrowed(id));
		// Fast path: the record already has an id in this table's space.
		if let Some(doc_id) = tx.get_key(&di, None).await? {
			return Ok(doc_id);
		}
		// Allocate a fresh, monotonic id from the table-scoped sequence.
		let doc_id = ctx
			.try_get_sequences()?
			.next_table_doc_id(
				Some(ctx),
				self.ns,
				self.db,
				self.tb.clone(),
				ctx.config.idx.table_doc_ids_batch_size,
			)
			.await?;
		// Claim the forward mapping. `putc` with no expected value only writes if
		// `!di` is still absent, and reading it arms the write-conflict check.
		match tx.put_key(&di, &doc_id).await {
			Ok(()) => {
				// We won the race: publish the reverse mapping too. Written only
				// after the forward claim succeeds, so a loser leaves no dangling
				// `!dd` entry to clean up.
				let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
				tx.set_key(&dd, id).await?;
				Ok(doc_id)
			}
			// Another index published the mapping first and it is already visible
			// in our snapshot: adopt the committed id and drop our speculative
			// one. (Under snapshot isolation the primary defence is the
			// commit-time conflict above plus the caller's retry; this branch
			// covers the mapping becoming visible between our two reads.)
			Err(e)
				if matches!(
					e.downcast_ref::<Error>(),
					Some(Error::Kvs(KvsError::TransactionKeyAlreadyExists))
				) =>
			{
				tx.get_key(&di, None).await?.ok_or_else(|| {
					anyhow::anyhow!("doc-ID mapping missing after a conditional-create conflict")
				})
			}
			Err(e) => Err(e),
		}
	}

	/// Returns the record key mapped to `doc_id`, or `None` if unmapped.
	pub(crate) async fn get_record_id(
		&self,
		tx: &Transaction,
		doc_id: DocId,
	) -> Result<Option<RecordIdKey>> {
		let key = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
		tx.get_key(&key, None).await
	}

	/// Resolves many doc-IDs to their record keys in one batched read, preserving
	/// input order. Unmapped ids yield `None` in their slot.
	pub(crate) async fn get_record_ids_batch(
		&self,
		tx: &Transaction,
		doc_ids: &[DocId],
	) -> Result<Vec<Option<RecordIdKey>>> {
		let keys: Vec<DocKeyKey<'_>> = doc_ids
			.iter()
			.map(|&doc_id| DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id))
			.collect();
		tx.get_many_key(keys, None).await
	}

	/// Removes both directions of the mapping for `id`. The id is *not* recycled.
	///
	/// Must only be called once a record is being purged and every index has
	/// released it (see the module invariants).
	///
	/// Returns the doc-ID the record was mapped to, or `None` if there was no
	/// mapping (already reclaimed) — the deferred-reclaim sweep uses it to
	/// restore the mapping of a record re-created concurrently with the sweep.
	pub(crate) async fn remove(&self, tx: &Transaction, id: &RecordIdKey) -> Result<Option<DocId>> {
		let di = DocLookupKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), Cow::Borrowed(id));
		if let Some(doc_id) = tx.get_key(&di, None).await? {
			tx.del_key(&di).await?;
			let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
			tx.del_key(&dd).await?;
			Ok(Some(doc_id))
		} else {
			Ok(None)
		}
	}

	/// Restores the mapping `id ↔ doc_id`, unless the record has already been
	/// handed a fresh id.
	///
	/// Used by the deferred-reclaim sweep to repair the narrow race where a
	/// record is re-created (adopting its still-live id via the
	/// [`resolve_or_assign`](Self::resolve_or_assign) fast path) concurrently
	/// with the sweep reclaiming that id: the re-created record's index entries
	/// reference `doc_id`, so the mapping must be put back. The forward mapping
	/// is written with a put-if-absent claim, so if the record was instead
	/// re-indexed with a freshly allocated id after the reclaim, the restore
	/// yields to it and returns `false`.
	pub(crate) async fn restore(
		&self,
		tx: &Transaction,
		id: &RecordIdKey,
		doc_id: DocId,
	) -> Result<bool> {
		let di = DocLookupKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), Cow::Borrowed(id));
		match tx.put_key(&di, &doc_id).await {
			Ok(()) => {
				let dd = DocKeyKey::new(self.ns, self.db, Cow::Borrowed(&self.tb), doc_id);
				tx.set_key(&dd, id).await?;
				Ok(true)
			}
			// The record already re-acquired a (fresh) id: leave it in place.
			Err(e)
				if matches!(
					e.downcast_ref::<Error>(),
					Some(Error::Kvs(KvsError::TransactionKeyAlreadyExists))
				) =>
			{
				Ok(false)
			}
			Err(e) => Err(e),
		}
	}

	/// Removes **all** doc-ID state for the table (`!di`/`!dd` mappings and
	/// `!dp` pending-reclaim markers) in three prefix deletes, reclaiming the
	/// shared space.
	///
	/// Intended for when the table's last doc-ID-consuming index is dropped:
	/// with no consumer left, a record delete's central [`remove`](Self::remove)
	/// no longer fires (see [`crate::doc`]'s `remove_doc_id`), so the mappings
	/// allocated while an index existed would otherwise be leaked — and a later
	/// re-create of a purged record key could be handed its stale, reused id.
	///
	/// The monotonic sequence counter is deliberately left intact, so a doc-ID
	/// index defined later keeps allocating fresh, never-reused ids.
	pub(crate) async fn remove_all(&self, tx: &Transaction) -> Result<()> {
		tx.delr(DocLookupPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?).await?;
		tx.delr(DocKeyPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?).await?;
		tx.delr(DocPendingPrefix::new(self.ns, self.db, Cow::Borrowed(&self.tb)).range()?).await?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::kvs::TransactionType::{Read, Write};
	use crate::kvs::{Datastore, TransactionType};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(1);

	fn rid(s: &str) -> RecordIdKey {
		RecordIdKey::from(s.to_owned())
	}

	/// Builds a frozen context (with sequences) plus a `TableDocIds` for table
	/// `t`. `idx` distinguishes independent per-index handles onto the *same*
	/// table-level space.
	async fn new_op(ds: &Datastore, tt: TransactionType) -> (FrozenContext, TableDocIds) {
		let mut ctx = ds.setup_ctx().unwrap();
		let tx = ds.transaction(tt).await.unwrap();
		ctx.set_transaction(tx.into());
		let d = TableDocIds::new(NS, DB, "t".into());
		(ctx.freeze(), d)
	}

	async fn finish(ctx: FrozenContext) {
		ctx.tx().commit().await.unwrap();
	}

	#[tokio::test]
	async fn resolve_is_idempotent_and_monotonic() {
		let ds = Datastore::new("memory").await.unwrap();
		{
			let (ctx, d) = new_op(&ds, Write).await;
			// First record gets id 0.
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			// Same record resolves to the same id (idempotent).
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			// A new record gets the next monotonic id.
			assert_eq!(d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(), 1);
			finish(ctx).await;
		}
		// Both directions of the mapping are persisted.
		let (ctx, d) = new_op(&ds, Read).await;
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), Some(0));
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("b")).await.unwrap(), Some(1));
		assert_eq!(d.get_record_id(&ctx.tx(), 0).await.unwrap(), Some(rid("a")));
		assert_eq!(d.get_record_id(&ctx.tx(), 1).await.unwrap(), Some(rid("b")));
	}

	#[tokio::test]
	async fn one_doc_id_shared_across_indexes() {
		// Two independent handles model two indexes on the same table. The first
		// to touch a record assigns the id; the second reads the committed value,
		// so both observe the SAME doc-id.
		let ds = Datastore::new("memory").await.unwrap();
		let assigned = {
			let (ctx, index_a) = new_op(&ds, Write).await;
			let id = index_a.resolve_or_assign(&ctx, &rid("shared")).await.unwrap();
			finish(ctx).await;
			id
		};
		let (ctx, index_b) = new_op(&ds, Write).await;
		assert_eq!(index_b.resolve_or_assign(&ctx, &rid("shared")).await.unwrap(), assigned);
		finish(ctx).await;
	}

	#[tokio::test]
	async fn delete_then_recreate_yields_new_id() {
		let ds = Datastore::new("memory").await.unwrap();
		{
			let (ctx, d) = new_op(&ds, Write).await;
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 0);
			assert_eq!(d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(), 1);
			finish(ctx).await;
		}
		// Remove "a" (doc 0).
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.remove(&ctx.tx(), &rid("a")).await.unwrap();
			finish(ctx).await;
		}
		// Re-create "a": it must get a fresh id (2), never the recycled 0.
		{
			let (ctx, d) = new_op(&ds, Write).await;
			assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 2);
			finish(ctx).await;
		}
		let (ctx, d) = new_op(&ds, Read).await;
		// The old reverse mapping for 0 is gone.
		assert_eq!(d.get_record_id(&ctx.tx(), 0).await.unwrap(), None);
		assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), Some(2));
	}

	#[tokio::test]
	async fn batch_resolve_preserves_order() {
		let ds = Datastore::new("memory").await.unwrap();
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(); // 0
			d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(); // 1
			finish(ctx).await;
		}
		let (ctx, d) = new_op(&ds, Read).await;
		// Order is preserved and unmapped ids yield None in their slot.
		let got = d.get_record_ids_batch(&ctx.tx(), &[1, 0, 9]).await.unwrap();
		assert_eq!(got, vec![Some(rid("b")), Some(rid("a")), None]);
	}

	#[tokio::test]
	async fn remove_all_clears_mappings_and_keeps_monotonicity() {
		// Models dropping the last doc-ID-consuming index on a table: every
		// `!di`/`!dd` mapping is reclaimed, but the sequence counter is kept so a
		// later index still allocates fresh, never-reused ids.
		let ds = Datastore::new("memory").await.unwrap();
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(); // 0
			d.resolve_or_assign(&ctx, &rid("b")).await.unwrap(); // 1
			finish(ctx).await;
		}
		// Purge the whole shared space.
		{
			let (ctx, d) = new_op(&ds, Write).await;
			d.remove_all(&ctx.tx()).await.unwrap();
			finish(ctx).await;
		}
		// Both directions are gone for every record.
		{
			let (ctx, d) = new_op(&ds, Read).await;
			assert_eq!(d.get_doc_id(&ctx.tx(), &rid("a")).await.unwrap(), None);
			assert_eq!(d.get_doc_id(&ctx.tx(), &rid("b")).await.unwrap(), None);
			assert_eq!(d.get_record_id(&ctx.tx(), 0).await.unwrap(), None);
			assert_eq!(d.get_record_id(&ctx.tx(), 1).await.unwrap(), None);
		}
		// A record re-created after the purge gets a fresh id (2), never the
		// reclaimed 0/1 — the never-reuse invariant survives a full purge.
		let (ctx, d) = new_op(&ds, Write).await;
		assert_eq!(d.resolve_or_assign(&ctx, &rid("a")).await.unwrap(), 2);
		finish(ctx).await;
	}
}

/// Cross-index concurrency regression tests. These exercise the last-writer-wins
/// `!di` assignment race (race #2) that only manifests on TiKV, so they are
/// gated to the `kv-tikv` backend and require a running cluster at
/// 127.0.0.1:2379 (the `tikv` CI lane).
#[cfg(test)]
#[cfg(feature = "kv-tikv")]
mod tikv_concurrency {
	use std::borrow::Cow;
	use std::collections::HashSet;
	use std::sync::Arc;

	use uuid::Uuid;

	use super::{DocId, TableDocIds};
	use crate::CommunityComposer;
	use crate::catalog::{DatabaseId, NamespaceId};
	use crate::key::schema::{DocKeyPrefix, RootRoot, VersionKey};
	use crate::kvs::{Datastore, TransactionType, is_retryable_transaction_conflict};
	use crate::val::RecordIdKey;

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(1);

	async fn fresh_tikv_ds() -> Arc<Datastore> {
		let ds = Datastore::builder()
			.with_id(Uuid::new_v4())
			.build_with_factory_path("tikv:127.0.0.1:2379", CommunityComposer())
			.await
			.unwrap();
		let tx = ds.transaction(TransactionType::Write).await.unwrap();
		// Both top-level regions: everything under the root, and the storage
		// version key, which sits outside it.
		tx.delr(RootRoot {}.range_subtree().unwrap()).await.unwrap();
		tx.del_key(&VersionKey {}).await.unwrap();
		tx.commit().await.unwrap();
		Arc::new(ds)
	}

	/// Resolve-or-assign `rid` in its own transaction, retrying on a transaction
	/// conflict until it commits. Returns the doc-ID that was durably committed.
	async fn resolve_committed(ds: &Datastore, rid: &RecordIdKey) -> DocId {
		loop {
			let mut ctx = ds.setup_ctx().unwrap();
			let tx = ds.transaction(TransactionType::Write).await.unwrap();
			ctx.set_transaction(tx.into());
			let ctx = ctx.freeze();
			let d = TableDocIds::new(NS, DB, "t".into());
			match d.resolve_or_assign(&ctx, rid).await {
				Ok(doc_id) => match ctx.tx().commit().await {
					Ok(()) => return doc_id,
					Err(e) if is_retryable_transaction_conflict(&e) => continue,
					Err(e) => panic!("unexpected commit error: {e}"),
				},
				Err(e) => {
					let _ = ctx.tx().cancel().await;
					if is_retryable_transaction_conflict(&e) {
						continue;
					}
					panic!("unexpected resolve error: {e}");
				}
			}
		}
	}

	/// Race #2: several indexes first-touching the SAME record concurrently must
	/// converge on ONE doc-ID. A blind `set !di` lets a later writer overwrite
	/// the forward mapping on TiKV while every writer's distinct `!dd` reverse
	/// entry survives — leaving a record with several doc-IDs. The conditional
	/// `putc` claim keeps a single winner that all others adopt, so exactly one
	/// `!di`/`!dd` pair remains.
	#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
	#[serial_test::serial]
	async fn one_doc_id_across_concurrent_indexes() {
		const TASKS: usize = 6;
		let ds = fresh_tikv_ds().await;
		let rid = RecordIdKey::from("shared".to_owned());

		let mut handles = Vec::with_capacity(TASKS);
		for _ in 0..TASKS {
			let ds = Arc::clone(&ds);
			let rid = rid.clone();
			handles.push(tokio::spawn(async move { resolve_committed(&ds, &rid).await }));
		}
		let mut ids = Vec::with_capacity(TASKS);
		for h in handles {
			ids.push(h.await.unwrap());
		}

		// Every task converged on the same committed doc-ID.
		let unique: HashSet<_> = ids.iter().copied().collect();
		assert_eq!(unique.len(), 1, "all indexes must share one doc-ID, got {ids:?}");
		let doc_id = ids[0];

		let tb = "t".into();
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let d = TableDocIds::new(NS, DB, "t".into());
		// The bidirectional mapping is consistent...
		assert_eq!(d.get_doc_id(&tx, &rid).await.unwrap(), Some(doc_id));
		assert_eq!(d.get_record_id(&tx, doc_id).await.unwrap(), Some(rid.clone()));
		// ...and no loser leaked a stale reverse mapping: exactly one `!dd` entry.
		let reverse = tx
			.getr(DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap(), None)
			.await
			.unwrap();
		assert_eq!(
			reverse.len(),
			1,
			"exactly one reverse mapping expected, found {}",
			reverse.len()
		);
		tx.cancel().await.unwrap();
	}

	/// Race #2, deterministic (fails before the fix, passes after): drive the
	/// exact last-writer-wins window instead of relying on scheduler timing.
	/// Index B opens its transaction — and pins its snapshot with a read — before
	/// index A assigns and commits the doc-ID. B then resolves on that stale
	/// snapshot: a blind `set !di` would last-writer-wins overwrite A's mapping
	/// (leaving the record with two doc-IDs), whereas the `putc` claim makes B's
	/// commit conflict. Either way the shared space must end with exactly one
	/// mapping, pointing at A's doc-ID.
	#[tokio::test]
	#[serial_test::serial]
	async fn first_touch_never_diverges_in_lww_window() {
		let ds = fresh_tikv_ds().await;
		let rid = RecordIdKey::from("rec".to_owned());

		// B's transaction starts first; the explicit read pins its snapshot to
		// the pre-assignment state (no `!di` yet), even if start timestamps are
		// assigned lazily.
		let mut ctx_b = ds.setup_ctx().unwrap();
		let tx_b = ds.transaction(TransactionType::Write).await.unwrap();
		ctx_b.set_transaction(tx_b.into());
		let ctx_b = ctx_b.freeze();
		let d_b = TableDocIds::new(NS, DB, "t".into());
		assert_eq!(d_b.get_doc_id(&ctx_b.tx(), &rid).await.unwrap(), None);

		// A assigns a doc-ID and commits.
		let a_doc = {
			let mut ctx_a = ds.setup_ctx().unwrap();
			let tx_a = ds.transaction(TransactionType::Write).await.unwrap();
			ctx_a.set_transaction(tx_a.into());
			let ctx_a = ctx_a.freeze();
			let d_a = TableDocIds::new(NS, DB, "t".into());
			let id = d_a.resolve_or_assign(&ctx_a, &rid).await.unwrap();
			ctx_a.tx().commit().await.unwrap();
			id
		};

		// B resolves on its stale snapshot, then tries to commit.
		let b_doc = d_b.resolve_or_assign(&ctx_b, &rid).await.unwrap();
		let b_commit_ok = ctx_b.tx().commit().await.is_ok();

		// Exactly one mapping survives, and it is A's.
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let d = TableDocIds::new(NS, DB, "t".into());
		let tb = "t".into();
		let reverse = tx
			.getr(DocKeyPrefix::new(NS, DB, Cow::Borrowed(&tb)).range().unwrap(), None)
			.await
			.unwrap();
		assert_eq!(d.get_doc_id(&tx, &rid).await.unwrap(), Some(a_doc));
		assert_eq!(
			reverse.len(),
			1,
			"record must have exactly one doc-ID (b_doc={b_doc}, b_commit_ok={b_commit_ok})",
		);
		tx.cancel().await.unwrap();
	}
}
