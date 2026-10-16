//! This module applies index mutations for a single document across different
//! index types (UNIQUE, regular, search, fulltext, Hnsw). Index keys are
//! constructed via key::index and field values are encoded using
//! key::value::Array.
//!
//! Numeric normalization in keys:
//! - Array normalizes Number values (Int/Float/Decimal) through a lexicographic numeric encoding so
//!   that byte order mirrors numeric order.
//! - Numerically equal values (e.g., 0, 0.0, 0dec) map to the same key bytes. On UNIQUE indexes,
//!   such inserts collide and produce a uniqueness error.
//!
//! Planner/executor simplification:
//! - Numeric predicates need a single probe/range in the index; per-variant fan-out is no longer
//!   required.

use std::path::PathBuf;

use anyhow::{Result, bail};
use surrealdb_datastore::index_shapes::{ShapeRecorder, ValueShape};
use surrealdb_datastore::{Transaction, storage_error};
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	CondText, DatabaseId, DiskAnnParams, FullTextParams, HnswParams, Index, IndexDefinition,
	NamespaceId, TableId,
};
use crate::count::{IndexCountCompactionPlan, IndexCountThingIterator};
use crate::docids::{DocId, TableDocIds};
use crate::entry::IndexEntryValue;
use crate::env::{IndexEnv, expect_not_done};
use crate::expr::Part;
use crate::ft::analyzer::AnalyzerFunction;
use crate::ft::fulltext::{FullTextCompactionPlan, FullTextIndex};
use crate::kernels::btree::{IndexKeyspace, StoredKey};
#[cfg(diskann)]
use crate::trees::diskann::index::{DiskAnnCompactionPlan, DiskAnnIndex};
use crate::trees::hnsw::index::{HnswCompactionOutcome, HnswCompactionPlan, HnswIndex};
use crate::trees::store::IndexStores;
use crate::val::{Array, RecordId, TableName, Value};
use crate::{CompactionApply, Error as IdxError, IndexKeyBase, key};

pub struct IndexOperation<'a> {
	env: &'a dyn IndexEnv,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableId,
	ix: &'a IndexDefinition,
	ikb: IndexKeyBase,
	/// `ix.table_name` wrapped as a `TableName`, resolved once here rather
	/// than per key built below (`stored_key` runs once per indexed value,
	/// i.e. per row for a scalar column) so per-value key construction stays
	/// a cheap borrow instead of a `Strand` clone per value.
	table_name: TableName,
	/// The old values (if existing)
	o: Option<Vec<Value>>,
	/// The new values (if existing)
	n: Option<Vec<Value>>,
	rid: &'a RecordId,
	/// For COUNT indexes with a WHERE condition: pre-evaluated condition results.
	/// `(old_doc_matches, new_doc_matches)` — whether the old/new document
	/// satisfies the COUNT index condition. `None` for non-COUNT indexes.
	count_cond_match: Option<(bool, bool)>,
	/// Who records the value shapes this operation writes: a document write,
	/// whose recording stamps the table's catalog version, or a build
	/// ([`surrealdb_datastore::index_shapes`]).
	shapes_by: ShapeRecorder,
}

impl<'a> IndexOperation<'a> {
	#[expect(clippy::too_many_arguments)]
	pub fn new(
		env: &'a dyn IndexEnv,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: &'a IndexDefinition,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
		rid: &'a RecordId,
	) -> Self {
		let table_name = ix.table_name.clone();
		Self {
			env,
			ns,
			db,
			tb,
			ix,
			ikb: IndexKeyBase::new(ns, db, table_name.clone(), ix.index_id),
			table_name,
			o,
			n,
			rid,
			count_cond_match: None,
			shapes_by: ShapeRecorder::Write,
		}
	}

	/// The same operation, applied by an index build: the value shapes it
	/// records stamp no catalog version.
	pub fn within_build(mut self) -> Self {
		self.shapes_by = ShapeRecorder::Build;
		self
	}

	pub fn with_count_cond_match(mut self, old_matches: bool, new_matches: bool) -> Self {
		self.count_cond_match = Some((old_matches, new_matches));
		self
	}

	pub async fn create_fulltext_index(
		env: &dyn IndexEnv,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
	) -> Result<Option<FullTextIndex>> {
		let Index::FullText(p) = &ix.index else {
			return Ok(None);
		};
		let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
		Ok(Some(
			FullTextIndex::new(
				env.index_stores(),
				&env.tx(),
				ikb,
				p,
				&env.config().file_allowlist,
				ix.format_version,
			)
			.await?,
		))
	}

	pub async fn compute(
		&mut self,
		az_fn: &dyn AnalyzerFunction,
		require_compaction: &mut bool,
	) -> Result<()> {
		// Index operation dispatching
		match &self.ix.index {
			Index::Uniq => self.index_unique().await,
			Index::Idx => self.index_non_unique().await,
			Index::FullText(p) => self.index_fulltext(az_fn, p, require_compaction).await,
			Index::Hnsw(p) => self.index_hnsw(p, require_compaction).await,
			Index::DiskAnn(p) => self.index_diskann(p, require_compaction).await,
			Index::Count(c) => self.index_count(c.as_ref(), require_compaction).await,
		}
	}

	/// Where the entry for values `v` of this record is stored: the unique
	/// slot on a `UNIQUE` index for a tuple holding no `NONE` or `NULL`, and
	/// otherwise the non-unique key naming the record
	/// ([`IndexKeyspace::stored_key`], which the read path's ranges agree
	/// with). Numbers of every kind share key bytes, so `0`, `0f` and `0dec`
	/// collide on a `UNIQUE` index.
	fn stored_key(&self, unique: bool, v: &[Value]) -> Result<StoredKey> {
		IndexKeyspace::of(self.ns, self.db, self.ix).stored_key(unique, v, &self.rid.key)
	}

	/// The value stored in this index's entries: the record ID, plus the
	/// record's table-level doc-ID when the index format carries it (see
	/// [`StoredIndexDefinition::has_entry_doc_ids`]). The doc-ID is resolved — or
	/// assigned on first use — through the table's shared doc-ID space, so all
	/// of a table's indexes agree on the record's doc-ID.
	async fn entry_value(&self) -> Result<IndexEntryValue> {
		let doc_id: Option<DocId> = if self.ix.has_entry_doc_ids() {
			let doc_ids = TableDocIds::new(self.ns, self.db, self.table_name.clone());
			Some(doc_ids.resolve_or_assign(self.env, &self.rid.key).await?)
		} else {
			None
		};
		Ok(IndexEntryValue {
			rid: self.rid.clone(),
			doc_id,
		})
	}

	/// Delete an index entry with a guarded (compare) delete, tolerating an
	/// absent or foreign entry.
	///
	/// When the expected value carries a doc-ID and the guarded delete misses,
	/// the delete is retried against the bare record-ID encoding: an entry may
	/// predate the index's doc-ID format (e.g. written by an older binary
	/// during a rolling upgrade) and must still be removable.
	async fn del_entry<K>(txn: &Transaction, key: &K, expected: &IndexEntryValue) -> Result<()>
	where
		K: key::KVKey<Value = IndexEntryValue> + std::fmt::Debug,
	{
		fn is_condition_not_met(e: &anyhow::Error) -> bool {
			matches!(storage_error(e), Some(surrealdb_kvs::Error::TransactionConditionNotMet))
		}
		match txn.del_compare_key(key, Some(expected)).await {
			Err(e) if is_condition_not_met(&e) => {
				if expected.doc_id.is_some() {
					let bare = IndexEntryValue {
						rid: expected.rid.clone(),
						doc_id: None,
					};
					match txn.del_compare_key(key, Some(&bare)).await {
						Err(e) if is_condition_not_met(&e) => Ok(()),
						other => other,
					}
				} else {
					Ok(())
				}
			}
			other => other,
		}
	}

	/// A document's entries are written one per combination of its columns'
	/// elements, so a single document can be an arbitrarily long run of
	/// writes. The cancellation checkpoint therefore runs per entry rather
	/// than once per document, or nothing could interrupt one document's
	/// fan-out and a long one would starve the worker it runs on.
	///
	/// A query that is cancelled or times out stops the loop with an error —
	/// `QueryCancelled` or `QueryTimedout`, whichever stopped it — and never
	/// with an early `Ok`, which would report a half-maintained index as
	/// maintained. See [`Self::checkpoint`] for what the stop does to the
	/// transaction.
	///
	/// A uniqueness conflict (`IndexExists`) also stops the loop part-way but
	/// leaves the transaction committable, because `INSERT ... ON DUPLICATE KEY
	/// UPDATE` and `UPSERT` recover from it by rolling back to a save point
	/// taken ahead of the write. Only the caller aborting or rolling back keeps
	/// the entries written before a conflict out of a commit.
	///
	/// [`Self::index_non_unique`] keeps the same contract.
	async fn index_unique(&mut self) -> Result<()> {
		let txn = self.env.tx();
		let value = self.entry_value().await?;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for (entry, o) in i.into_iter().enumerate() {
				self.checkpoint(&txn, entry).await?;
				let key = self.stored_key(true, &o)?;
				Self::del_entry(&txn, &key, &value).await?;
			}
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			self.record_shapes(&txn, &n).await?;
			let i = Indexable::new(n, self.ix);
			for (entry, n) in i.into_iter().enumerate() {
				self.checkpoint(&txn, entry).await?;
				let key = self.stored_key(true, &n)?;
				if !key.unique {
					// A tuple holding NONE or NULL names the record and is
					// checked for no uniqueness: NULL equals no other NULL.
					txn.set_key(&key, &value).await?;
				} else if txn.put_compare_key(&key, &value, None).await.is_err() {
					let entry: IndexEntryValue =
						txn.get_key(&key, None).await?.expect("record should exist");
					return self.err_index_exists(entry.rid, n);
				}
			}
		}
		Ok(())
	}

	/// Record the shapes of the values `n` this b-tree index stores beyond
	/// those its columns are expected to ([`recorded_shapes`]), in the
	/// transaction that writes their entries ([`Transaction::record_index_shapes`]).
	async fn record_shapes(&self, txn: &Transaction, n: &[Value]) -> Result<()> {
		let ix = self.ix.index_id;
		let known = txn.index_shapes(self.ns, self.db, &self.table_name, ix).await?;
		if known.records_all(recorded_shapes(n, self.ix)) {
			return Ok(());
		}
		let shapes: Vec<_> = recorded_shapes(n, self.ix).collect();
		txn.record_index_shapes(self.ns, self.db, &self.table_name, ix, &shapes, self.shapes_by)
			.await
	}

	/// The cancellation checkpoint a b-tree fan-out runs before its `entry`-th
	/// write.
	///
	/// A stop poisons `txn` before it is reported (see
	/// [`Transaction::poison_interrupted_write`]): the document's record and the
	/// entries written so far are in the transaction, and not every caller that
	/// sees the statement fail discards it.
	async fn checkpoint(&self, txn: &Transaction, entry: usize) -> Result<()> {
		let stopped = expect_not_done(self.env, Some(entry)).await;
		if stopped.is_err() {
			txn.poison_interrupted_write();
		}
		stopped
	}

	/// Maintains a non-unique index's entries under the contract on
	/// [`Self::index_unique`].
	async fn index_non_unique(&mut self) -> Result<()> {
		// Lock the transaction
		let txn = self.env.tx();
		let value = self.entry_value().await?;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for (entry, o) in i.into_iter().enumerate() {
				self.checkpoint(&txn, entry).await?;
				let key = self.stored_key(false, &o)?;
				Self::del_entry(&txn, &key, &value).await?;
			}
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			self.record_shapes(&txn, &n).await?;
			let i = Indexable::new(n, self.ix);
			for (entry, n) in i.into_iter().enumerate() {
				self.checkpoint(&txn, entry).await?;
				let key = self.stored_key(false, &n)?;
				txn.set_key(&key, &value).await?;
			}
		}
		Ok(())
	}

	async fn index_count(
		&mut self,
		cond: Option<&CondText>,
		require_compaction: &mut bool,
	) -> Result<()> {
		let mut relative_count: i8 = 0;
		if let Some(_c) = cond {
			let (old_matches, new_matches) = self.count_cond_match.unwrap_or((false, false));
			if self.o.is_some() && old_matches {
				relative_count -= 1;
			}
			if self.n.is_some() && new_matches {
				relative_count += 1;
			}
		} else {
			if self.o.is_some() {
				relative_count -= 1;
			}
			if self.n.is_some() {
				relative_count += 1;
			}
		}
		if relative_count == 0 {
			return Ok(());
		}
		// Accumulate into the transaction's per-index total instead of writing
		// one `!iu` entry per document. The entry is still written blind under a
		// key no other transaction shares, so nothing contends; it is just
		// written once per transaction, at commit, carrying the net delta.
		// Since every `count()` read sums the un-compacted entries, the
		// per-document form made read cost scale with write volume.
		self.env.tx().buffer_count_delta(
			self.ns,
			self.db,
			&self.table_name,
			self.ix.index_id,
			relative_count as i64,
			self.env.node_id(),
		);
		*require_compaction = true;
		Ok(())
	}

	/// Creates the read-phase plan for full-text compaction.
	///
	/// The caller owns the transaction split so this can run in a read-only
	/// transaction and be applied later with a short write transaction.
	pub async fn prepare_fulltext_compaction(
		ixs: &IndexStores,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		p: &FullTextParams,
		allow_list: &[PathBuf],
		format_version: u16,
	) -> Result<FullTextCompactionPlan> {
		let ft = FullTextIndex::new(ixs, tx, ikb.clone(), p, allow_list, format_version).await?;
		ft.prepare_compaction(tx).await
	}

	/// Applies a prepared full-text compaction plan.
	///
	/// Reports [`CompactionApply::Empty`] when the plan has no work, and
	/// [`CompactionApply::LostRace`] when another compactor advanced a generation
	/// first.
	pub async fn apply_fulltext_compaction(
		ixs: &IndexStores,
		ikb: &IndexKeyBase,
		tx: &Transaction,
		p: &FullTextParams,
		allow_list: &[PathBuf],
		format_version: u16,
		plan: FullTextCompactionPlan,
	) -> Result<CompactionApply> {
		let ft = FullTextIndex::new(ixs, tx, ikb.clone(), p, allow_list, format_version).await?;
		ft.apply_compaction(tx, plan).await
	}

	/// Creates the read-phase plan for HNSW pending compaction.
	pub async fn prepare_hnsw_compaction(
		env: &dyn IndexEnv,
		ikb: &IndexKeyBase,
	) -> Result<HnswCompactionPlan> {
		HnswIndex::prepare_compaction(env, ikb).await
	}

	/// Applies a prepared HNSW pending compaction plan.
	///
	/// Reports [`HnswCompactionOutcome::Empty`] when there is no work or the
	/// table is gone, and [`HnswCompactionOutcome::LostRace`] when another
	/// compactor advanced the generation first or a captured pending key changed
	/// before the write. What the other outcomes owe a caller whose commit then
	/// fails is on [`HnswCompactionOutcome`].
	pub async fn apply_hnsw_compaction(
		env: &dyn IndexEnv,
		ixs: &IndexStores,
		ikb: &IndexKeyBase,
		p: &HnswParams,
		format_version: u16,
		plan: HnswCompactionPlan,
	) -> Result<HnswCompactionOutcome> {
		let tx = env.tx();
		if let Some(tb) = tx.get_tb(ikb.ns(), ikb.db(), ikb.table(), None).await? {
			let hnsw = ixs.get_index_hnsw(env, tb.table_id, ikb, p, format_version).await?;
			return hnsw.apply_compaction(env, plan).await;
		}
		Ok(HnswCompactionOutcome::Empty)
	}

	#[cfg(diskann)]
	/// Creates the read-phase plan for DiskANN pending compaction.
	pub async fn prepare_diskann_compaction(
		env: &dyn IndexEnv,
		ikb: &IndexKeyBase,
	) -> Result<DiskAnnCompactionPlan> {
		DiskAnnIndex::prepare_compaction(env, ikb).await
	}

	#[cfg(diskann)]
	/// Applies a prepared DiskANN pending compaction plan, committing its
	/// transaction when it reports [`CompactionApply::Applied`].
	///
	/// Reports [`CompactionApply::Empty`] when the table is gone, without
	/// finishing the transaction; every other outcome is as the index's own
	/// `DiskAnnIndex::apply_compaction` documents it.
	pub async fn apply_diskann_compaction(
		env: &dyn IndexEnv,
		ixs: &IndexStores,
		ikb: &IndexKeyBase,
		p: &DiskAnnParams,
		format_version: u16,
		plan: DiskAnnCompactionPlan,
	) -> Result<CompactionApply> {
		let tx = env.tx();
		if let Some(tb) = tx.get_tb(ikb.ns(), ikb.db(), ikb.table(), None).await? {
			let diskann = ixs.get_index_diskann(format_version, tb.table_id, ikb, p).await?;
			return diskann.apply_compaction(env, plan).await;
		}
		Ok(CompactionApply::Empty)
	}

	/// Creates the read-phase plan for count-index compaction.
	pub async fn prepare_count_compaction(
		ikb: &IndexKeyBase,
		tx: &Transaction,
	) -> Result<IndexCountCompactionPlan> {
		IndexCountThingIterator::new(ikb.ns(), ikb.db(), ikb.table(), ikb.index())?
			.prepare_compaction(ikb, tx)
			.await
	}

	/// Applies a prepared count-index compaction plan.
	///
	/// Reports [`CompactionApply::Empty`] when the plan has no work, and
	/// [`CompactionApply::LostRace`] when another compactor advanced the
	/// generation first.
	pub async fn apply_count_compaction(
		ikb: &IndexKeyBase,
		tx: &Transaction,
		plan: IndexCountCompactionPlan,
	) -> Result<CompactionApply> {
		IndexCountThingIterator::apply_compaction(ikb, tx, plan).await
	}

	/// Construct a consistent uniqueness violation error message.
	/// Formats the conflicting value as a single value or array depending on
	/// the number of indexed fields.
	fn err_index_exists(&self, rid: RecordId, mut n: Array) -> Result<()> {
		bail!(IdxError::IndexExists {
			record: rid,
			index: self.ix.name.to_string(),
			value: match n.0.len() {
				1 => n.0.remove(0).to_sql(),
				_ => n.to_sql(),
			},
		})
	}

	async fn index_fulltext(
		&mut self,
		az_fn: &dyn AnalyzerFunction,
		p: &FullTextParams,
		require_compaction: &mut bool,
	) -> Result<()> {
		// Build a FullText instance
		let fti = FullTextIndex::new(
			self.env.index_stores(),
			&self.env.tx(),
			self.ikb.clone(),
			p,
			&self.env.config().file_allowlist,
			self.ix.format_version,
		)
		.await?;
		self.compute_fulltext_with_index(az_fn, &fti, require_compaction).await
	}

	pub async fn compute_fulltext_with_index(
		&mut self,
		az_fn: &dyn AnalyzerFunction,
		fti: &FullTextIndex,
		require_compaction: &mut bool,
	) -> Result<()> {
		let mut rc = false;
		// Delete the old index data (posting lists, offsets, doc length/count).
		if let Some(o) = self.o.take() {
			fti.remove_content(self.env, az_fn, self.rid, o, &mut rc).await?;
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			fti.index_content(self.env, az_fn, self.rid, n, &mut rc).await?;
		}
		// The record ↔ doc-ID mapping is shared across all of the table's indexes,
		// so it is removed centrally at record purge, not per index here.
		// Do we need to trigger the compaction?
		if rc {
			*require_compaction = true;
		}
		Ok(())
	}

	pub async fn trigger_compaction(&self) -> Result<()> {
		IndexOperation::compaction_trigger(&self.ikb, &self.env.tx(), self.env.node_id()).await
	}

	/// Triggers index compaction.
	///
	/// This method adds an entry to the index compaction queue by creating an
	/// `Ic` key for the specified index. The index compaction thread will
	/// later process this entry and perform the actual compaction via
	/// [`Datastore::index_compaction`].
	///
	/// Every request writes its own unique key (node id plus a fresh UUIDv7)
	/// rather than updating one shared per-index key: blind writes of
	/// distinct keys never contend, whereas a shared key would put every
	/// concurrent transaction touching the same index into write-write
	/// conflict (and a read-modify-write dedup would be worse on
	/// last-writer-wins backends). The queue therefore grows with indexed
	/// write activity, and deduplication happens at drain time instead:
	/// [`Datastore::index_compaction`] compacts each distinct index per
	/// batch and deletes the batch's entries in bounded transactions.
	///
	/// Compaction helps optimize index performance after many mutations.
	/// For full-text indexes it consolidates term frequency and document
	/// length data; for HNSW indexes it processes pending vector operations;
	/// for count indexes it reconciles count tracking entries.
	pub async fn compaction_trigger(ikb: &IndexKeyBase, tx: &Transaction, nid: Uuid) -> Result<()> {
		// Deduplicated per index for this transaction and written at commit, so
		// a statement touching many documents enqueues one request rather than
		// one per document. The queue entry only names the index to compact, so
		// repeating it per document added drain work without adding information.
		tx.buffer_compaction_trigger(ikb.ns(), ikb.db(), ikb.table(), ikb.index(), nid);
		Ok(())
	}

	async fn index_hnsw(&mut self, p: &HnswParams, require_compaction: &mut bool) -> Result<()> {
		let hnsw = self
			.env
			.index_stores()
			.get_index_hnsw(self.env, self.tb, &self.ikb, p, self.ix.format_version)
			.await?;
		let old_values = self.o.take();
		let new_values = self.n.take();
		if old_values.is_some() || new_values.is_some() {
			hnsw.index(self.env, &self.rid.key, old_values, new_values).await?;
			*require_compaction = true;
		}
		Ok(())
	}

	async fn index_diskann(
		&mut self,
		p: &DiskAnnParams,
		require_compaction: &mut bool,
	) -> Result<()> {
		#[cfg(not(diskann))]
		{
			let _ = (p, require_compaction);
			bail!("DISKANN indexes require a 64-bit, non-WASM platform")
		}
		#[cfg(diskann)]
		{
			let diskann = self
				.env
				.index_stores()
				.get_index_diskann(self.ix.format_version, self.tb, &self.ikb, p)
				.await?;
			let old_values = self.o.take();
			let new_values = self.n.take();
			if old_values.is_some() || new_values.is_some() {
				diskann.index(self.env, &self.rid.key, old_values, new_values).await?;
				*require_compaction = true;
			}
			Ok(())
		}
	}
}

/// The shape the writer stores each column of `vals` in, for B-tree index
/// `ix`, by column position ([`ValueShape`]). A column whose idiom ends in a
/// flatten stores its value whole, an array included; any other column lists
/// an array once per element. `NONE` and `NULL` record no shape.
pub fn stored_shapes<'v>(
	vals: &'v [Value],
	ix: &'v IndexDefinition,
) -> impl Iterator<Item = (u16, ValueShape)> + 'v {
	vals.iter().zip(ix.cols.iter()).zip(0u16..).filter_map(|((v, col), i)| {
		let shape = match v {
			Value::None | Value::Null => return None,
			Value::Array(_) if !stores_arrays_whole(col) => ValueShape::Array,
			_ => ValueShape::Whole,
		};
		Some((i, shape))
	})
}

/// Whether the writer stores an array in column `col` whole, at one entry,
/// rather than once per element: the column's idiom ends in a flatten.
fn stores_arrays_whole(col: &crate::expr::Idiom) -> bool {
	matches!(col.0.last(), Some(&Part::Flatten))
}

/// The shape column `col` is expected to store: an array, listed by element,
/// where its idiom selects many values (`.*`, `[WHERE …]`, a graph lookup) and
/// does not end in a flatten; a whole value otherwise. A guess only: either
/// shape may be stored in any column.
fn expected_shape(col: &crate::expr::Idiom) -> ValueShape {
	let selects_many =
		col.0.iter().any(|part| matches!(part, Part::All | Part::Where(_) | Part::Lookup(_)));
	if selects_many && !stores_arrays_whole(col) {
		ValueShape::Array
	} else {
		ValueShape::Whole
	}
}

/// The shape each column of B-tree index `ix` is expected to store, by
/// column position. The transaction that begins a build records them with
/// the index's tracked marker, so a column storing the shape it is expected
/// to never makes a write record anything ([`recorded_shapes`]): a table's
/// concurrent writers do not all rewrite one key on their first write.
pub fn expected_shapes(ix: &IndexDefinition) -> Vec<(u16, ValueShape)> {
	ix.cols.iter().zip(0u16..).map(|(col, i)| (i, expected_shape(col))).collect()
}

/// The shapes of `vals` a write to B-tree index `ix` records: those
/// [`stored_shapes`] yields that their column is not expected to store
/// ([`expected_shapes`]), which the index has recorded since its build began.
pub fn recorded_shapes<'v>(
	vals: &'v [Value],
	ix: &'v IndexDefinition,
) -> impl Iterator<Item = (u16, ValueShape)> + 'v {
	stored_shapes(vals, ix).filter(|&(i, shape)| shape != expected_shape(&ix.cols[usize::from(i)]))
}

/// Extract from the given document, the values required by the index and put
/// then in an array. Eg. IF the index is composed of the columns `name` and
/// `instrument` Given this doc: { "id": 1, "instrument":"piano", "name":"Tobie"
/// } It will return: ["Tobie", "piano"]
pub struct Indexable(Vec<(Value, bool)>);

impl Indexable {
	pub fn new(vals: Vec<Value>, ix: &IndexDefinition) -> Self {
		let mut source = Vec::with_capacity(vals.len());
		for (v, i) in vals.into_iter().zip(ix.cols.iter()) {
			source.push((v, stores_arrays_whole(i)));
		}
		Self(source)
	}
}

impl IntoIterator for Indexable {
	type Item = Array;
	type IntoIter = Combinator;

	fn into_iter(self) -> Self::IntoIter {
		Combinator::new(self.0)
	}
}

/// The combinations of an [`Indexable`]'s values an index holds entries for,
/// one array of values per entry.
pub struct Combinator {
	iterators: Vec<Box<dyn ValuesIterator>>,
	has_next: bool,
}

impl Combinator {
	fn new(source: Vec<(Value, bool)>) -> Self {
		let mut iterators: Vec<Box<dyn ValuesIterator>> = Vec::new();
		// We create an iterator for each idiom
		for (v, f) in source {
			if !f {
				// Iterator for not flattened values
				if let Value::Array(v) = v {
					iterators.push(Box::new(MultiValuesIterator::new(v.0)));
					continue;
				}
			}
			iterators.push(Box::new(SingleValueIterator(v)));
		}
		Self {
			iterators,
			has_next: true,
		}
	}
}

impl Iterator for Combinator {
	type Item = Array;

	fn next(&mut self) -> Option<Self::Item> {
		if !self.has_next {
			return None;
		}
		// Emit the odometer's current reading, then advance it.
		let o = Array::from(self.iterators.iter().map(|i| i.current().clone()).collect::<Vec<_>>());
		// Advance the first column that can advance, and wind every column
		// before it back to its start — the carry is what makes this the
		// cartesian product of the columns rather than a walk down one
		// column and along the next. Without the rewind an index over two
		// array-valued columns of lengths m and n yields m + n - 1 keys
		// instead of m * n, leaving combinations unindexed: reads served by
		// the index miss rows and a UNIQUE index stops rejecting a
		// duplicate combination.
		self.has_next = false;
		for i in &mut self.iterators {
			if i.next() {
				self.has_next = true;
				break;
			}
			i.reset();
		}
		Some(o)
	}
}

trait ValuesIterator: Send {
	/// Advance to this column's next value, reporting whether one existed.
	fn next(&mut self) -> bool;
	/// Wind back to this column's first value, so an outer column's carry
	/// restarts the sweep.
	fn reset(&mut self);
	fn current(&self) -> &Value;
}

struct MultiValuesIterator {
	vals: Vec<Value>,
	done: bool,
	current: usize,
	end: usize,
}

impl MultiValuesIterator {
	fn new(vals: Vec<Value>) -> Self {
		let len = vals.len();
		if len == 0 {
			Self {
				vals,
				done: true,
				current: 0,
				end: 0,
			}
		} else {
			Self {
				vals,
				done: false,
				current: 0,
				end: len - 1,
			}
		}
	}
}

impl ValuesIterator for MultiValuesIterator {
	fn next(&mut self) -> bool {
		if self.done {
			return false;
		}
		if self.current == self.end {
			self.done = true;
			return false;
		}
		self.current += 1;
		true
	}

	fn reset(&mut self) {
		// An empty source stays exhausted: it has no first value to wind
		// back to, and `current()` keeps yielding `Value::Null` for it.
		if !self.vals.is_empty() {
			self.current = 0;
			self.done = false;
		}
	}

	fn current(&self) -> &Value {
		self.vals.get(self.current).unwrap_or(&Value::Null)
	}
}

struct SingleValueIterator(Value);

impl ValuesIterator for SingleValueIterator {
	fn next(&mut self) -> bool {
		false
	}

	fn reset(&mut self) {
		// One value, always current — nothing to wind back.
	}

	fn current(&self) -> &Value {
		&self.0
	}
}

#[cfg(test)]
mod tests {
	//! The key set one document contributes to a b-tree index.
	//!
	//! [`Combinator`] is the odometer that turns the per-column values
	//! [`Indexable`] collected into the tuples actually written. A column
	//! holding an array fans out over its elements, so the tuples are the
	//! cartesian product of the columns — which is what lets a prefix seek
	//! on the leading columns find every combination of the trailing ones.

	use super::{Combinator, MultiValuesIterator, ValuesIterator};
	use crate::val::{Array, Value};

	/// Drive the odometer over columns given as either a scalar or an array,
	/// in the shape `Indexable::new` hands it: `false` marks a column whose
	/// array value fans out.
	fn keys(cols: Vec<Value>) -> Vec<Vec<Value>> {
		let source = cols.into_iter().map(|v| (v, false)).collect();
		Combinator::new(source).map(|a: Array| a.0).collect()
	}

	fn arr(vals: &[&str]) -> Value {
		Value::Array(Array::from(vals.iter().map(|v| Value::from(*v)).collect::<Vec<_>>()))
	}

	#[test]
	fn a_single_scalar_column_contributes_one_tuple() {
		assert_eq!(keys(vec![Value::from("a")]), vec![vec![Value::from("a")]]);
	}

	#[test]
	fn every_scalar_column_still_contributes_one_tuple() {
		assert_eq!(
			keys(vec![Value::from("a"), Value::from("b"), Value::from("c")]),
			vec![vec![Value::from("a"), Value::from("b"), Value::from("c")]]
		);
	}

	#[test]
	fn one_array_column_fans_out_over_its_elements() {
		assert_eq!(
			keys(vec![arr(&["x", "y", "z"])]),
			vec![vec![Value::from("x")], vec![Value::from("y")], vec![Value::from("z")]]
		);
	}

	#[test]
	fn an_array_column_fans_out_against_each_scalar_column() {
		assert_eq!(
			keys(vec![Value::from("g"), arr(&["x", "y"])]),
			vec![
				vec![Value::from("g"), Value::from("x")],
				vec![Value::from("g"), Value::from("y")]
			]
		);
	}

	#[test]
	fn two_array_columns_produce_the_full_cartesian_product() {
		// The odometer's carry is what this pins: without it the walk
		// yields ['p',1], ['q',1], ['q',2] — three of the four tuples,
		// leaving ['p',2] unindexed.
		let got = keys(vec![arr(&["p", "q"]), arr(&["1", "2"])]);
		assert_eq!(got.len(), 4, "m * n tuples, not m + n - 1: {got:?}");
		for tuple in [
			vec![Value::from("p"), Value::from("1")],
			vec![Value::from("p"), Value::from("2")],
			vec![Value::from("q"), Value::from("1")],
			vec![Value::from("q"), Value::from("2")],
		] {
			assert!(got.contains(&tuple), "missing {tuple:?} from {got:?}");
		}
	}

	#[test]
	fn three_array_columns_multiply_rather_than_add() {
		assert_eq!(keys(vec![arr(&["a", "b"]), arr(&["c", "d"]), arr(&["e", "f"])]).len(), 8);
	}

	#[test]
	fn the_product_is_ordered_with_the_leading_column_cycling_fastest() {
		assert_eq!(
			keys(vec![arr(&["p", "q"]), arr(&["1", "2"])]),
			vec![
				vec![Value::from("p"), Value::from("1")],
				vec![Value::from("q"), Value::from("1")],
				vec![Value::from("p"), Value::from("2")],
				vec![Value::from("q"), Value::from("2")]
			]
		);
	}

	#[test]
	fn an_empty_array_column_contributes_null_and_does_not_annihilate_the_product() {
		// An empty array has no element to index, but the document must
		// still get a key or it would vanish from the index entirely.
		assert_eq!(
			keys(vec![Value::Array(Array::from(Vec::<Value>::new())), arr(&["x", "y"])]),
			vec![vec![Value::Null, Value::from("x")], vec![Value::Null, Value::from("y")]]
		);
	}

	#[test]
	fn duplicate_elements_repeat_the_tuple_and_collapse_in_the_store() {
		// Identical tuples encode to one key, so the store holds a single
		// entry; the odometer itself does not deduplicate.
		assert_eq!(
			keys(vec![arr(&["x", "x"])]),
			vec![vec![Value::from("x")], vec![Value::from("x")]]
		);
	}

	#[test]
	fn a_reset_column_sweeps_again_from_its_first_value() {
		let mut it = MultiValuesIterator::new(vec![Value::from("a"), Value::from("b")]);
		assert_eq!(it.current(), &Value::from("a"));
		assert!(it.next());
		assert_eq!(it.current(), &Value::from("b"));
		assert!(!it.next(), "exhausted at the last element");
		it.reset();
		assert_eq!(it.current(), &Value::from("a"), "wound back to the first value");
		assert!(it.next(), "and can sweep again");
	}

	#[test]
	fn resetting_an_empty_column_leaves_it_exhausted() {
		let mut it = MultiValuesIterator::new(Vec::new());
		assert!(!it.next());
		it.reset();
		assert!(!it.next(), "an empty column has no first value to wind back to");
		assert_eq!(it.current(), &Value::Null);
	}
}

#[cfg(test)]
mod interrupted_fan_out {
	//! What a b-tree index write leaves behind when the query stops part-way
	//! through one document's fan-out.
	//!
	//! The fixture document has two array columns of `SIDE` elements each, so
	//! it contributes `SIDE * SIDE` entries and the checkpoint runs once per
	//! entry. A write stopped at the `AT`-th checkpoint has written the
	//! `AT - 1` entries before it: the error must name why the write stopped,
	//! and those entries must reach no committed state even when the caller
	//! commits anyway, as a client-owned transaction can.

	use std::time::Duration;

	use anyhow::Result;
	use common::EngineError;
	use surrealdb_datastore::{DatastoreError, Transaction};
	use surrealdb_kvs::TransactionType;
	use surrealdb_strand::Strand;

	use super::IndexOperation;
	use crate::catalog::{
		DatabaseId, INDEX_FORMAT_VERSION, Index, IndexDefinition, IndexId, NamespaceId, TableId,
	};
	use crate::env::IndexEnv;
	use crate::expr::Idiom;
	use crate::ft::analyzer::{AnalyzerFunction, BoxAnalyzerFut};
	use crate::kernels::btree::IndexKeyspace;
	use crate::test_env::{CancelAtCheckpoint, TestIndexStore};
	use crate::val::{Array, RecordId, RecordIdKey, TableName, Value};

	const NS: NamespaceId = NamespaceId(1);
	const DB: DatabaseId = DatabaseId(2);
	const TB: TableId = TableId(3);
	const SIDE: i32 = 8;
	const ENTRIES: usize = (SIDE * SIDE) as usize;
	const AT: usize = 20;

	/// Stands in for the evaluator behind an analyzer's `FUNCTION` clause,
	/// which a b-tree index never reaches.
	struct NoAnalyzerFunction;

	impl AnalyzerFunction for NoAnalyzerFunction {
		fn call<'a>(&'a self, name: &'a str, _input: Strand) -> BoxAnalyzerFut<'a> {
			unreachable!("a b-tree index calls no analyzer FUNCTION {name}")
		}
	}

	fn definition(index: Index) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(4),
			name: "ix".into(),
			table_name: TableName::from("t"),
			cols: vec![Idiom::field("a"), Idiom::field("b")],
			index,
			count_cond: None,
			prepare_remove: false,
			format_version: INDEX_FORMAT_VERSION,
			comment: None,
		}
	}

	fn record() -> RecordId {
		RecordId::new(TableName::from("t"), RecordIdKey::from("doc".to_owned()))
	}

	/// The indexed values of the fixture document: two `SIDE`-element arrays.
	fn columns() -> Vec<Value> {
		let side = || Value::Array(Array::from((0..SIDE).map(Value::from).collect::<Vec<_>>()));
		vec![side(), side()]
	}

	/// Runs one document's index write: a create when `o` is `None`, a delete
	/// when `n` is.
	async fn write(
		env: &dyn IndexEnv,
		ix: &IndexDefinition,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
	) -> Result<()> {
		let rid = record();
		let mut op = IndexOperation::new(env, NS, DB, TB, ix, o, n, &rid);
		let mut require_compaction = false;
		op.compute(&NoAnalyzerFunction, &mut require_compaction).await
	}

	/// The index's entries as `tx` sees them, unique and non-unique alike.
	async fn entries(tx: &Transaction, ix: &IndexDefinition) -> usize {
		let range = IndexKeyspace::of(NS, DB, ix).all().unwrap();
		tx.count(range, None).await.unwrap()
	}

	async fn committed_entries(ds: &TestIndexStore, ix: &IndexDefinition) -> usize {
		let tx = ds.transaction(TransactionType::Read).await.unwrap();
		let n = entries(&tx, ix).await;
		tx.cancel().await.unwrap();
		n
	}

	/// Commits the whole fixture document, so a delete has entries to remove.
	async fn committed_document(ix: &IndexDefinition) -> TestIndexStore {
		let ds = TestIndexStore::new().await;
		let env = ds.env(TransactionType::Write).await;
		write(&env, ix, None, Some(columns())).await.unwrap();
		env.tx().commit().await.unwrap();
		assert_eq!(committed_entries(&ds, ix).await, ENTRIES, "one entry per combination");
		ds
	}

	fn assert_cancelled(err: &anyhow::Error) {
		assert!(
			matches!(err.downcast_ref::<EngineError>(), Some(EngineError::QueryCancelled)),
			"expected a cancellation, got {err:#}"
		);
	}

	/// Commits the stopped write's transaction as a caller that carries on past
	/// the failed statement would, and asserts the commit is refused.
	async fn assert_commit_refused(tx: &Transaction) {
		let err = tx.commit().await.unwrap_err();
		assert!(
			matches!(
				err.downcast_ref::<DatastoreError>(),
				Some(DatastoreError::QueryNotExecuted { .. })
			),
			"expected the commit to be refused, got {err:#}"
		);
	}

	async fn cancelled_create_refuses_the_commit(index: Index) {
		let ix = definition(index);
		let ds = TestIndexStore::new().await;
		let env = CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, AT);
		let err = write(&env, &ix, None, Some(columns())).await.unwrap_err();
		assert_cancelled(&err);
		assert_eq!(entries(&env.tx(), &ix).await, AT - 1, "stopped at the checkpoint that fired");
		assert_commit_refused(&env.tx()).await;
		assert_eq!(committed_entries(&ds, &ix).await, 0);
	}

	async fn cancelled_delete_refuses_the_commit(index: Index) {
		let ix = definition(index);
		let ds = committed_document(&ix).await;
		let env = CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, AT);
		let err = write(&env, &ix, Some(columns()), None).await.unwrap_err();
		assert_cancelled(&err);
		assert_eq!(
			entries(&env.tx(), &ix).await,
			ENTRIES - (AT - 1),
			"stopped at the checkpoint that fired"
		);
		assert_commit_refused(&env.tx()).await;
		assert_eq!(committed_entries(&ds, &ix).await, ENTRIES, "the committed document is intact");
	}

	#[tokio::test]
	async fn cancelling_a_unique_create_mid_fan_out_fails_it_and_refuses_the_commit() {
		cancelled_create_refuses_the_commit(Index::Uniq).await;
	}

	#[tokio::test]
	async fn cancelling_a_non_unique_create_mid_fan_out_fails_it_and_refuses_the_commit() {
		cancelled_create_refuses_the_commit(Index::Idx).await;
	}

	#[tokio::test]
	async fn cancelling_a_unique_delete_mid_fan_out_fails_it_and_refuses_the_commit() {
		cancelled_delete_refuses_the_commit(Index::Uniq).await;
	}

	#[tokio::test]
	async fn cancelling_a_non_unique_delete_mid_fan_out_fails_it_and_refuses_the_commit() {
		cancelled_delete_refuses_the_commit(Index::Idx).await;
	}

	#[tokio::test]
	async fn a_deadline_mid_fan_out_is_reported_as_a_timeout_not_a_cancellation() {
		let timeout = Duration::from_millis(250);
		for index in [Index::Uniq, Index::Idx] {
			let ix = definition(index);
			let ds = TestIndexStore::new().await;
			let env =
				CancelAtCheckpoint::timing_out(ds.env(TransactionType::Write).await, AT, timeout);
			let err = write(&env, &ix, None, Some(columns())).await.unwrap_err();
			match err.downcast_ref::<EngineError>() {
				Some(EngineError::QueryTimedout(d)) => assert_eq!(*d, timeout),
				_ => panic!("expected a timeout, got {err:#}"),
			}
			assert_commit_refused(&env.tx()).await;
			assert_eq!(committed_entries(&ds, &ix).await, 0);
		}
	}
}

#[cfg(test)]
mod value_shapes {
	//! The value shape each column of a b-tree write records
	//! ([`super::stored_shapes`]): an array the writer lists by element, or a
	//! value it stores whole.

	use std::collections::BTreeMap;

	use surrealdb_datastore::index_shapes::ValueShape;

	use super::{expected_shapes, recorded_shapes, stored_shapes};
	use crate::catalog::{INDEX_FORMAT_VERSION, Index, IndexDefinition, IndexId};
	use crate::expr::{Idiom, Part};
	use crate::val::{Array, Object, Set, TableName, Value};

	/// An index over `cols`, each a field optionally followed by `parts`.
	fn index(cols: Vec<Idiom>) -> IndexDefinition {
		IndexDefinition {
			index_id: IndexId(1),
			name: "ix".into(),
			table_name: TableName::from("t"),
			cols,
			index: Index::Idx,
			count_cond: None,
			prepare_remove: false,
			format_version: INDEX_FORMAT_VERSION,
			comment: None,
		}
	}

	fn with(field: &str, part: Part) -> Idiom {
		let mut idiom = Idiom::field(field);
		idiom.0.push(part);
		idiom
	}

	fn shapes(vals: &[Value], ix: &IndexDefinition) -> Vec<(u16, ValueShape)> {
		stored_shapes(vals, ix).collect()
	}

	#[test]
	fn a_column_selecting_many_values_is_expected_to_store_arrays() {
		let ix = index(vec![
			Idiom::field("a"),
			with("tags", Part::All),
			with("tags", Part::Flatten),
			with("b", Part::Field("c".into())),
		]);
		assert_eq!(
			expected_shapes(&ix),
			[
				(0, ValueShape::Whole),
				(1, ValueShape::Array),
				(2, ValueShape::Whole),
				(3, ValueShape::Whole),
			]
		);
	}

	#[test]
	fn a_write_records_only_the_shapes_its_columns_are_not_expected_to_store() {
		let ix = index(vec![Idiom::field("a"), with("tags", Part::All)]);
		let array = Value::Array(Array::from(vec![Value::from(1)]));
		let expected = [Value::from("x"), array.clone()];
		assert_eq!(recorded_shapes(&expected, &ix).collect::<Vec<_>>(), []);
		let unexpected = [array, Value::from("x")];
		assert_eq!(
			recorded_shapes(&unexpected, &ix).collect::<Vec<_>>(),
			[(0, ValueShape::Array), (1, ValueShape::Whole)]
		);
	}

	#[test]
	fn a_plain_column_records_an_array_or_a_whole_value() {
		let ix = index(vec![Idiom::field("a"), Idiom::field("b")]);
		let vals = [Value::Array(Array::from(vec![Value::from(1)])), Value::from("x")];
		assert_eq!(shapes(&vals, &ix), [(0, ValueShape::Array), (1, ValueShape::Whole)]);
	}

	#[test]
	fn an_empty_array_is_an_array() {
		let ix = index(vec![Idiom::field("a")]);
		assert_eq!(shapes(&[Value::Array(Array::new())], &ix), [(0, ValueShape::Array)]);
	}

	#[test]
	fn none_and_null_record_nothing() {
		let ix = index(vec![Idiom::field("a"), Idiom::field("b")]);
		assert_eq!(shapes(&[Value::None, Value::Null], &ix), []);
	}

	#[test]
	fn an_element_column_stores_a_string_an_object_and_a_set_whole() {
		let ix = index(vec![with("tags", Part::All)]);
		let object =
			Value::Object(Object::from(BTreeMap::from([("x".to_owned(), Value::from(1))])));
		let set = Value::Set(Set::from(std::collections::BTreeSet::from([Value::from("x")])));
		for value in [Value::from("x"), object, set] {
			assert_eq!(
				shapes(std::slice::from_ref(&value), &ix),
				[(0, ValueShape::Whole)],
				"{value:?}"
			);
		}
		let array = Value::Array(Array::from(vec![Value::from("x")]));
		assert_eq!(shapes(&[array], &ix), [(0, ValueShape::Array)]);
	}

	#[test]
	fn a_flattened_column_stores_an_array_whole() {
		let ix = index(vec![with("tags", Part::Flatten)]);
		let array = Value::Array(Array::from(vec![Value::from("x")]));
		assert_eq!(shapes(&[array], &ix), [(0, ValueShape::Whole)]);
	}
}
