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
use std::sync::Arc;

use anyhow::{Result, bail};
use surrealdb_datastore::index_shapes::{ShapeRecorder, ValueShape};
use surrealdb_datastore::{Transaction, storage_error};
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	AnalyzerDefinition, CondText, DatabaseId, DiskAnnParams, FullTextParams, HnswParams, Index,
	IndexDefinition, NamespaceId, TableId,
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
use crate::trees::hnsw::index::{
	HnswCompactionError, HnswCompactionOutcome, HnswCompactionPlan, HnswIndex,
};
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
	/// The record's doc-ID in the table's space, when the caller has resolved
	/// it ([`Self::with_doc_id`]).
	doc_id: Option<DocId>,
}

type SingleGeometryCover = (Vec<crate::spatial::SpatialCellId>, Option<(f64, f64)>);

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
			doc_id: None,
		}
	}

	/// The same operation for a record whose doc-ID in the table's space the
	/// caller has resolved, or assigned, in the operation's transaction
	/// ([`TableDocIds::resolve_or_assign_many`]): its entries carry that ID.
	pub fn with_doc_id(mut self, doc_id: DocId) -> Self {
		self.doc_id = Some(doc_id);
		self
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

	/// The full-text index `ix` defines, or `None` for an index of any other
	/// kind.
	///
	/// It analyses with `analyzer` when the caller hands one over, and
	/// otherwise with the analyzer `ix` names as `env`'s transaction reads it.
	pub async fn create_fulltext_index(
		env: &dyn IndexEnv,
		ns: NamespaceId,
		db: DatabaseId,
		ix: &IndexDefinition,
		analyzer: Option<Arc<AnalyzerDefinition>>,
	) -> Result<Option<FullTextIndex>> {
		let Index::FullText(p) = &ix.index else {
			return Ok(None);
		};
		let ikb = IndexKeyBase::new(ns, db, ix.table_name.clone(), ix.index_id);
		let ixs = env.index_stores();
		let allow_list = &env.config().file_allowlist;
		let index = match analyzer {
			Some(az) => {
				FullTextIndex::with_analyzer(ixs, az, ikb, p, allow_list, ix.format_version).await?
			}
			None => {
				FullTextIndex::new(ixs, &env.tx(), ikb, p, allow_list, ix.format_version).await?
			}
		};
		Ok(Some(index))
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
			Index::Spatial => self.index_spatial().await,
		}
	}

	/// Where the entry for values `v` of this record is stored: the unique
	/// slot on a `UNIQUE` index for a tuple holding no `NONE`, and no `NULL`
	/// unless the index is `NULLS NOT DISTINCT`, and otherwise the non-unique
	/// key naming the record
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
		let doc_id: Option<DocId> = match (self.ix.has_entry_doc_ids(), self.doc_id) {
			(false, _) => None,
			(true, Some(doc_id)) => Some(doc_id),
			(true, None) => {
				let doc_ids = TableDocIds::new(self.ns, self.db, self.table_name.clone());
				Some(doc_ids.resolve_or_assign(self.env, &self.rid.key).await?)
			}
		};
		Ok(IndexEntryValue {
			rid: self.rid.clone(),
			doc_id,
			point: None,
		})
	}

	/// Whether `e` is a conditional write refused because the key did not
	/// hold the expected value.
	fn is_condition_not_met(e: &anyhow::Error) -> bool {
		matches!(storage_error(e), Some(surrealdb_kvs::Error::TransactionConditionNotMet))
	}

	/// Delete an index entry with a guarded (compare) delete, tolerating an
	/// absent or foreign entry.
	///
	/// When the expected value carries a doc-ID and the guarded delete misses,
	/// the delete is retried against the bare record-ID encoding: an entry may
	/// predate the index's doc-ID format (e.g. written by an older binary
	/// during a rolling upgrade) and must still be removable.
	///
	/// An entry left alone is read under a commit-time conflict registration.
	/// The delete is judged on this transaction's snapshot, and an entry for
	/// the record's old value can be written after it — by an index build
	/// replaying the write that produced that value. A miss that registered
	/// nothing would commit beside that entry and leave the record indexed
	/// under a value it no longer holds; registered, the commit fails as a
	/// retryable conflict.
	async fn del_entry<K>(txn: &Transaction, key: &K, expected: &IndexEntryValue) -> Result<()>
	where
		K: key::KVKey<Value = IndexEntryValue> + std::fmt::Debug,
	{
		match txn.del_compare_key(key, Some(expected)).await {
			Err(e) if Self::is_condition_not_met(&e) => {
				if expected.doc_id.is_some() || expected.point.is_some() {
					let bare = IndexEntryValue {
						rid: expected.rid.clone(),
						doc_id: None,
						point: None,
					};
					match txn.del_compare_key(key, Some(&bare)).await {
						Err(e) if Self::is_condition_not_met(&e) => {}
						other => return other,
					}
				}
				txn.getu_key_raw(key).await.map(drop)
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
					// A tuple exempt from uniqueness (holding `NONE`, or `NULL`
					// unless `NULLS NOT DISTINCT`) names the record and is
					// checked for no uniqueness.
					txn.set_key(&key, &value).await?;
				} else {
					match txn.put_compare_key(&key, &value, None).await {
						Ok(()) => {}
						// The key is taken: report the record that holds it. An
						// entry that refused the write and then reads as absent
						// is a corrupt index, which fails the write.
						Err(e) if Self::is_condition_not_met(&e) => {
							let Some(entry) = txn.get_key(&key, None).await? else {
								return Err(surrealdb_datastore::DatastoreError::CorruptedIndex(
									"a unique entry refused a write and is absent",
								)
								.into());
							};
							if entry.rid != *self.rid {
								return self.err_index_exists(entry.rid, n);
							}
						}
						Err(e) => return Err(e),
					}
				}
			}
		}
		Ok(())
	}

	/// Record the shapes of the values `n` this b-tree, spatial or vector index
	/// stores beyond those its columns are expected to ([`recorded_shapes`]), in
	/// the transaction that writes their entries ([`Transaction::record_index_shapes`]).
	async fn record_shapes(&self, txn: &Transaction, n: &[Value]) -> Result<()> {
		if recorded_shapes(n, self.ix).next().is_none() {
			return Ok(());
		}
		let ix = self.ix.index_id;
		let known = txn.index_shapes(self.ns, self.db, &self.table_name, ix).await?;
		if known.records_all(recorded_shapes(n, self.ix)) {
			return Ok(());
		}
		let shapes: Vec<_> = recorded_shapes(n, self.ix).collect();
		txn.record_index_shapes(self.ns, self.db, &self.table_name, ix, &shapes, self.shapes_by)
			.await
	}

	/// Record the shapes of the new values this operation stores
	/// ([`Self::record_shapes`]) without writing them, for a build that hands
	/// the values to the index another way.
	pub async fn record_value_shapes(&self) -> Result<()> {
		match &self.n {
			Some(n) => self.record_shapes(&self.env.tx(), n).await,
			None => Ok(()),
		}
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

	/// Stream the Hilbert covering cells for a single element of a trailing
	/// `Array` or `Set` spatial column directly to `txn`.
	///
	/// Each geometry produces at most `cfg.max_cells` cells, which are written or
	/// deleted immediately with a per-cell cancellation checkpoint; an element
	/// that yields no cells still runs a checkpoint so a large container of
	/// non-geometry values cannot spin unchecked.
	#[expect(clippy::too_many_arguments)]
	async fn apply_spatial_container_elem(
		&self,
		txn: &Transaction,
		prefix: &mut Vec<Value>,
		elem: &Value,
		value: &IndexEntryValue,
		cfg: &crate::spatial::CoverConfig,
		is_delete: bool,
		entry: &mut usize,
	) -> Result<()> {
		let cells = elem
			.geometry_operand()
			.map(|geom| crate::spatial::cover_geometry_for_index(&geom, cfg))
			.unwrap_or_default();
		if cells.is_empty() {
			self.checkpoint(txn, *entry).await?;
			*entry += 1;
			return Ok(());
		}
		for cell in cells {
			self.checkpoint(txn, *entry).await?;
			*entry += 1;
			prefix.push(Value::Number(crate::val::Number::Int(cell.0)));
			let key = self.stored_key(false, prefix)?;
			prefix.pop();
			if is_delete {
				Self::del_entry(txn, &key, value).await?;
			} else {
				txn.set_key(&key, value).await?;
			}
		}
		Ok(())
	}

	/// Extract the sorted, deduplicated covering cells (`<= cfg.max_cells`) and
	/// optional exact `Point` coordinates when `last` is a single geometry.
	fn extract_single_geometry_cells(
		last: &Value,
		cfg: &crate::spatial::CoverConfig,
	) -> Option<SingleGeometryCover> {
		let geom = last.geometry_operand()?;
		let pt = match geom.as_ref() {
			crate::val::Geometry::Point(pt) => Some((pt.x(), pt.y())),
			crate::val::Geometry::Line(_)
			| crate::val::Geometry::Polygon(_)
			| crate::val::Geometry::MultiPoint(_)
			| crate::val::Geometry::MultiLine(_)
			| crate::val::Geometry::MultiPolygon(_)
			| crate::val::Geometry::Collection(_) => None,
		};
		let cells = crate::spatial::cover_geometry_for_index(&geom, cfg);
		Some((cells, pt))
	}

	/// Stream the Hilbert covering cells for `vals` (leading prefix columns plus
	/// trailing spatial column) directly to `txn`, running [`Self::checkpoint`]
	/// before each KV write/delete so neither compound prefix fan-out nor a
	/// trailing geometry `Array`/`Set` is materialized in memory.
	async fn apply_spatial_entries(
		&self,
		txn: &Transaction,
		mut vals: Vec<Value>,
		value: &mut IndexEntryValue,
		cfg: &crate::spatial::CoverConfig,
		is_delete: bool,
	) -> Result<()> {
		let Some(last) = vals.pop() else {
			return Ok(());
		};
		let mut entry = 0usize;
		if let Some((cells, pt)) = Self::extract_single_geometry_cells(&last, cfg) {
			if cells.is_empty() {
				return Ok(());
			}
			value.point = pt;
			for mut prefix in Indexable::new(vals, self.ix) {
				for cell in &cells {
					self.checkpoint(txn, entry).await?;
					entry += 1;
					prefix.0.push(Value::Number(crate::val::Number::Int(cell.0)));
					let key = self.stored_key(false, &prefix.0)?;
					prefix.0.pop();
					if is_delete {
						Self::del_entry(txn, &key, value).await?;
					} else {
						txn.set_key(&key, value).await?;
					}
				}
			}
		} else {
			let has_elements = match &last {
				Value::Array(arr) => !arr.is_empty(),
				Value::Set(set) => !set.is_empty(),
				Value::None
				| Value::Null
				| Value::Bool(_)
				| Value::Number(_)
				| Value::String(_)
				| Value::Duration(_)
				| Value::Datetime(_)
				| Value::Date(_)
				| Value::Time(_)
				| Value::Timestamp(_)
				| Value::Interval(_)
				| Value::Uuid(_)
				| Value::Bytes(_)
				| Value::Table(_)
				| Value::RecordId(_)
				| Value::Object(_)
				| Value::Geometry(_)
				| Value::File(_)
				| Value::Range(_)
				| Value::Regex(_)
				| Value::Closure(_) => false,
			};
			if !has_elements {
				return Ok(());
			}
			value.point = None;
			for mut prefix in Indexable::new(vals, self.ix) {
				match &last {
					Value::Array(arr) => {
						for elem in arr.iter() {
							if let Value::Set(set) = elem {
								if set.is_empty() {
									self.checkpoint(txn, entry).await?;
									entry += 1;
								} else {
									for sub in set.iter() {
										self.apply_spatial_container_elem(
											txn,
											&mut prefix.0,
											sub,
											value,
											cfg,
											is_delete,
											&mut entry,
										)
										.await?;
									}
								}
							} else {
								self.apply_spatial_container_elem(
									txn,
									&mut prefix.0,
									elem,
									value,
									cfg,
									is_delete,
									&mut entry,
								)
								.await?;
							}
						}
					}
					Value::Set(set) => {
						for elem in set.iter() {
							self.apply_spatial_container_elem(
								txn,
								&mut prefix.0,
								elem,
								value,
								cfg,
								is_delete,
								&mut entry,
							)
							.await?;
						}
					}
					Value::None
					| Value::Null
					| Value::Bool(_)
					| Value::Number(_)
					| Value::String(_)
					| Value::Duration(_)
					| Value::Datetime(_)
					| Value::Date(_)
					| Value::Time(_)
					| Value::Timestamp(_)
					| Value::Interval(_)
					| Value::Uuid(_)
					| Value::Bytes(_)
					| Value::Table(_)
					| Value::RecordId(_)
					| Value::Object(_)
					| Value::Geometry(_)
					| Value::File(_)
					| Value::Range(_)
					| Value::Regex(_)
					| Value::Closure(_) => {}
				}
			}
		}
		Ok(())
	}

	async fn index_spatial(&mut self) -> Result<()> {
		let txn = self.env.tx();
		let mut value = self.entry_value().await?;
		let cfg = crate::spatial::CoverConfig::default_index();

		// When updating a single-geometry value whose leading prefix columns are
		// unchanged, diff the bounded cell coverings (`<= cfg.max_cells`) so
		// shared cells with identical trailer payloads are neither deleted nor
		// rewritten.
		if let (Some(o_vals), Some(n_vals)) = (&self.o, &self.n)
			&& let (Some((o_last, o_prefix)), Some((n_last, n_prefix))) =
				(o_vals.split_last(), n_vals.split_last())
			&& o_prefix == n_prefix
			&& let (Some((mut old_cells, old_pt)), Some((mut new_cells, new_pt))) = (
				Self::extract_single_geometry_cells(o_last, &cfg),
				Self::extract_single_geometry_cells(n_last, &cfg),
			) {
			let _ = self.o.take();
			let n = self.n.take().unwrap_or_default();
			self.record_shapes(&txn, &n).await?;
			let same_pt = match (old_pt, new_pt) {
				(None, None) => true,
				(Some((ox, oy)), Some((nx, ny))) => {
					ox.to_bits() == nx.to_bits() && oy.to_bits() == ny.to_bits()
				}
				_ => false,
			};
			if same_pt {
				let orig_old = old_cells.clone();
				old_cells.retain(|c| !new_cells.contains(c));
				new_cells.retain(|c| !orig_old.contains(c));
			}
			let mut prefix_vals = n;
			prefix_vals.pop();
			if !old_cells.is_empty() || !new_cells.is_empty() {
				let mut entry = 0usize;
				for mut prefix in Indexable::new(prefix_vals, self.ix) {
					value.point = old_pt;
					for cell in &old_cells {
						self.checkpoint(&txn, entry).await?;
						entry += 1;
						prefix.0.push(Value::Number(crate::val::Number::Int(cell.0)));
						let key = self.stored_key(false, &prefix.0)?;
						prefix.0.pop();
						Self::del_entry(&txn, &key, &value).await?;
					}
					value.point = new_pt;
					for cell in &new_cells {
						self.checkpoint(&txn, entry).await?;
						entry += 1;
						prefix.0.push(Value::Number(crate::val::Number::Int(cell.0)));
						let key = self.stored_key(false, &prefix.0)?;
						prefix.0.pop();
						txn.set_key(&key, &value).await?;
					}
				}
			}
			return Ok(());
		}

		if let Some(o) = self.o.take() {
			self.apply_spatial_entries(&txn, o, &mut value, &cfg, true).await?;
		}
		if let Some(n) = self.n.take() {
			self.record_shapes(&txn, &n).await?;
			self.apply_spatial_entries(&txn, n, &mut value, &cfg, false).await?;
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

	/// Creates the read-phase plan for HNSW pending compaction of an index
	/// stamped with `format_version`.
	pub async fn prepare_hnsw_compaction(
		env: &dyn IndexEnv,
		ikb: &IndexKeyBase,
		format_version: u16,
	) -> Result<HnswCompactionPlan> {
		HnswIndex::prepare_compaction(env, ikb, format_version).await
	}

	/// Applies a prepared HNSW pending compaction plan.
	///
	/// Reports [`HnswCompactionOutcome::Empty`] when there is no work or the
	/// table is gone, and [`HnswCompactionOutcome::LostRace`] when another
	/// compactor advanced the generation first or a captured pending key changed
	/// before the write. What the other outcomes owe a caller whose commit then
	/// fails is on [`HnswCompactionOutcome`]; on error, [`HnswCompactionError::in_place`]
	/// tells the caller whether the resident graph was mutated before the failure.
	pub async fn apply_hnsw_compaction(
		env: &dyn IndexEnv,
		ixs: &IndexStores,
		ikb: &IndexKeyBase,
		p: &HnswParams,
		format_version: u16,
		plan: HnswCompactionPlan,
	) -> std::result::Result<HnswCompactionOutcome, HnswCompactionError> {
		if !plan.has_work() {
			return Ok(HnswCompactionOutcome::Empty);
		}
		let tx = env.tx();
		if let Some(tb) = tx
			.get_tb(ikb.ns(), ikb.db(), ikb.table(), None)
			.await
			.map_err(HnswCompactionError::untouched)?
		{
			let hnsw = ixs
				.get_index_hnsw(env, tb.table_id, ikb, p, format_version)
				.await
				.map_err(HnswCompactionError::untouched)?;
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
	/// Registers a compaction request for the index with the transaction,
	/// which writes one `Ic` key for it at commit however many documents the
	/// transaction mutates under the index. The index compaction task later
	/// drains the queue and performs the actual compaction via
	/// [`Datastore::index_compaction`].
	///
	/// Every request writes its own unique key (node id plus a fresh UUIDv7)
	/// rather than updating one shared per-index key: blind writes of
	/// distinct keys never contend, whereas a shared key would put every
	/// concurrent transaction touching the same index into write-write
	/// conflict (and a read-modify-write dedup would be worse on
	/// last-writer-wins backends). The queue therefore grows with the number
	/// of committed transactions touching each index, and deduplication
	/// across transactions happens at drain time instead:
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
		self.record_value_shapes().await?;
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
			self.record_value_shapes().await?;
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

/// The shape a single geometry value stores in the trailing spatial column.
fn spatial_geometry_shape(geom: &crate::val::Geometry) -> ValueShape {
	match geom {
		crate::val::Geometry::Point(pt) => {
			if !pt.x().is_finite() || !pt.y().is_finite() {
				ValueShape::NonGeometry
			} else if crate::spatial::point_in_wgs84_bounds(pt.x(), pt.y()) {
				ValueShape::Whole
			} else {
				ValueShape::MultiCell
			}
		}
		crate::val::Geometry::Line(_)
		| crate::val::Geometry::Polygon(_)
		| crate::val::Geometry::MultiPoint(_)
		| crate::val::Geometry::MultiLine(_)
		| crate::val::Geometry::MultiPolygon(_)
		| crate::val::Geometry::Collection(_) => ValueShape::MultiCell,
	}
}

/// The shape a value in the trailing geometry column of a `SPATIAL` index
/// stores.
fn spatial_column_shape(v: &Value) -> ValueShape {
	if let Some(geom) = v.geometry_operand() {
		return spatial_geometry_shape(&geom);
	}
	match v {
		Value::Array(_) => ValueShape::Array,
		Value::Set(set) => {
			let has_geom = set.iter().any(|elem| {
				elem.geometry_operand().is_some_and(|g| {
					matches!(spatial_geometry_shape(&g), ValueShape::Whole | ValueShape::MultiCell)
				})
			});
			if has_geom {
				ValueShape::MultiCell
			} else {
				ValueShape::NonGeometry
			}
		}
		Value::None
		| Value::Null
		| Value::Bool(_)
		| Value::Number(_)
		| Value::String(_)
		| Value::Duration(_)
		| Value::Datetime(_)
		| Value::Date(_)
		| Value::Time(_)
		| Value::Timestamp(_)
		| Value::Interval(_)
		| Value::Uuid(_)
		| Value::Bytes(_)
		| Value::Table(_)
		| Value::RecordId(_)
		| Value::Object(_)
		| Value::Geometry(_)
		| Value::File(_)
		| Value::Range(_)
		| Value::Regex(_)
		| Value::Closure(_) => ValueShape::NonGeometry,
	}
}

/// The shape the writer stores each column of `vals` in, for B-tree or
/// `SPATIAL` index `ix`, by column position ([`ValueShape`]). A column whose
/// idiom ends in a flatten stores its value whole, an array included; any other
/// column lists an array once per element. For the trailing geometry column of
/// a `SPATIAL` index, a single in-bounds `Point` is stored at one leaf cell
/// ([`ValueShape::Whole`]), any non-point geometry, out-of-bounds point, or
/// multi-geometry set that emits multiple or coarse ancestor cells (`L < 28`)
/// records [`ValueShape::MultiCell`], an array records [`ValueShape::Array`],
/// and any other value (including `NONE`, `NULL`, a string, number, or
/// non-finite point that emits no spatial cells) records
/// [`ValueShape::NonGeometry`]. For B-tree columns, `NONE` and `NULL` record no
/// shape. For the vector column of an HNSW or DiskANN index, `NONE` and `NULL`,
/// which the index stores no entry for, record [`ValueShape::Absent`], an array
/// of arrays, which it flattens into one vector, records [`ValueShape::Nested`],
/// and a flat vector records nothing.
pub fn stored_shapes<'v>(
	vals: &'v [Value],
	ix: &'v IndexDefinition,
) -> impl Iterator<Item = (u16, ValueShape)> + 'v {
	let is_spatial = matches!(ix.index, Index::Spatial);
	let last_col = ix.cols.len().saturating_sub(1);
	let is_vector = matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_));
	vals.iter().zip(ix.cols.iter()).zip(0u16..).filter_map(move |((v, col), i)| {
		if is_vector {
			if v.is_nullish() {
				return Some((i, ValueShape::Absent));
			}
			let nested =
				matches!(v, Value::Array(a) if a.iter().any(|e| matches!(e, Value::Array(_))));
			return nested.then_some((i, ValueShape::Nested));
		}
		if is_spatial && usize::from(i) == last_col {
			return Some((i, spatial_column_shape(v)));
		}
		if v.is_nullish() {
			return None;
		}
		let shape = if matches!(v, Value::Array(_)) && !stores_arrays_whole(col) {
			ValueShape::Array
		} else {
			ValueShape::Whole
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
	// A vector index records only the records it holds no entry for, which no column expects.
	if matches!(ix.index, Index::Hnsw(_) | Index::DiskAnn(_)) {
		return Vec::new();
	}
	ix.cols.iter().zip(0u16..).map(|(col, i)| (i, expected_shape(col))).collect()
}

/// The shapes of `vals` a write to B-tree, spatial or vector index `ix` records: those
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
		let cols = if matches!(index, Index::Spatial) {
			vec![Idiom::field("a"), Idiom::field("b"), Idiom::field("loc")]
		} else {
			vec![Idiom::field("a"), Idiom::field("b")]
		};
		IndexDefinition {
			index_id: IndexId(4),
			name: "ix".into(),
			table_name: TableName::from("t"),
			cols,
			index,
			count_cond: None,
			prepare_remove: false,
			format_version: INDEX_FORMAT_VERSION,
			comment: None,
			nulls_not_distinct: false,
		}
	}

	fn record() -> RecordId {
		RecordId::new(TableName::from("t"), RecordIdKey::from("doc".to_owned()))
	}

	/// The indexed values of the fixture document: two `SIDE`-element arrays
	/// (plus a single in-bounds point when `ix` is a `SPATIAL` index).
	fn columns_for(ix: &IndexDefinition) -> Vec<Value> {
		let side = || Value::Array(Array::from((0..SIDE).map(Value::from).collect::<Vec<_>>()));
		if matches!(ix.index, Index::Spatial) {
			vec![side(), side(), Value::Geometry(crate::val::Geometry::Point((-0.12, 51.5).into()))]
		} else {
			vec![side(), side()]
		}
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
		write(&env, ix, None, Some(columns_for(ix))).await.unwrap();
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
		let err = write(&env, &ix, None, Some(columns_for(&ix))).await.unwrap_err();
		assert_cancelled(&err);
		assert_eq!(entries(&env.tx(), &ix).await, AT - 1, "stopped at the checkpoint that fired");
		assert_commit_refused(&env.tx()).await;
		assert_eq!(committed_entries(&ds, &ix).await, 0);
	}

	async fn cancelled_delete_refuses_the_commit(index: Index) {
		let ix = definition(index);
		let ds = committed_document(&ix).await;
		let env = CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, AT);
		let err = write(&env, &ix, Some(columns_for(&ix)), None).await.unwrap_err();
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
	async fn cancelling_a_spatial_create_mid_fan_out_fails_it_and_refuses_the_commit() {
		cancelled_create_refuses_the_commit(Index::Spatial).await;
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
	async fn cancelling_a_spatial_delete_mid_fan_out_fails_it_and_refuses_the_commit() {
		cancelled_delete_refuses_the_commit(Index::Spatial).await;
	}

	#[tokio::test]
	async fn cancelling_a_spatial_container_fan_out_streams_entries_and_refuses_the_commit() {
		let ix = IndexDefinition {
			index_id: IndexId(5),
			name: "ix_loc".into(),
			table_name: TableName::from("t"),
			cols: vec![Idiom::field("loc")],
			index: Index::Spatial,
			count_cond: None,
			prepare_remove: false,
			format_version: INDEX_FORMAT_VERSION,
			comment: None,
			nulls_not_distinct: false,
		};
		let points: Vec<Value> = (0..ENTRIES)
			.map(|i| Value::Geometry(crate::val::Geometry::Point((i as f64, 0.0).into())))
			.collect();
		let containers = [
			Value::Array(Array::from(points.clone())),
			Value::Set(crate::val::Set::from(
				points.into_iter().collect::<std::collections::BTreeSet<_>>(),
			)),
		];
		for container in containers {
			let cols = vec![container];

			// Create: entries are streamed directly to `txn` before the
			// cancellation checkpoint fires at `AT`.
			let ds = TestIndexStore::new().await;
			let env = CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, AT);
			let err = write(&env, &ix, None, Some(cols.clone())).await.unwrap_err();
			assert_cancelled(&err);
			assert_eq!(
				entries(&env.tx(), &ix).await,
				AT - 1,
				"streamed container entries up to the checkpoint that fired"
			);
			assert_commit_refused(&env.tx()).await;
			assert_eq!(committed_entries(&ds, &ix).await, 0);

			// Commit the full container, then cancel mid-delete.
			let env_ok = ds.env(TransactionType::Write).await;
			write(&env_ok, &ix, None, Some(cols.clone())).await.unwrap();
			env_ok.tx().commit().await.unwrap();
			assert_eq!(committed_entries(&ds, &ix).await, ENTRIES);

			let env_del = CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, AT);
			let err = write(&env_del, &ix, Some(cols), None).await.unwrap_err();
			assert_cancelled(&err);
			assert_eq!(
				entries(&env_del.tx(), &ix).await,
				ENTRIES - (AT - 1),
				"streamed container deletes up to the checkpoint that fired"
			);
			assert_commit_refused(&env_del.tx()).await;
			assert_eq!(committed_entries(&ds, &ix).await, ENTRIES);
		}
	}

	#[tokio::test]
	async fn updating_a_single_spatial_geometry_diffs_shared_covering_cells() {
		let ix = IndexDefinition {
			index_id: IndexId(6),
			name: "ix_poly".into(),
			table_name: TableName::from("t"),
			cols: vec![Idiom::field("loc")],
			index: Index::Spatial,
			count_cond: None,
			prepare_remove: false,
			format_version: INDEX_FORMAT_VERSION,
			comment: None,
			nulls_not_distinct: false,
		};
		let cfg = crate::spatial::CoverConfig::default_index();
		let old_geom = crate::val::Geometry::Polygon(geo::Polygon::new(
			geo::LineString::from(vec![
				(0.0, 0.0),
				(10.0, 0.0),
				(10.0, 10.0),
				(0.0, 10.0),
				(0.0, 0.0),
			]),
			vec![],
		));
		let new_geom = crate::val::Geometry::Polygon(geo::Polygon::new(
			geo::LineString::from(vec![
				(0.0, 0.0),
				(10.1, 0.0),
				(10.1, 10.0),
				(0.0, 10.0),
				(0.0, 0.0),
			]),
			vec![],
		));
		let old_cells = crate::spatial::cover_geometry_for_index(&old_geom, &cfg);
		let new_cells = crate::spatial::cover_geometry_for_index(&new_geom, &cfg);
		let shared = old_cells.iter().filter(|c| new_cells.contains(c)).count();
		assert!(shared > 0, "expected nudged polygons to share covering cells");
		let diff_ops = (old_cells.len() - shared) + (new_cells.len() - shared);
		assert!(diff_ops < old_cells.len() + new_cells.len());

		let old_cols = vec![Value::Geometry(old_geom)];
		let new_cols = vec![Value::Geometry(new_geom)];

		let ds = TestIndexStore::new().await;
		let env_create = ds.env(TransactionType::Write).await;
		write(&env_create, &ix, None, Some(old_cols.clone())).await.unwrap();
		env_create.tx().commit().await.unwrap();
		assert_eq!(committed_entries(&ds, &ix).await, old_cells.len());

		// Identical geometry update performs 0 KV writes/checkpoints.
		let env_noop = CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, 1);
		write(&env_noop, &ix, Some(old_cols.clone()), Some(old_cols.clone())).await.unwrap();
		env_noop.tx().commit().await.unwrap();

		// Nudged geometry update only touches `diff_ops` cells rather than `old + new`.
		let env_update =
			CancelAtCheckpoint::new(ds.env(TransactionType::Write).await, diff_ops + 1);
		write(&env_update, &ix, Some(old_cols), Some(new_cols)).await.unwrap();
		env_update.tx().commit().await.unwrap();
		assert_eq!(committed_entries(&ds, &ix).await, new_cells.len());
	}

	#[tokio::test]
	async fn a_deadline_mid_fan_out_is_reported_as_a_timeout_not_a_cancellation() {
		let timeout = Duration::from_millis(250);
		for index in [Index::Uniq, Index::Idx, Index::Spatial] {
			let ix = definition(index);
			let ds = TestIndexStore::new().await;
			let env =
				CancelAtCheckpoint::timing_out(ds.env(TransactionType::Write).await, AT, timeout);
			let err = write(&env, &ix, None, Some(columns_for(&ix))).await.unwrap_err();
			match err.downcast_ref::<EngineError>() {
				Some(EngineError::QueryTimedout(d)) => assert_eq!(*d, timeout),
				_ => panic!("expected a timeout, got {err:#}"),
			}
			assert_commit_refused(&env.tx()).await;
			assert_eq!(committed_entries(&ds, &ix).await, 0);
		}
	}

	#[tokio::test]
	async fn unique_fan_out_repeating_an_element_within_one_record_succeeds() {
		let ix = definition(Index::Uniq);
		let ds = TestIndexStore::new().await;
		let env = ds.env(TransactionType::Write).await;
		let dup = Value::Array(Array::from(vec![Value::from("a"), Value::from("a")]));
		let single = Value::Array(Array::from(vec![Value::from("x")]));
		write(&env, &ix, None, Some(vec![dup.clone(), single.clone()])).await.unwrap();
		env.tx().commit().await.unwrap();
		assert_eq!(committed_entries(&ds, &ix).await, 1);

		// A second distinct record with the same element must still be rejected.
		let env2 = ds.env(TransactionType::Write).await;
		let other_rid = RecordId::new(TableName::from("t"), RecordIdKey::from("other".to_owned()));
		let mut op =
			IndexOperation::new(&env2, NS, DB, TB, &ix, None, Some(vec![dup, single]), &other_rid);
		let mut require_compaction = false;
		let err = op.compute(&NoAnalyzerFunction, &mut require_compaction).await.unwrap_err();
		assert!(
			matches!(err.downcast_ref::<crate::Error>(), Some(crate::Error::IndexExists { .. })),
			"expected IndexExists for another record, got {err:#}"
		);
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
			nulls_not_distinct: false,
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

	#[test]
	fn spatial_trailing_column_records_whole_only_for_finite_in_bounds_points() {
		let mut ix = index(vec![Idiom::field("loc")]);
		ix.index = Index::Spatial;

		let valid_pt = Value::Geometry(crate::val::Geometry::Point((-0.12, 51.5).into()));
		assert_eq!(shapes(std::slice::from_ref(&valid_pt), &ix), [(0, ValueShape::Whole)]);

		let oob_pt = Value::Geometry(crate::val::Geometry::Point((200.0, 51.5).into()));
		assert_eq!(shapes(&[oob_pt], &ix), [(0, ValueShape::MultiCell)]);

		let line = Value::Geometry(crate::val::Geometry::Line(geo::LineString::from(vec![
			(0.0, 0.0),
			(1.0, 1.0),
		])));
		assert_eq!(shapes(&[line], &ix), [(0, ValueShape::MultiCell)]);

		let single_pt_set =
			Value::Set(Set::from(std::collections::BTreeSet::from([valid_pt.clone()])));
		assert_eq!(shapes(&[single_pt_set], &ix), [(0, ValueShape::MultiCell)]);

		let single_pt_array = Value::Array(Array::from(vec![valid_pt]));
		assert_eq!(shapes(std::slice::from_ref(&single_pt_array), &ix), [(0, ValueShape::Array)]);

		let mut flat_ix = index(vec![with("loc", Part::Flatten)]);
		flat_ix.index = Index::Spatial;
		assert_eq!(shapes(&[single_pt_array], &flat_ix), [(0, ValueShape::Array)]);

		let geojson_pt = Value::Object(Object::from(BTreeMap::from([
			("type".to_owned(), Value::from("Point")),
			(
				"coordinates".to_owned(),
				Value::Array(Array::from(vec![Value::from(1.0), Value::from(1.0)])),
			),
		])));
		assert_eq!(shapes(&[geojson_pt], &ix), [(0, ValueShape::Whole)]);

		let geojson_oob_pt = Value::Object(Object::from(BTreeMap::from([
			("type".to_owned(), Value::from("Point")),
			(
				"coordinates".to_owned(),
				Value::Array(Array::from(vec![Value::from(200.0), Value::from(1.0)])),
			),
		])));
		assert_eq!(shapes(&[geojson_oob_pt], &ix), [(0, ValueShape::MultiCell)]);

		let geojson_line = Value::Object(Object::from(BTreeMap::from([
			("type".to_owned(), Value::from("LineString")),
			(
				"coordinates".to_owned(),
				Value::Array(Array::from(vec![
					Value::Array(Array::from(vec![Value::from(0.0), Value::from(0.0)])),
					Value::Array(Array::from(vec![Value::from(1.0), Value::from(1.0)])),
				])),
			),
		])));
		assert_eq!(shapes(&[geojson_line], &ix), [(0, ValueShape::MultiCell)]);

		let nan_pt = Value::Geometry(crate::val::Geometry::Point((f64::NAN, 51.5).into()));
		assert_eq!(shapes(&[nan_pt], &ix), [(0, ValueShape::NonGeometry)]);

		let non_geom = Value::from("not a point");
		assert_eq!(shapes(&[non_geom], &ix), [(0, ValueShape::NonGeometry)]);

		assert_eq!(shapes(&[Value::None], &ix), [(0, ValueShape::NonGeometry)]);
		assert_eq!(shapes(&[Value::Null], &ix), [(0, ValueShape::NonGeometry)]);
		assert_eq!(
			recorded_shapes(&[Value::None], &ix).collect::<Vec<_>>(),
			[(0, ValueShape::NonGeometry)]
		);
		assert_eq!(
			recorded_shapes(&[Value::Null], &ix).collect::<Vec<_>>(),
			[(0, ValueShape::NonGeometry)]
		);
	}

	#[test]
	fn a_vector_column_records_only_a_value_distance_does_not_measure() {
		let ix = IndexDefinition {
			index: Index::Hnsw(crate::catalog::HnswParams {
				dimension: 2,
				distance: crate::catalog::Distance::Euclidean,
				vector_type: crate::catalog::VectorType::F32,
				m: 12,
				m0: 24,
				ml: crate::val::Number::from(0.4),
				ef_construction: 150,
				extend_candidates: false,
				keep_pruned_connections: false,
				use_hashed_vector: false,
			}),
			..index(vec![Idiom::field("v")])
		};
		assert_eq!(expected_shapes(&ix), []);
		let vector = Value::Array(Array::from(vec![Value::from(1.0), Value::from(2.0)]));
		assert_eq!(shapes(std::slice::from_ref(&vector), &ix), []);
		assert_eq!(recorded_shapes(&[vector], &ix).collect::<Vec<_>>(), []);
		for absent in [Value::None, Value::Null] {
			assert_eq!(shapes(std::slice::from_ref(&absent), &ix), [(0, ValueShape::Absent)]);
			assert_eq!(
				recorded_shapes(&[absent], &ix).collect::<Vec<_>>(),
				[(0, ValueShape::Absent)]
			);
		}
		let nested = Value::Array(Array::from(vec![
			Value::Array(Array::from(vec![Value::from(1.0)])),
			Value::Array(Array::from(vec![Value::from(2.0)])),
		]));
		assert_eq!(recorded_shapes(&[nested], &ix).collect::<Vec<_>>(), [(0, ValueShape::Nested)]);
	}
}
