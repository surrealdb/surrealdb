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

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::{
	DatabaseId, FullTextParams, HnswParams, Index, IndexDefinition, NamespaceId, TableId,
};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Cond, Part};
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::planner::iterators::IndexCountThingIterator;
use crate::key;
use crate::key::index::iu::IndexCountKey;
use crate::key::root::ic::IndexCompactionKey;
use crate::kvs::Transaction;
use crate::val::{Array, RecordId, Value};

pub(crate) struct IndexOperation<'a> {
	ctx: &'a Context,
	opt: &'a Options,
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableId,
	ix: &'a IndexDefinition,
	ikb: IndexKeyBase,
	/// The old values (if existing)
	o: Option<Vec<Value>>,
	/// The new values (if existing)
	n: Option<Vec<Value>>,
	rid: &'a RecordId,
}

impl<'a> IndexOperation<'a> {
	#[expect(clippy::too_many_arguments)]
	pub(crate) fn new(
		ctx: &'a Context,
		opt: &'a Options,
		ns: NamespaceId,
		db: DatabaseId,
		tb: TableId,
		ix: &'a IndexDefinition,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
		rid: &'a RecordId,
	) -> Self {
		Self {
			ctx,
			opt,
			ns,
			db,
			tb,
			ix,
			ikb: IndexKeyBase::new(ns, db, &ix.table_name, ix.index_id),
			o,
			n,
			rid,
		}
	}

	pub(crate) async fn compute(
		&mut self,
		stk: &mut Stk,
		require_compaction: &mut bool,
	) -> Result<()> {
		// Index operation dispatching
		match &self.ix.index {
			Index::Uniq => self.index_unique().await,
			Index::Idx => self.index_non_unique().await,
			Index::FullText(p) => self.index_fulltext(stk, p, require_compaction).await,
			Index::Hnsw(p) => self.index_hnsw(p).await,
			Index::Count(c) => self.index_count(stk, c.as_ref(), require_compaction).await,
		}
	}

	/// Build the KV key for a unique index. The Array encodes values in
	/// a canonical, lexicographically ordered byte form which normalizes numeric
	/// types (Int/Float/Decimal). This means equal numeric values like 0, 0.0 and
	/// 0dec map to the same index key and therefore conflict on UNIQUE indexes.
	fn get_unique_index_key(&self, v: &'a Array) -> Result<key::index::Index<'_>> {
		Ok(key::index::Index::new(self.ns, self.db, &self.ix.table_name, self.ix.index_id, v, None))
	}

	/// Build the KV key for a non-unique index. The record id is appended
	/// to the encoded field values so multiple records can share the same field
	/// bytes; numeric values inside fd are normalized via Array.
	fn get_non_unique_index_key(&self, v: &'a Array) -> Result<key::index::Index<'_>> {
		Ok(key::index::Index::new(
			self.ns,
			self.db,
			&self.ix.table_name,
			self.ix.index_id,
			v,
			Some(&self.rid.key),
		))
	}

	async fn index_unique(&mut self) -> Result<()> {
		// Get the transaction
		let txn = self.ctx.tx();
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for o in i {
				let key = self.get_unique_index_key(&o)?;
				match txn.delc(&key, Some(self.rid)).await {
					Err(e) => {
						if matches!(
							e.downcast_ref::<Error>(),
							Some(Error::Kvs(crate::kvs::Error::TransactionConditionNotMet))
						) {
							Ok(())
						} else {
							Err(e)
						}
					}
					Ok(v) => Ok(v),
				}?
			}
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			let i = Indexable::new(n, self.ix);
			for n in i {
				if !n.is_all_none_or_null() {
					let key = self.get_unique_index_key(&n)?;
					if txn.putc(&key, self.rid, None).await.is_err() {
						let key = self.get_unique_index_key(&n)?;
						let rid: RecordId =
							txn.get(&key, None).await?.expect("record should exist");
						return self.err_index_exists(rid, n);
					}
				}
			}
		}
		Ok(())
	}

	async fn index_non_unique(&mut self) -> Result<()> {
		// Lock the transaction
		let txn = self.ctx.tx();
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for o in i {
				let key = self.get_non_unique_index_key(&o)?;
				match txn.delc(&key, Some(self.rid)).await {
					Err(e) => {
						if matches!(
							e.downcast_ref::<Error>(),
							Some(Error::Kvs(crate::kvs::Error::TransactionConditionNotMet))
						) {
							Ok(())
						} else {
							Err(e)
						}
					}
					Ok(v) => Ok(v),
				}?
			}
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			let i = Indexable::new(n, self.ix);
			for n in i {
				let key = self.get_non_unique_index_key(&n)?;
				txn.set(&key, self.rid, None).await?;
			}
		}
		Ok(())
	}

	async fn index_count(
		&mut self,
		_stk: &mut Stk,       // Placeholder for phase 2 (Condition)
		_cond: Option<&Cond>, // Placeholder for phase 2 (Condition)
		require_compaction: &mut bool,
	) -> Result<()> {
		// Phase 2 (Condition)
		// let is_truthy = async |stk: &mut Stk, c: &Cond, d: &CursorDoc| -> Result<bool> {
		// 	Ok(stk.run(|stk| c.0.compute(stk, ctx, opt, Some(d))).await.catch_return()?.is_truthy())
		// };
		let mut relative_count: i8 = 0;
		// Phase 2 - with condition
		// if let Some(c) = cond {
		// 	if self.o.is_some() {
		// 		if is_truthy(stk, c, &self.doc.initial).await? {
		// 			relative_count -= 1;
		// 		}
		// 	}
		// 	if self.n.is_some() {
		// 		if is_truthy(stk, c, &self.doc.current).await? {
		// 			relative_count += 1;
		// 		}
		// 	}
		// } else {
		if self.o.is_some() {
			relative_count -= 1;
		}
		if self.n.is_some() {
			relative_count += 1;
		}
		// }
		if relative_count == 0 {
			return Ok(());
		}
		let key = IndexCountKey::new(
			self.ns,
			self.db,
			&self.ix.table_name,
			self.ix.index_id,
			Some((self.opt.id(), uuid::Uuid::now_v7())),
			relative_count > 0,
			relative_count.unsigned_abs() as u64,
		);
		self.ctx.tx().put(&key, &(), None).await?;
		*require_compaction = true;
		Ok(())
	}

	pub(crate) async fn index_count_compaction(
		ic: &IndexCompactionKey<'_>,
		tx: &Transaction,
	) -> Result<()> {
		IndexCountThingIterator::new(ic.ns, ic.db, ic.tb.as_ref(), ic.ix)?.compaction(ic, tx).await
	}

	/// Construct a consistent uniqueness violation error message.
	/// Formats the conflicting value as a single value or array depending on
	/// the number of indexed fields.
	fn err_index_exists(&self, rid: RecordId, mut n: Array) -> Result<()> {
		bail!(Error::IndexExists {
			record: rid,
			index: self.ix.name.clone(),
			value: match n.0.len() {
				1 => n.0.remove(0).to_sql(),
				_ => n.to_sql(),
			},
		})
	}

	async fn index_fulltext(
		&mut self,
		stk: &mut Stk,
		p: &FullTextParams,
		require_compaction: &mut bool,
	) -> Result<()> {
		let mut rc = false;
		// Build a FullText instance
		let fti =
			FullTextIndex::new(self.ctx.get_index_stores(), &self.ctx.tx(), self.ikb.clone(), p)
				.await?;
		// Delete the old index data
		let doc_id = if let Some(o) = self.o.take() {
			fti.remove_content(stk, self.ctx, self.opt, self.rid, o, &mut rc).await?
		} else {
			None
		};
		// Create the new index data
		if let Some(n) = self.n.take() {
			fti.index_content(stk, self.ctx, self.opt, self.rid, n, &mut rc).await?;
		} else {
			// It is a deletion, we can remove the doc
			if let Some(doc_id) = doc_id {
				fti.remove_doc(self.ctx, doc_id).await?;
			}
		}
		// Do we need to trigger the compaction?
		if rc {
			*require_compaction = true;
		}
		Ok(())
	}

	pub(crate) async fn trigger_compaction(&self) -> Result<()> {
		FullTextIndex::trigger_compaction(&self.ikb, &self.ctx.tx(), self.opt.id()).await
	}

	async fn index_hnsw(&mut self, p: &HnswParams) -> Result<()> {
		let txn = self.ctx.tx();
		let hnsw = self
			.ctx
			.get_index_stores()
			.get_index_hnsw(self.ns, self.db, self.ctx, self.tb, self.ix, p)
			.await?;
		let mut hnsw = hnsw.write().await;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			hnsw.remove_document(&txn, self.rid.key.clone(), &o).await?;
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			hnsw.index_document(&txn, &self.rid.key, &n).await?;
		}
		Ok(())
	}
}

/// Extract from the given document, the values required by the index and put
/// then in an array. Eg. IF the index is composed of the columns `name` and
/// `instrument` Given this doc: { "id": 1, "instrument":"piano", "name":"Tobie"
/// } It will return: ["Tobie", "piano"]
struct Indexable(Vec<(Value, bool)>);

impl Indexable {
	fn new(vals: Vec<Value>, ix: &IndexDefinition) -> Self {
		let mut source = Vec::with_capacity(vals.len());
		for (v, i) in vals.into_iter().zip(ix.cols.iter()) {
			let f = matches!(i.0.last(), Some(&Part::Flatten));
			source.push((v, f));
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

struct Combinator {
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
		let mut o = Vec::with_capacity(self.iterators.len());
		// Create the combination and advance to the next
		self.has_next = false;
		for i in &mut self.iterators {
			o.push(i.current().clone());
			if !self.has_next {
				// We advance only one iterator per iteration
				if i.next() {
					self.has_next = true;
				}
			}
		}
		let o = Array::from(o);
		Some(o)
	}
}

trait ValuesIterator: Send {
	fn next(&mut self) -> bool;
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

	fn current(&self) -> &Value {
		self.vals.get(self.current).unwrap_or(&Value::Null)
	}
}

struct SingleValueIterator(Value);

impl ValuesIterator for SingleValueIterator {
	fn next(&mut self) -> bool {
		false
	}

	fn current(&self) -> &Value {
		&self.0
	}
}
