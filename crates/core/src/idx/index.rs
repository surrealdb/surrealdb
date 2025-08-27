#![cfg(not(target_family = "wasm"))]

//! Index operation implementation for non-WASM targets.
//!
//! This module applies index mutations for a single document across different
//! index types (UNIQUE, regular, search, fulltext, MTree, Hnsw). Index keys are
//! constructed via key::index and field values are encoded using
//! key::value::StoreKeyArray.
//!
//! Numeric normalization in keys:
//! - StoreKeyArray normalizes Number values (Int/Float/Decimal) through a lexicographic numeric
//!   encoding so that byte order mirrors numeric order.
//! - Numerically equal values (e.g., 0, 0.0, 0dec) map to the same key bytes. On UNIQUE indexes,
//!   such inserts collide and produce a uniqueness error.
//!
//! Planner/executor simplification:
//! - Numeric predicates need a single probe/range in the index; per-variant fan-out is no longer
//!   required.

use std::sync::atomic::AtomicBool;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{DatabaseId, NamespaceId};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::index::{FullTextParams, HnswParams, MTreeParams, SearchParams};
use crate::expr::statements::DefineIndexStatement;
use crate::expr::{Index, Part};
use crate::idx::IndexKeyBase;
use crate::idx::ft::fulltext::FullTextIndex;
use crate::idx::ft::search::SearchIndex;
use crate::idx::trees::mtree::MTreeIndex;
use crate::key;
use crate::key::value::StoreKeyArray;
use crate::kvs::TransactionType;
use crate::val::{Array, RecordId, Value};

pub(crate) struct IndexOperation<'a> {
	ctx: &'a Context,
	opt: &'a Options,
	ns: NamespaceId,
	db: DatabaseId,
	ix: &'a DefineIndexStatement,
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
		ix: &'a DefineIndexStatement,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
		rid: &'a RecordId,
	) -> Self {
		Self {
			ctx,
			opt,
			ns,
			db,
			ix,
			o,
			n,
			rid,
		}
	}

	pub(crate) async fn compute(
		&mut self,
		stk: &mut Stk,
		require_compaction: &AtomicBool,
	) -> Result<()> {
		// Index operation dispatching
		match &self.ix.index {
			Index::Uniq => self.index_unique().await,
			Index::Idx => self.index_non_unique().await,
			Index::Search(p) => self.index_search(stk, p).await,
			Index::FullText(p) => self.index_fulltext(stk, p, require_compaction).await,
			Index::MTree(p) => self.index_mtree(stk, p).await,
			Index::Hnsw(p) => self.index_hnsw(p).await,
		}
	}

	/// Build the KV key for a unique index. The StoreKeyArray encodes values in
	/// a canonical, lexicographically ordered byte form which normalizes numeric
	/// types (Int/Float/Decimal). This means equal numeric values like 0, 0.0 and
	/// 0dec map to the same index key and therefore conflict on UNIQUE indexes.
	fn get_unique_index_key(&self, v: &'a StoreKeyArray) -> Result<key::index::Index> {
		Ok(key::index::Index::new(self.ns, self.db, &self.ix.what, &self.ix.name, v, None))
	}

	/// Build the KV key for a non-unique index. The record id is appended
	/// to the encoded field values so multiple records can share the same field
	/// bytes; numeric values inside fd are normalized via StoreKeyArray.
	fn get_non_unique_index_key(&self, v: &'a StoreKeyArray) -> Result<key::index::Index> {
		Ok(key::index::Index::new(
			self.ns,
			self.db,
			&self.ix.what,
			&self.ix.name,
			v,
			Some(&self.rid.key),
		))
	}

	async fn index_unique(&mut self) -> Result<()> {
		// Lock the transaction
		let tx = self.ctx.tx();
		let mut txn = tx.lock().await;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for o in i {
				let o = o.into();
				let key = self.get_unique_index_key(&o)?;
				match txn.delc(&key, Some(self.rid)).await {
					Err(e) => {
						if matches!(e.downcast_ref::<Error>(), Some(Error::TxConditionNotMet)) {
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
					let n = n.into();
					let key = self.get_unique_index_key(&n)?;
					if txn.putc(&key, self.rid, None).await.is_err() {
						let key = self.get_unique_index_key(&n)?;
						let rid: RecordId = txn.get(&key, None).await?.unwrap();
						return self.err_index_exists(rid, n.into());
					}
				}
			}
		}
		Ok(())
	}

	async fn index_non_unique(&mut self) -> Result<()> {
		// Lock the transaction
		let tx = self.ctx.tx();
		let mut txn = tx.lock().await;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for o in i {
				let o = o.into();
				let key = self.get_non_unique_index_key(&o)?;
				match txn.delc(&key, Some(self.rid)).await {
					Err(e) => {
						if matches!(e.downcast_ref::<Error>(), Some(Error::TxConditionNotMet)) {
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
				let n = n.into();
				let key = self.get_non_unique_index_key(&n)?;
				txn.set(&key, self.rid, None).await?;
			}
		}
		Ok(())
	}

	/// Construct a consistent uniqueness violation error message.
	/// Formats the conflicting value as a single value or array depending on
	/// the number of indexed fields.
	fn err_index_exists(&self, rid: RecordId, n: Array) -> Result<()> {
		Err(anyhow::Error::new(Error::IndexExists {
			thing: rid,
			index: self.ix.name.to_string(),
			value: match n.len() {
				1 => n.first().unwrap().to_string(),
				_ => n.to_string(),
			},
		}))
	}

	async fn index_search(&mut self, stk: &mut Stk, p: &SearchParams) -> Result<()> {
		let ikb = IndexKeyBase::new(self.ns, self.db, &self.ix.what, &self.ix.name);

		let mut ft =
			SearchIndex::new(self.ctx, self.ns, self.db, &p.az, ikb, p, TransactionType::Write)
				.await?;

		if let Some(n) = self.n.take() {
			ft.index_document(stk, self.ctx, self.opt, self.rid, n).await?;
		} else {
			ft.remove_document(self.ctx, self.rid).await?;
		}
		ft.finish(self.ctx).await
	}

	async fn index_fulltext(
		&mut self,
		stk: &mut Stk,
		p: &FullTextParams,
		require_compaction: &AtomicBool,
	) -> Result<()> {
		let ikb = IndexKeyBase::new(self.ns, self.db, &self.ix.what, &self.ix.name);
		let mut rc = false;
		// Build a FullText instance
		let s =
			FullTextIndex::new(self.opt.id()?, self.ctx.get_index_stores(), &self.ctx.tx(), ikb, p)
				.await?;
		// Delete the old index data
		let doc_id = if let Some(o) = self.o.take() {
			s.remove_content(stk, self.ctx, self.opt, self.rid, o, &mut rc).await?
		} else {
			None
		};
		// Create the new index data
		if let Some(n) = self.n.take() {
			s.index_content(stk, self.ctx, self.opt, self.rid, n, &mut rc).await?;
		} else {
			// It is a deletion, we can remove the doc
			if let Some(doc_id) = doc_id {
				s.remove_doc(self.ctx, doc_id).await?;
			}
		}
		if rc {
			require_compaction.store(true, std::sync::atomic::Ordering::Relaxed);
		}
		Ok(())
	}

	async fn index_mtree(&mut self, stk: &mut Stk, p: &MTreeParams) -> Result<()> {
		let txn = self.ctx.tx();
		let ikb = IndexKeyBase::new(self.ns, self.db, &self.ix.what, &self.ix.name);
		let mut mt = MTreeIndex::new(&txn, ikb, p, TransactionType::Write).await?;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			mt.remove_document(stk, &txn, self.rid, &o).await?;
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			mt.index_document(stk, &txn, self.rid, &n).await?;
		}
		mt.finish(&txn).await
	}

	async fn index_hnsw(&mut self, p: &HnswParams) -> Result<()> {
		let txn = self.ctx.tx();
		let hnsw = self
			.ctx
			.get_index_stores()
			.get_index_hnsw(self.ns, self.db, self.ctx, self.ix, p)
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
	fn new(vals: Vec<Value>, ix: &DefineIndexStatement) -> Self {
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
