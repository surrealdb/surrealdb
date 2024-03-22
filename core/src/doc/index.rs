use crate::ctx::Context;
use crate::dbs::{Force, Statement};
use crate::dbs::{Options, Transaction};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::ft::FtIndex;
use crate::idx::trees::mtree::MTreeIndex;
use crate::idx::IndexKeyBase;
use crate::key;
use crate::kvs::TransactionType;
use crate::sql::array::Array;
use crate::sql::index::{Index, MTreeParams, SearchParams};
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Part, Thing, Value};

impl<'a> Document<'a> {
	pub async fn index(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check import
		if opt.import {
			return Ok(());
		}
		// Was this force targeted at a specific index?
		let targeted_force = matches!(opt.force, Force::Index(_));
		// Collect indexes or skip
		let ixs = match &opt.force {
			Force::Index(ix)
				if ix.first().is_some_and(|ix| self.id.is_some_and(|id| ix.what.0 == id.tb)) =>
			{
				ix.clone()
			}
			Force::All => self.ix(opt, txn).await?,
			_ if self.changed() => self.ix(opt, txn).await?,
			_ => return Ok(()),
		};
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Loop through all index statements
		for ix in ixs.iter() {
			// Calculate old values
			let o = build_opt_values(ctx, opt, txn, ix, &self.initial).await?;

			// Calculate new values
			let n = build_opt_values(ctx, opt, txn, ix, &self.current).await?;

			// Update the index entries
			if targeted_force || o != n {
				// Store all the variable and parameters required by the index operation
				let mut ic = IndexOperation::new(opt, ix, o, n, rid);

				// Index operation dispatching
				match &ix.index {
					Index::Uniq => ic.index_unique(txn).await?,
					Index::Idx => ic.index_non_unique(txn).await?,
					Index::Search(p) => ic.index_full_text(ctx, txn, p).await?,
					Index::MTree(p) => ic.index_mtree(ctx, txn, p).await?,
				};
			}
		}
		// Carry on
		Ok(())
	}
}

/// Extract from the given document, the values required by the index and put then in an array.
/// Eg. IF the index is composed of the columns `name` and `instrument`
/// Given this doc: { "id": 1, "instrument":"piano", "name":"Tobie" }
/// It will return: ["Tobie", "piano"]
async fn build_opt_values(
	ctx: &Context<'_>,
	opt: &Options,
	txn: &Transaction,
	ix: &DefineIndexStatement,
	doc: &CursorDoc<'_>,
) -> Result<Option<Vec<Value>>, Error> {
	if !doc.doc.is_some() {
		return Ok(None);
	}
	let mut o = Vec::with_capacity(ix.cols.len());
	for i in ix.cols.iter() {
		let v = i.compute(ctx, opt, txn, Some(doc)).await?;
		o.push(v);
	}
	Ok(Some(o))
}

/// Extract from the given document, the values required by the index and put then in an array.
/// Eg. IF the index is composed of the columns `name` and `instrument`
/// Given this doc: { "id": 1, "instrument":"piano", "name":"Tobie" }
/// It will return: ["Tobie", "piano"]
struct Indexable(Vec<(Value, bool)>);

impl Indexable {
	fn new(vals: Vec<Value>, ix: &DefineIndexStatement) -> Self {
		let mut source = Vec::with_capacity(vals.len());
		for (v, i) in vals.into_iter().zip(ix.cols.0.iter()) {
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

struct IndexOperation<'a> {
	opt: &'a Options,
	ix: &'a DefineIndexStatement,
	/// The old values (if existing)
	o: Option<Vec<Value>>,
	/// The new values (if existing)
	n: Option<Vec<Value>>,
	rid: &'a Thing,
}

impl<'a> IndexOperation<'a> {
	fn new(
		opt: &'a Options,
		ix: &'a DefineIndexStatement,
		o: Option<Vec<Value>>,
		n: Option<Vec<Value>>,
		rid: &'a Thing,
	) -> Self {
		Self {
			opt,
			ix,
			o,
			n,
			rid,
		}
	}

	fn get_unique_index_key(&self, v: &'a Array) -> key::index::Index {
		crate::key::index::Index::new(
			self.opt.ns(),
			self.opt.db(),
			&self.ix.what,
			&self.ix.name,
			v,
			None,
		)
	}

	fn get_non_unique_index_key(&self, v: &'a Array) -> key::index::Index {
		crate::key::index::Index::new(
			self.opt.ns(),
			self.opt.db(),
			&self.ix.what,
			&self.ix.name,
			v,
			Some(&self.rid.id),
		)
	}

	async fn index_unique(&mut self, txn: &Transaction) -> Result<(), Error> {
		let mut run = txn.lock().await;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for o in i {
				let key = self.get_unique_index_key(&o);
				match run.delc(key, Some(self.rid)).await {
					Err(Error::TxConditionNotMet) => Ok(()),
					Err(e) => Err(e),
					Ok(v) => Ok(v),
				}?
			}
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			let i = Indexable::new(n, self.ix);
			for n in i {
				if !n.is_all_none_or_null() {
					let key = self.get_unique_index_key(&n);
					if run.putc(key, self.rid, None).await.is_err() {
						let key = self.get_unique_index_key(&n);
						let val = run.get(key).await?.unwrap();
						let rid: Thing = val.into();
						return self.err_index_exists(rid, n);
					}
				}
			}
		}
		Ok(())
	}

	async fn index_non_unique(&mut self, txn: &Transaction) -> Result<(), Error> {
		let mut run = txn.lock().await;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			let i = Indexable::new(o, self.ix);
			for o in i {
				let key = self.get_non_unique_index_key(&o);
				match run.delc(key, Some(self.rid)).await {
					Err(Error::TxConditionNotMet) => Ok(()),
					Err(e) => Err(e),
					Ok(v) => Ok(v),
				}?
			}
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			let i = Indexable::new(n, self.ix);
			for n in i {
				let key = self.get_non_unique_index_key(&n);
				if run.putc(key, self.rid, None).await.is_err() {
					let key = self.get_non_unique_index_key(&n);
					let val = run.get(key).await?.unwrap();
					let rid: Thing = val.into();
					return self.err_index_exists(rid, n);
				}
			}
		}
		Ok(())
	}

	fn err_index_exists(&self, rid: Thing, n: Array) -> Result<(), Error> {
		Err(Error::IndexExists {
			thing: rid,
			index: self.ix.name.to_string(),
			value: match n.len() {
				1 => n.first().unwrap().to_string(),
				_ => n.to_string(),
			},
		})
	}

	async fn index_full_text(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		p: &SearchParams,
	) -> Result<(), Error> {
		let ikb = IndexKeyBase::new(self.opt, self.ix);

		let mut ft = FtIndex::new(
			ctx.get_index_stores(),
			self.opt,
			txn,
			&p.az,
			ikb,
			p,
			TransactionType::Write,
		)
		.await?;

		if let Some(n) = self.n.take() {
			ft.index_document(ctx, self.opt, txn, self.rid, n).await?;
		} else {
			ft.remove_document(txn, self.rid).await?;
		}
		ft.finish(txn).await
	}

	async fn index_mtree(
		&mut self,
		ctx: &Context<'_>,
		txn: &Transaction,
		p: &MTreeParams,
	) -> Result<(), Error> {
		let mut tx = txn.lock().await;
		let ikb = IndexKeyBase::new(self.opt, self.ix);
		let mut mt =
			MTreeIndex::new(ctx.get_index_stores(), &mut tx, ikb, p, TransactionType::Write)
				.await?;
		// Delete the old index data
		if let Some(o) = self.o.take() {
			mt.remove_document(&mut tx, self.rid, o).await?;
		}
		// Create the new index data
		if let Some(n) = self.n.take() {
			mt.index_document(&mut tx, self.rid, n).await?;
		}
		mt.finish(&mut tx).await
	}
}
