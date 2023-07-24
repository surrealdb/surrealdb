use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::{Options, Transaction};
use crate::doc::{CursorDoc, Document};
use crate::err::Error;
use crate::idx::btree::store::BTreeStoreType;
use crate::idx::ft::FtIndex;
use crate::idx::IndexKeyBase;
use crate::sql::array::Array;
use crate::sql::index::Index;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::Part::All;
use crate::sql::{Ident, Thing, Value};
use crate::{key, kvs};

impl<'a> Document<'a> {
	pub async fn index(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check indexes
		if !opt.indexes {
			return Ok(());
		}
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Loop through all index statements
		for ix in self.ix(opt, txn).await?.iter() {
			// Calculate old values
			let o = Self::build_opt_array(ctx, opt, txn, ix, &self.initial).await?;

			// Calculate new values
			let n = Self::build_opt_array(ctx, opt, txn, ix, &self.current).await?;

			// Update the index entries
			if opt.force || o != n {
				// Claim transaction
				let mut run = txn.lock().await;

				// Store all the variable and parameters required by the index operation
				let ic = IndexOperation::new(opt, ix, o, n, rid);

				// Index operation dispatching
				match &ix.index {
					Index::Uniq => ic.index_unique(&mut run).await?,
					Index::Idx => ic.index_non_unique(&mut run).await?,
					Index::Search {
						az,
						sc,
						hl,
						order,
					} => ic.index_full_text(&mut run, az, *order, sc, *hl).await?,
				};
			}
		}
		// Carry on
		Ok(())
	}

	/// Extract from the given document, the values required by the index and put then in an array.
	/// Eg. IF the index is composed of the columns `name` and `instrument`
	/// Given this doc: { "id": 1, "instrument":"piano", "name":"Tobie" }
	/// It will return: ["Tobie", "piano"]
	async fn build_opt_array(
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		ix: &DefineIndexStatement,
		doc: &CursorDoc<'_>,
	) -> Result<Option<Vec<Array>>, Error> {
		if !doc.doc.is_some() {
			return Ok(None);
		}
		let mut f = Vec::with_capacity(ix.cols.len());
		let mut o = Vec::with_capacity(f.len());
		let mut no_f = true;
		for i in &ix.cols.0 {
			let v = i.compute(ctx, opt, txn, Some(doc)).await?;
			let b = matches!(i.0.last(), Some(&All));
			if b {
				no_f = false;
			}
			f.push(b);
			o.push(v);
		}
		// Nothing flattened? We can return the array of values
		if no_f {
			return Ok(Some(vec![Array(o)]));
		}
		let mut iterators: Vec<Box<dyn ValuesIterator>> = Vec::new();
		// Otherwise we generate all the possibilities.
		for (i, v) in o.iter().enumerate() {
			if f.get(i) == Some(&true) {
				if let Value::Array(v) = v {
					iterators.push(Box::new(MultiValuesIterator {
						vals: &v.0,
						done: false,
						current: 0,
					}));
					continue;
				}
			}
			iterators.push(Box::new(SingleValueIterator {
				val: v,
			}));
		}
		let mut r = Vec::new();
		let mut has_next = true;
		while has_next {
			let mut o = Vec::with_capacity(f.len());
			// Create the combination and advance to the next
			has_next = false;
			for i in &mut iterators {
				o.push(i.current().clone());
				if !has_next {
					// We advance only one iterator per iteration
					if i.next() {
						has_next = true;
					}
				}
			}
			r.push(Array(o));
		}
		Ok(Some(r))
	}
}

trait ValuesIterator<'a> {
	fn next(&mut self) -> bool;
	fn current(&self) -> &'a Value;
}

struct MultiValuesIterator<'a> {
	vals: &'a Vec<Value>,
	done: bool,
	current: usize,
}

impl<'a> ValuesIterator<'a> for MultiValuesIterator<'a> {
	fn next(&mut self) -> bool {
		if self.done {
			return false;
		}
		if self.current == self.vals.len() - 1 {
			self.done = true;
			return false;
		}
		self.current += 1;
		true
	}

	fn current(&self) -> &'a Value {
		self.vals.get(self.current).unwrap_or(&Value::Null)
	}
}

struct SingleValueIterator<'a> {
	val: &'a Value,
}

impl<'a> ValuesIterator<'a> for SingleValueIterator<'a> {
	fn next(&mut self) -> bool {
		false
	}

	fn current(&self) -> &'a Value {
		self.val
	}
}

struct IndexOperation<'a> {
	opt: &'a Options,
	ix: &'a DefineIndexStatement,
	/// The old values (if existing)
	o: Option<Vec<Array>>,
	/// The new values (if existing)
	n: Option<Vec<Array>>,
	rid: &'a Thing,
}

impl<'a> IndexOperation<'a> {
	fn new(
		opt: &'a Options,
		ix: &'a DefineIndexStatement,
		o: Option<Vec<Array>>,
		n: Option<Vec<Array>>,
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

	fn get_non_unique_index_key(&self, v: &Array) -> key::index::Index {
		key::index::Index::new(
			self.opt.ns(),
			self.opt.db(),
			&self.ix.what,
			&self.ix.name,
			v.to_owned(),
			Some(self.rid.id.to_owned()),
		)
	}

	async fn index_non_unique(&self, run: &mut kvs::Transaction) -> Result<(), Error> {
		// Delete the old index data
		if let Some(o) = &self.o {
			for o in o {
				let key = self.get_non_unique_index_key(o);
				let _ = run.delc(key, Some(self.rid)).await; // Ignore this error
			}
		}
		// Create the new index data
		if let Some(n) = &self.n {
			for n in n {
				let key = self.get_non_unique_index_key(n);
				if run.putc(key, self.rid, None).await.is_err() {
					return self.err_index_exists(n);
				}
			}
		}
		Ok(())
	}

	fn get_unique_index_key(&self, v: &Array) -> key::index::Index {
		key::index::Index::new(
			self.opt.ns(),
			self.opt.db(),
			&self.ix.what,
			&self.ix.name,
			v.to_owned(),
			None,
		)
	}

	async fn index_unique(&self, run: &mut kvs::Transaction) -> Result<(), Error> {
		// Delete the old index data
		if let Some(o) = &self.o {
			for o in o {
				let key = self.get_unique_index_key(o);
				let _ = run.delc(key, Some(self.rid)).await; // Ignore this error
			}
		}
		// Create the new index data
		if let Some(n) = &self.n {
			for n in n {
				if !n.is_all_none_or_null() {
					let key = self.get_unique_index_key(n);
					if run.putc(key, self.rid, None).await.is_err() {
						return self.err_index_exists(n);
					}
				}
			}
		}
		Ok(())
	}

	fn err_index_exists(&self, n: &Array) -> Result<(), Error> {
		Err(Error::IndexExists {
			thing: self.rid.to_string(),
			index: self.ix.name.to_string(),
			value: match n.len() {
				1 => n.first().unwrap().to_string(),
				_ => n.to_string(),
			},
		})
	}

	async fn index_full_text(
		&self,
		run: &mut kvs::Transaction,
		az: &Ident,
		order: u32,
		scoring: &Scoring,
		hl: bool,
	) -> Result<(), Error> {
		let ikb = IndexKeyBase::new(self.opt, self.ix);
		let az = run.get_az(self.opt.ns(), self.opt.db(), az.as_str()).await?;
		let mut ft = FtIndex::new(run, az, ikb, order, scoring, hl, BTreeStoreType::Write).await?;
		if let Some(n) = &self.n {
			ft.index_document(run, self.rid, n).await?;
		} else {
			ft.remove_document(run, self.rid).await?;
		}
		ft.finish(run).await
	}
}
