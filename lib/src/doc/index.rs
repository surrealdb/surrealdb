use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::idx::ft::FtIndex;
use crate::idx::IndexKeyBase;
use crate::sql::array::Array;
use crate::sql::index::Index;
use crate::sql::scoring::Scoring;
use crate::sql::statements::DefineIndexStatement;
use crate::sql::{Ident, Thing, Value};
use crate::{key, kvs};

impl<'a> Document<'a> {
	pub async fn index(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check events
		if !opt.indexes {
			return Ok(());
		}
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Clone transaction
		let txn = ctx.clone_transaction()?;
		// Check if the table is a view
		if self.tb(opt, &txn).await?.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Loop through all index statements
		for ix in self.ix(opt, &txn).await?.iter() {
			// Calculate old values
			let o = Self::build_opt_array(ctx, opt, ix, &self.initial).await?;

			// Calculate new values
			let n = Self::build_opt_array(ctx, opt, ix, &self.current).await?;

			// Update the index entries
			if opt.force || o != n {
				// Claim transaction
				let mut run = txn.lock().await;

				// Store all the variable and parameters required by the index operation
				let mut ic = IndexOperation::new(opt, ix, o, n, rid);

				// Index operation dispatching
				match &ix.index {
					Index::Uniq => ic.index_unique(&mut run).await?,
					Index::Idx => ic.index_non_unique(&mut run).await?,
					Index::Search {
						az,
						sc,
						hl,
						order,
					} => match sc {
						Scoring::Bm {
							..
						} => ic.index_best_matching_search(&mut run, az, *order, *hl).await?,
						Scoring::Vs => ic.index_vector_search(az, *hl).await?,
					},
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
		ix: &DefineIndexStatement,
		value: &Value,
	) -> Result<Option<Array>, Error> {
		if !value.is_some() {
			return Ok(None);
		}
		let mut ctx = Context::new(ctx);
		ctx.add_cursor_doc(value);
		let mut o = Array::with_capacity(ix.cols.len());
		for i in ix.cols.iter() {
			let v = i.compute(&ctx, opt).await?;
			o.push(v);
		}
		Ok(Some(o))
	}
}

struct IndexOperation<'a> {
	opt: &'a Options,
	ix: &'a DefineIndexStatement,
	/// The old value (if existing)
	o: Option<Array>,
	/// The new value (if existing)
	n: Option<Array>,
	rid: &'a Thing,
}

impl<'a> IndexOperation<'a> {
	fn new(
		opt: &'a Options,
		ix: &'a DefineIndexStatement,
		o: Option<Array>,
		n: Option<Array>,
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
		key::index::new(
			self.opt.ns(),
			self.opt.db(),
			&self.ix.what,
			&self.ix.name,
			v,
			Some(&self.rid.id),
		)
	}

	async fn index_non_unique(&self, run: &mut kvs::Transaction) -> Result<(), Error> {
		// Delete the old index data
		if let Some(o) = &self.o {
			let key = self.get_non_unique_index_key(o);
			let _ = run.delc(key, Some(self.rid)).await; // Ignore this error
		}
		// Create the new index data
		if let Some(n) = &self.n {
			let key = self.get_non_unique_index_key(n);
			if run.putc(key, self.rid, None).await.is_err() {
				return self.err_index_exists(n);
			}
		}
		Ok(())
	}

	fn get_unique_index_key(&self, v: &Array) -> key::index::Index {
		key::index::new(self.opt.ns(), self.opt.db(), &self.ix.what, &self.ix.name, v, None)
	}

	async fn index_unique(&self, run: &mut kvs::Transaction) -> Result<(), Error> {
		// Delete the old index data
		if let Some(o) = &self.o {
			let key = self.get_unique_index_key(o);
			let _ = run.delc(key, Some(self.rid)).await; // Ignore this error
		}
		// Create the new index data
		if let Some(n) = &self.n {
			let key = self.get_unique_index_key(n);
			if run.putc(key, self.rid, None).await.is_err() {
				return self.err_index_exists(n);
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

	async fn index_best_matching_search(
		&self,
		run: &mut kvs::Transaction,
		az: &Ident,
		order: u32,
		_hl: bool,
	) -> Result<(), Error> {
		let ikb = IndexKeyBase::new(self.opt, self.ix);
		let az = run.get_az(self.opt.ns(), self.opt.db(), az.as_str()).await?;
		let mut ft = FtIndex::new(run, az, ikb, order).await?;
		if let Some(n) = &self.n {
			// TODO: Apply the analyzer
			ft.index_document(run, self.rid, n).await
		} else {
			ft.remove_document(run, self.rid).await
		}
	}

	async fn index_vector_search(&mut self, _az: &Ident, _hl: bool) -> Result<(), Error> {
		Err(Error::FeatureNotYetImplemented {
			feature: "VectorSearch indexing",
		})
	}
}
