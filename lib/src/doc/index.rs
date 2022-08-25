use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::array::Array;

impl<'a> Document<'a> {
	pub async fn index(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
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
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Loop through all index statements
		for ix in self.ix(opt, txn).await?.iter() {
			// Calculate old values
			let mut o = Array::with_capacity(ix.cols.len());
			for i in ix.cols.iter() {
				let v = i.compute(ctx, opt, txn, Some(&self.initial)).await?;
				o.push(v);
			}
			// Calculate new values
			let mut n = Array::with_capacity(ix.cols.len());
			for i in ix.cols.iter() {
				let v = i.compute(ctx, opt, txn, Some(&self.current)).await?;
				n.push(v);
			}
			// Clone transaction
			let run = txn.clone();
			// Claim transaction
			let mut run = run.lock().await;
			// Update the index entries
			if opt.force || o != n {
				match ix.uniq {
					true => {
						// Delete the old index data
						if self.initial.is_some() {
							#[rustfmt::skip]
							let key = crate::key::index::new(opt.ns(), opt.db(), &ix.what, &ix.name, o, None);
							let _ = run.delc(key, Some(rid)).await; // Ignore this error
						}
						// Create the new index data
						if self.current.is_some() {
							#[rustfmt::skip]
							let key = crate::key::index::new(opt.ns(), opt.db(), &ix.what, &ix.name, n, None);
							if run.putc(key, rid, None).await.is_err() {
								return Err(Error::IndexExists {
									index: ix.name.to_string(),
									thing: rid.to_string(),
								});
							}
						}
					}
					false => {
						// Delete the old index data
						if self.initial.is_some() {
							#[rustfmt::skip]
							let key = crate::key::index::new(opt.ns(), opt.db(), &ix.what, &ix.name, o, Some(&rid.id));
							let _ = run.delc(key, Some(rid)).await; // Ignore this error
						}
						// Create the new index data
						if self.current.is_some() {
							#[rustfmt::skip]
							let key = crate::key::index::new(opt.ns(), opt.db(), &ix.what, &ix.name, n, Some(&rid.id));
							if run.putc(key, rid, None).await.is_err() {
								return Err(Error::IndexExists {
									index: ix.name.to_string(),
									thing: rid.to_string(),
								});
							}
						}
					}
				};
			}
		}
		// Carry on
		Ok(())
	}
}
