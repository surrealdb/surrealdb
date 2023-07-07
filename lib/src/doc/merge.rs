use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn merge(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.doc.to_mut().def(rid);
		// This is an INSERT statement
		if let Workable::Insert(v) = &self.extras {
			let v = v.compute(ctx, opt, txn, Some(&self.current)).await?;
			self.current.doc.to_mut().merge(v)?;
		}
		// Set default field values
		self.current.doc.to_mut().def(rid);
		// Carry on
		Ok(())
	}
}
