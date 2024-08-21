use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use reblessive::tree::Stk;

impl Document {
	pub async fn merge(
		&mut self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.doc.to_mut().def(rid);
		// This is an INSERT statement
		if let Workable::Insert(v) = &self.extras {
			let v = v.compute(stk, ctx, opt, Some(&self.current)).await?;
			self.current.doc.to_mut().merge(v)?;
		}
		// This is an INSERT RELATION statement
		if let Workable::Relate(_, _, Some(v)) = &self.extras {
			let v = v.compute(stk, ctx, opt, Some(&self.current)).await?;
			self.current.doc.to_mut().merge(v)?;
		}
		// Set default field values
		self.current.doc.to_mut().def(rid);
		// Carry on
		Ok(())
	}
}
