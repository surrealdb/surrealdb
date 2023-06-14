use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;

impl<'a> Document<'a> {
	pub async fn merge(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.to_mut().def(rid);
		// This is an INSERT statement
		if let Workable::Insert(v) = &self.extras {
			let mut ctx = Context::new(ctx);
			ctx.add_cursor_doc(&self.current);
			let v = v.compute(&ctx, opt).await?;
			self.current.to_mut().merge(v)?;
		}
		// Set default field values
		self.current.to_mut().def(rid);
		// Carry on
		Ok(())
	}
}
