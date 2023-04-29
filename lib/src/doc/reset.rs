use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::value::Value;

impl<'a> Document<'a> {
	pub async fn reset(
		&mut self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Set default field values
		self.current.to_mut().def(ctx, opt, txn, rid).await?;
		// Ensure edge fields are reset
		if self.initial.pick(&*EDGE).is_true() {
			self.current.to_mut().put(&*EDGE, Value::Bool(true));
			self.current.to_mut().put(&*IN, self.initial.pick(&*IN));
			self.current.to_mut().put(&*OUT, self.initial.pick(&*OUT));
		}
		// Carry on
		Ok(())
	}
}
