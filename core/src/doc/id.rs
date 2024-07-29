use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::Value;
use reblessive::tree::Stk;

impl<'a> Document<'a> {
	pub async fn id(
		&mut self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		stm: &Statement<'_>,
	) -> Result<(), Error> {
		if matches!(stm, Statement::Upsert { .. }) && !self.is_new() {
			return Ok(());
		}

		// Get the record id
		let mut rid = *self.id.to_owned().unwrap().clone();
		// Get the ID field definition, if any
		if let Ok(fd) = ctx.tx().get_tb_field(opt.ns()?, opt.db()?, &rid.tb, "id").await {
			// Variables
			let inp: Value = rid.id.clone().into();
			let mut val = inp.clone();
			let old = Value::None;
			// Check for a TYPE clause
			val = fd.compute_type(&rid, val)?;
			// Check for a VALUE clause
			val = (&fd).compute_value(stk, ctx, opt, Some(&self.current), &inp, val, &old).await?;
			// Check for a TYPE clause
			val = fd.compute_type(&rid, val)?;
			// Check for a ASSERT clause
			fd.compute_assert(stk, ctx, opt, Some(&self.current), &rid, &inp, &val, &old).await?;
			// Update rid
			rid.id = val.try_into()?;
		}
		// Set default field values
		self.current.doc.to_mut().def(&rid);
		self.id = Some(Box::new(rid.clone()));

		// Carry on
		Ok(())
	}
}
