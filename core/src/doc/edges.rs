use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::value::Value;
use crate::sql::Dir;

impl Document {
	pub async fn edges(
		&mut self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if the table is a view
		if self.tb(ctx, opt).await?.drop {
			return Ok(());
		}
		// Get the transaction
		let txn = ctx.tx();
		// Lock the transaction
		let mut txn = txn.lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Store the record edges
		if let Workable::Relate(l, r, _) = &self.extras {
			// Get temporary edge references
			let (ref o, ref i) = (Dir::Out, Dir::In);
			// Store the left pointer edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &l.tb, &l.id, o, rid);
			txn.set(key, vec![], None).await?;
			// Store the left inner edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id, i, l);
			txn.set(key, vec![], None).await?;
			// Store the right inner edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id, o, r);
			txn.set(key, vec![], None).await?;
			// Store the right pointer edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &r.tb, &r.id, i, rid);
			txn.set(key, vec![], None).await?;
			// Store the edges on the record
			self.current.doc.to_mut().put(&*EDGE, Value::Bool(true));
			self.current.doc.to_mut().put(&*IN, l.clone().into());
			self.current.doc.to_mut().put(&*OUT, r.clone().into());
		}
		// Carry on
		Ok(())
	}
}
