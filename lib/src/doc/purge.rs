use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::dbs::Transaction;
use crate::doc::Document;
use crate::err::Error;
use crate::sql::dir::Dir;
use crate::sql::edges::Edges;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::statements::DeleteStatement;
use crate::sql::table::Tables;
use crate::sql::value::{Value, Values};

impl<'a> Document<'a> {
	pub async fn purge(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if forced
		if !opt.force && !self.changed() {
			return Ok(());
		}
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Clone transaction
		let run = txn.clone();
		// Claim transaction
		let mut run = run.lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Purge the record data
		let key = crate::key::thing::new(opt.ns(), opt.db(), &rid.tb, &rid.id);
		run.del(key).await?;
		// Purge the record edges
		match (self.initial.pick(&*EDGE), self.initial.pick(&*IN), self.initial.pick(&*OUT)) {
			(Value::Bool(true), Value::Thing(ref l), Value::Thing(ref r)) => {
				// Get temporary edge references
				let (ref o, ref i) = (Dir::Out, Dir::In);
				// Purge the left pointer edge
				let key = crate::key::graph::new(opt.ns(), opt.db(), &l.tb, &l.id, o, rid);
				run.del(key).await?;
				// Purge the left inner edge
				let key = crate::key::graph::new(opt.ns(), opt.db(), &rid.tb, &rid.id, i, l);
				run.del(key).await?;
				// Purge the right inner edge
				let key = crate::key::graph::new(opt.ns(), opt.db(), &rid.tb, &rid.id, o, r);
				run.del(key).await?;
				// Purge the right pointer edge
				let key = crate::key::graph::new(opt.ns(), opt.db(), &r.tb, &r.id, i, rid);
				run.del(key).await?;
			}
			_ => {
				// Release the transaction
				drop(run);
				// Setup the delete statement
				let stm = DeleteStatement {
					what: Values(vec![Value::from(Edges {
						dir: Dir::Both,
						from: rid.clone(),
						what: Tables::default(),
					})]),
					..DeleteStatement::default()
				};
				// Execute the delete statement
				stm.compute(ctx, opt, txn, None).await?;
			}
		}
		// Carry on
		Ok(())
	}
}
