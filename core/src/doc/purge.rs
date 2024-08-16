use crate::ctx::Context;
use crate::dbs::Options;
use crate::dbs::Statement;
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
use reblessive::tree::Stk;

impl Document {
	pub async fn purge(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Get the transaction
		let txn = ctx.tx();
		// Lock the transaction
		let mut txn = txn.lock().await;
		// Get the record id
		if let Some(rid) = &self.id {
			// Purge the record data
			let key = crate::key::thing::new(
				opt.ns()?,
				opt.db()?,
				&rid.tb,
				&rid.id.to_owned().try_into()?,
			);
			txn.del(key).await?;
			// Purge the record edges
			match (
				self.initial.doc.as_ref().pick(&*EDGE),
				self.initial.doc.as_ref().pick(&*IN),
				self.initial.doc.as_ref().pick(&*OUT),
			) {
				(Value::Bool(true), Value::Thing(ref l), Value::Thing(ref r)) => {
					// Get temporary edge references
					let (ref o, ref i) = (Dir::Out, Dir::In);
					// Purge the left pointer edge
					let key = crate::key::graph::new(opt.ns()?, opt.db()?, &l.tb, &l.id, o, rid);
					txn.del(key).await?;
					// Purge the left inner edge
					let key = crate::key::graph::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id, i, l);
					txn.del(key).await?;
					// Purge the right inner edge
					let key = crate::key::graph::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id, o, r);
					txn.del(key).await?;
					// Purge the right pointer edge
					let key = crate::key::graph::new(opt.ns()?, opt.db()?, &r.tb, &r.id, i, rid);
					txn.del(key).await?;
				}
				_ => {
					// Release the transaction
					drop(txn);
					// Setup the delete statement
					let stm = DeleteStatement {
						what: Values(vec![Value::from(Edges {
							dir: Dir::Both,
							from: rid.as_ref().clone(),
							what: Tables::default(),
						})]),
						..DeleteStatement::default()
					};
					// Execute the delete statement
					stm.compute(stk, ctx, opt, None).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
