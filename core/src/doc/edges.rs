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
use crate::sql::Relation;
use crate::sql::TableType;

impl Document {
	pub async fn store_edges_data(
		&mut self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Get the table
		let tb = self.tb(ctx, opt).await?;
		// Check if the table is a view
		if tb.drop {
			return Ok(());
		}
		// Get the transaction
		let txn = ctx.tx();
		// Lock the transaction
		let mut txn = txn.lock().await;
		// Get the record id
		let rid = self.id()?;
		// Store the record edges
		if let Workable::Relate(l, r, _) = &self.extras {
			// For enforced relations, ensure that the edges exist
			if matches!(
				tb.kind,
				TableType::Relation(Relation {
					enforced: true,
					..
				})
			) {
				// Check that the `in` record exists
				let key = crate::key::thing::new(opt.ns()?, opt.db()?, &l.tb, &l.id);
				if !txn.exists(key).await? {
					return Err(Error::IdNotFound {
						value: l.to_string(),
					});
				}
				// Check that the `out` record exists
				let key = crate::key::thing::new(opt.ns()?, opt.db()?, &r.tb, &r.id);
				if !txn.exists(key).await? {
					return Err(Error::IdNotFound {
						value: r.to_string(),
					});
				}
			}
			// Get temporary edge references
			let (ref o, ref i) = (Dir::Out, Dir::In);
			// Store the left pointer edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &l.tb, &l.id, o, &rid);
			txn.set(key, vec![], None).await?;
			// Store the left inner edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id, i, l);
			txn.set(key, vec![], None).await?;
			// Store the right inner edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &rid.tb, &rid.id, o, r);
			txn.set(key, vec![], None).await?;
			// Store the right pointer edge
			let key = crate::key::graph::new(opt.ns()?, opt.db()?, &r.tb, &r.id, i, &rid);
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
