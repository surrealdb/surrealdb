use crate::ctx::Context;
use crate::dbs::Statement;
use crate::dbs::Workable;
use crate::dbs::{Options, Transaction};
use crate::doc::Document;
use crate::err::Error;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::value::Value;
use crate::sql::Dir;

impl<'a> Document<'a> {
	pub async fn edges(
		&mut self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		_stm: &Statement<'_>,
	) -> Result<(), Error> {
		// Check if the table is a view
		if self.tb(opt, txn).await?.drop {
			return Ok(());
		}
		// Claim transaction
		let mut run = txn.lock().await;
		// Get the record id
		let rid = self.id.as_ref().unwrap();
		// Store the record edges
		if let Workable::Relate(l, r) = &self.extras {
			// Get temporary edge references
			let (ref o, ref i) = (Dir::Out, Dir::In);
			// Get ns and db ids
			let (ns, db) = run.get_ns_db_ids(opt.ns(), opt.db()).await?;
			// Store the left pointer edge
			let tb = run.get_tb_id_by_name(ns, db, &l.tb).await?;
			let key = crate::key::graph::new(ns, db, tb, &l.id, o, rid);
			run.set(key, vec![]).await?;
			// Store the left inner edge
			let tb = run.get_tb_id_by_name(ns, db, &rid.tb).await?;
			let key = crate::key::graph::new(ns, db, tb, &rid.id, i, l);
			run.set(key, vec![]).await?;
			// Store the right inner edge
			let key = crate::key::graph::new(ns, db, tb, &rid.id, o, r);
			run.set(key, vec![]).await?;
			// Store the right pointer edge
			let tb = run.get_tb_id_by_name(ns, db, &r.tb).await?;
			let key = crate::key::graph::new(ns, db, tb, &r.id, i, rid);
			run.set(key, vec![]).await?;
			// Store the edges on the record
			self.current.doc.to_mut().put(&*EDGE, Value::Bool(true));
			self.current.doc.to_mut().put(&*IN, l.clone().into());
			self.current.doc.to_mut().put(&*OUT, r.clone().into());
		}
		// Carry on
		Ok(())
	}
}
