use anyhow::{Result, ensure};

use crate::catalog::{Relation, TableType};
use crate::ctx::Context;
use crate::dbs::{Options, Statement, Workable};
use crate::doc::Document;
use crate::err::Error;
use crate::expr::Dir;
use crate::expr::paths::{IN, OUT};
use crate::val::record::RecordType;

impl Document {
	pub(super) async fn store_edges_data(
		&mut self,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<()> {
		// Get the table
		let tb = self.tb(ctx, opt).await?;
		// Check if the table is a view
		if tb.drop {
			return Ok(());
		}
		// Store the record edges
		if let Workable::Relate(l, r, _) = &self.extras {
			// Get the namespace / database
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			// Get the record id
			let rid = self.id()?;
			// Get the transaction
			let txn = ctx.tx();
			// Lock the transaction
			let mut txn = txn.lock().await;
			// For enforced relations, ensure that the edges exist
			if matches!(
				tb.table_type,
				TableType::Relation(Relation {
					enforced: true,
					..
				})
			) {
				// Check that the `in` record exists
				let key = crate::key::thing::new(ns, db, &l.table, &l.key);
				ensure!(
					txn.exists(&key, None).await?,
					Error::IdNotFound {
						rid: l.to_string(),
					}
				);
				// Check that the `out` record exists
				let key = crate::key::thing::new(ns, db, &r.table, &r.key);
				ensure!(
					txn.exists(&key, None).await?,
					Error::IdNotFound {
						rid: r.to_string(),
					}
				);
			}
			// Get temporary edge references
			let (ref o, ref i) = (Dir::Out, Dir::In);
			// Store the left pointer edge
			let key = crate::key::graph::new(ns, db, &l.table, &l.key, o, &rid);
			txn.set(&key, &(), opt.version).await?;
			// Store the left inner edge
			let key = crate::key::graph::new(ns, db, &rid.table, &rid.key, i, l);
			txn.set(&key, &(), opt.version).await?;
			// Store the right inner edge
			let key = crate::key::graph::new(ns, db, &rid.table, &rid.key, o, r);
			txn.set(&key, &(), opt.version).await?;
			// Store the right pointer edge
			let key = crate::key::graph::new(ns, db, &r.table, &r.key, i, &rid);
			txn.set(&key, &(), opt.version).await?;
			// Store the edges on the record
			// Mark this record as an edge type in its metadata for efficient identification
			self.current.doc.set_record_type(RecordType::Edge);
			self.current.doc.to_mut().put(&*IN, l.clone().into());
			self.current.doc.to_mut().put(&*OUT, r.clone().into());
		}
		// Carry on
		Ok(())
	}
}
