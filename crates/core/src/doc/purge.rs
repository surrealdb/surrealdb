use crate::ctx::Context;
use crate::ctx::MutableContext;
use crate::dbs::Options;
use crate::dbs::Statement;
use crate::doc::CursorDoc;
use crate::doc::CursorValue;
use crate::doc::Document;
use crate::err::Error;
use crate::key::r#ref::Ref;
use crate::sql::dir::Dir;
use crate::sql::edges::Edges;
use crate::sql::paths::EDGE;
use crate::sql::paths::IN;
use crate::sql::paths::OUT;
use crate::sql::reference::ReferenceDeleteStrategy;
use crate::sql::statements::DeleteStatement;
use crate::sql::table::Tables;
use crate::sql::value::{Value, Values};
use crate::sql::Thing;
use reblessive::tree::Stk;

impl Document {
	pub(super) async fn purge(
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
			// Get the namespace
			let ns = opt.ns()?;
			// Get the database
			let db = opt.db()?;
			// Purge the record data
			let key = crate::key::thing::new(ns, db, &rid.tb, &rid.id);
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
					let key = crate::key::graph::new(ns, db, &l.tb, &l.id, o, rid);
					txn.del(key).await?;
					// Purge the left inner edge
					let key = crate::key::graph::new(ns, db, &rid.tb, &rid.id, i, l);
					txn.del(key).await?;
					// Purge the right inner edge
					let key = crate::key::graph::new(ns, db, &rid.tb, &rid.id, o, r);
					txn.del(key).await?;
					// Purge the right pointer edge
					let key = crate::key::graph::new(ns, db, &r.tb, &r.id, i, rid);
					txn.del(key).await?;
					// Release the transaction
					drop(txn);
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
			// Process any record references
			{
				let prefix = crate::key::r#ref::prefix(ns, db, &rid.tb, &rid.id);
				let suffix = crate::key::r#ref::suffix(ns, db, &rid.tb, &rid.id);
				let range = prefix..suffix;

				let txn = ctx.tx();
				let mut keys = txn.keys(range.clone(), 1000, None).await?;
				while !keys.is_empty() {
					for key in keys.drain(..) {
						let r#ref = Ref::from(&key);
						let fd = txn.get_tb_field(ns, db, r#ref.ft, r#ref.ff).await?;
						if let Some(reference) = &fd.reference {
							match &reference.on_delete {
								ReferenceDeleteStrategy::Ignore => (),
								ReferenceDeleteStrategy::Reject => {
									let thing = Thing {
										tb: r#ref.ft.to_string(),
										id: r#ref.fk.clone(),
									};

									return Err(Error::DeleteRejectedByReference(
										rid.to_string(),
										thing.to_string(),
									));
								}
								ReferenceDeleteStrategy::Cascade => {
									let thing = Thing {
										tb: r#ref.ft.to_string(),
										id: r#ref.fk.clone(),
									};

									// Setup the delete statement
									let stm = DeleteStatement {
										what: Values(vec![Value::from(thing)]),
										..DeleteStatement::default()
									};
									// Execute the delete statement
									stm.compute(stk, ctx, opt, None).await?;
								}
								ReferenceDeleteStrategy::Custom(v) => {
									let reference = Value::from(rid.as_ref().clone());
									let this = Thing {
										tb: r#ref.ft.to_string(),
										id: r#ref.fk.clone(),
									};

									let mut ctx = MutableContext::new(ctx);
									ctx.add_value("reference", reference.into());
									let ctx = ctx.freeze();

									let doc: CursorValue = Value::None.into();
									let doc = CursorDoc::new(Some(this.into()), None, doc);

									v.compute(stk, &ctx, opt, Some(&doc)).await?;
								}
							}
						}

						txn.del(key).await?;
					}
					keys = txn.keys(range.clone(), 1000, None).await?;
				}
			}
		}
		// Carry on
		Ok(())
	}
}
