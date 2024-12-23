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
use crate::sql::statements::UpdateStatement;
use crate::sql::table::Tables;
use crate::sql::value::{Value, Values};
use crate::sql::Data;
use crate::sql::Operator;
use crate::sql::Part;
use crate::sql::Thing;
use futures::StreamExt;
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
		// Get the record id
		if let Some(rid) = &self.id {
			// Get the namespace
			let ns = opt.ns()?;
			// Get the database
			let db = opt.db()?;
			// Purge the record data
			txn.del_record(ns, db, &rid.tb, &rid.id).await?;
			// Purge the record edges
			match (
				self.initial.doc.as_ref().pick(&*EDGE),
				self.initial.doc.as_ref().pick(&*IN),
				self.initial.doc.as_ref().pick(&*OUT),
			) {
				(Value::Bool(true), Value::Thing(ref l), Value::Thing(ref r)) => {
					// Lock the transaction
					let mut txn = txn.lock().await;
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

				// Obtain a transaction
				let txn = ctx.tx();
				// Obtain a stream of keys
				let mut stream = txn.stream_keys(range.clone());
				// Loop until no more entries
				while let Some(res) = stream.next().await {
					// Decode the key
					let key = res?;
					let r#ref = Ref::from(&key);
					// Obtain the remote field definition
					let fd = txn.get_tb_field(ns, db, r#ref.ft, r#ref.ff).await?;
					// Check if there is a reference defined on the field
					if let Some(reference) = &fd.reference {
						match &reference.on_delete {
							// Ignore this reference
							ReferenceDeleteStrategy::Ignore => (),
							// Reject the delete operation, as indicated by the reference
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
							// Delete the remote record which referenced this record
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
								stm.compute(stk, ctx, &opt.clone().with_perms(false), None)
									.await
									// Wrap any error in an error explaining what went wrong
									.map_err(|e| {
										Error::RefsUpdateFailure(rid.to_string(), e.to_string())
									})?;
							}
							// Delete only the reference on the remote record
							ReferenceDeleteStrategy::Unset => {
								let thing = Thing {
									tb: r#ref.ft.to_string(),
									id: r#ref.fk.clone(),
								};

								// Determine how we perform the update
								let data = match fd.name.last() {
									// This is a part of an array, remove all values like it
									Some(Part::All) => Data::SetExpression(vec![(
										fd.name.as_ref()[..fd.name.len() - 1].into(),
										Operator::Dec,
										Value::Thing(rid.as_ref().clone()),
									)]),
									// This is a self contained value, we can set it NONE
									_ => Data::UnsetExpression(vec![fd.name.as_ref().into()]),
								};

								// Setup the delete statement
								let stm = UpdateStatement {
									what: Values(vec![Value::from(thing)]),
									data: Some(data),
									..UpdateStatement::default()
								};

								// Execute the delete statement
								stm.compute(stk, ctx, &opt.clone().with_perms(false), None)
									.await
									// Wrap any error in an error explaining what went wrong
									.map_err(|e| {
										Error::RefsUpdateFailure(rid.to_string(), e.to_string())
									})?;
							}
							// Process a custom delete strategy
							ReferenceDeleteStrategy::Custom(v) => {
								// Value for the `$reference` variable is the current record
								let reference = Value::from(rid.as_ref().clone());
								// Value for the document is the remote record
								let this = Thing {
									tb: r#ref.ft.to_string(),
									id: r#ref.fk.clone(),
								};

								// Set the `$reference` variable in the context
								let mut ctx = MutableContext::new(ctx);
								ctx.add_value("reference", reference.into());
								let ctx = ctx.freeze();

								// Obtain the document for the remote record
								let doc: CursorValue = Value::Thing(this)
									.get(
										stk,
										&ctx,
										&opt.clone().with_perms(false),
										None,
										&[Part::All],
									)
									.await?
									.into();
								// Construct the document for the compute method
								let doc = CursorDoc::new(None, None, doc);

								// Compute the custom instruction.
								v.compute(stk, &ctx, &opt.clone().with_perms(false), Some(&doc))
									.await
									// Wrap any error in an error explaining what went wrong
									.map_err(|e| {
										Error::RefsUpdateFailure(rid.to_string(), e.to_string())
									})?;
							}
						}
					}
				}

				// After all references have been processed, delete them
				txn.delr(range).await?;
			}
		}
		// Carry on
		Ok(())
	}
}
