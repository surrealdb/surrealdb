use anyhow::{Result, bail};
use futures::StreamExt;
use reblessive::tree::Stk;

use crate::ctx::{Context, MutableContext};
use crate::dbs::capabilities::ExperimentalTarget;
use crate::dbs::{Options, Statement};
use crate::doc::{CursorDoc, CursorRecord, Document};
use crate::err::Error;
use crate::expr::data::Assignment;
use crate::expr::dir::Dir;
use crate::expr::lookup::LookupKind;
use crate::expr::paths::{IN, OUT};
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::statements::{DeleteStatement, UpdateStatement};
use crate::expr::{AssignOperator, Data, Expr, FlowResultExt as _, Idiom, Literal, Lookup, Part};
use crate::idx::planner::ScanDirection;
use crate::key::r#ref::Ref;
use crate::val::{RecordId, Value};

impl Document {
	pub(super) async fn purge(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_stm: &Statement<'_>,
	) -> Result<()> {
		// Check if changed
		if !self.changed() {
			return Ok(());
		}
		// Get the transaction
		let txn = ctx.tx();
		// Get the record id
		if let Some(rid) = &self.id {
			// Get the namespace / database
			let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
			// Purge the record data
			txn.del_record(ns, db, &rid.table, &rid.key).await?;
			// Purge the record edges
			match (
				self.initial.doc.is_edge(),
				self.initial.doc.as_ref().pick(&*IN),
				self.initial.doc.as_ref().pick(&*OUT),
			) {
				(true, Value::RecordId(ref l), Value::RecordId(ref r)) => {
					// Lock the transaction
					let mut txn = txn.lock().await;
					// Get temporary edge references
					let (ref o, ref i) = (Dir::Out, Dir::In);
					// Purge the left pointer edge
					let key = crate::key::graph::new(ns, db, &l.table, &l.key, o, rid);
					txn.del(&key).await?;
					// Purge the left inner edge
					let key = crate::key::graph::new(ns, db, &rid.table, &rid.key, i, l);
					txn.del(&key).await?;
					// Purge the right inner edge
					let key = crate::key::graph::new(ns, db, &rid.table, &rid.key, o, r);
					txn.del(&key).await?;
					// Purge the right pointer edge
					let key = crate::key::graph::new(ns, db, &r.table, &r.key, i, rid);
					txn.del(&key).await?;
					// Release the transaction
					drop(txn);
				}
				_ => {
					let what = vec![
						Part::Start(Expr::Literal(Literal::RecordId(
							(**rid).clone().into_literal(),
						))),
						Part::Lookup(Lookup {
							kind: LookupKind::Graph(Dir::Both),
							..Default::default()
						}),
					];

					// Setup the delete statement
					let stm = DeleteStatement {
						what: vec![Expr::Idiom(Idiom(what))],
						..DeleteStatement::default()
					};
					// Execute the delete statement
					stm.compute(stk, ctx, opt, None).await?;
				}
			}
			// Process any record references
			if ctx.get_capabilities().allows_experimental(&ExperimentalTarget::RecordReferences) {
				let prefix = crate::key::r#ref::prefix(ns, db, &rid.table, &rid.key)?;
				let suffix = crate::key::r#ref::suffix(ns, db, &rid.table, &rid.key)?;
				let range = prefix..suffix;

				// Obtain a transaction
				let txn = ctx.tx();
				// Obtain a stream of keys
				let mut stream = txn.stream_keys(range.clone(), None, ScanDirection::Forward);
				// Loop until no more entries
				while let Some(res) = stream.next().await {
					yield_now!();
					// Decode the key
					let key = res?;
					let ref_key = Ref::decode_key(&key)?;
					// Obtain the remote field definition
					let Some(fd) = txn.get_tb_field(ns, db, ref_key.ft, ref_key.ff).await? else {
						return Err(Error::FdNotFound {
							name: ref_key.ff.to_string(),
						}
						.into());
					};
					// Check if there is a reference defined on the field
					if let Some(reference) = &fd.reference {
						match &reference.on_delete {
							// Ignore this reference
							ReferenceDeleteStrategy::Ignore => (),
							// Reject the delete operation, as indicated by the reference
							ReferenceDeleteStrategy::Reject => {
								let thing = RecordId {
									table: ref_key.ft.to_string(),
									key: ref_key.fk.clone(),
								};

								bail!(Error::DeleteRejectedByReference(
									rid.to_string(),
									thing.to_string(),
								));
							}
							// Delete the remote record which referenced this record
							ReferenceDeleteStrategy::Cascade => {
								let record_id = RecordId {
									table: ref_key.ft.to_string(),
									key: ref_key.fk.clone(),
								};

								// Setup the delete statement
								let stm = DeleteStatement {
									what: vec![Expr::Literal(Literal::RecordId(
										record_id.into_literal(),
									))],
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
								let thing = RecordId {
									table: ref_key.ft.to_string(),
									key: ref_key.fk.clone(),
								};

								// Determine how we perform the update
								let data = match fd.name.last() {
									// This is a part of an array, remove all values like it
									Some(Part::All) => Data::SetExpression(vec![Assignment {
										place: Idiom(
											fd.name.as_ref()[..fd.name.len() - 1].to_vec(),
										),
										operator: AssignOperator::Subtract,
										value: Expr::Literal(Literal::RecordId(
											(**rid).clone().into_literal(),
										)),
									}]),
									// This is a self contained value, we can set it NONE
									_ => Data::UnsetExpression(vec![fd.name.clone()]),
								};

								// Setup the delete statement
								let stm = UpdateStatement {
									what: vec![Expr::Literal(Literal::RecordId(
										thing.into_literal(),
									))],
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
								let this = RecordId {
									table: ref_key.ft.to_string(),
									key: ref_key.fk.clone(),
								};

								// Set the `$reference` variable in the context
								let mut ctx = MutableContext::new(ctx);
								ctx.add_value("reference", reference.into());
								let ctx = ctx.freeze();

								// Obtain the document for the remote record
								let doc: CursorRecord = Value::RecordId(this)
									.get(
										stk,
										&ctx,
										&opt.clone().with_perms(false),
										None,
										&[Part::All],
									)
									.await
									.catch_return()?
									.into();
								// Construct the document for the compute method
								let doc = CursorDoc::new(None, None, doc);

								let opt = opt.clone().with_perms(false);
								// Compute the custom instruction.
								stk.run(|stk| v.compute(stk, &ctx, &opt, Some(&doc)))
									.await
									.catch_return()
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
