use std::sync::Arc;

use anyhow::{Result, bail};
use futures::StreamExt;
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::{Options, Statement};
use crate::doc::{CursorDoc, Document};
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
		ctx: &FrozenContext,
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
			self.purge_edges(stk, ctx, opt, rid.as_ref()).await?;
			// Purge any record references
			self.purge_references(stk, ctx, opt, rid.as_ref()).await?;
		}
		// Carry on
		Ok(())
	}

	async fn purge_edges(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
	) -> Result<()> {
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace / database
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		if self.initial.doc.is_edge() {
			// Get the in record id
			let l = self.initial.doc.as_ref().pick(&*IN);
			let Value::RecordId(ref l) = l else {
				fail!("Expected a record id for the `in` field, found {}", l.to_sql());
			};
			// Get the out record id
			let r = self.initial.doc.as_ref().pick(&*OUT);
			let Value::RecordId(ref r) = r else {
				fail!("Expected a record id for the `out` field, found {}", r.to_sql());
			};
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

		// Cleanup edges via which the current record relates to other records
		let what = vec![
			Part::Start(Expr::Literal(Literal::RecordId(rid.clone().into_literal()))),
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
		// Carry on
		Ok(())
	}

	async fn purge_references(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
	) -> Result<()> {
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace / database
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

		let prefix = crate::key::r#ref::prefix(ns, db, &rid.table, &rid.key)?;
		let suffix = crate::key::r#ref::suffix(ns, db, &rid.table, &rid.key)?;
		let range = prefix..suffix;

		// Obtain a stream of keys
		let mut stream = txn.stream_keys(range.clone(), None, None, ScanDirection::Forward);
		// Loop until no more entries
		while let Some(res) = stream.next().await {
			// Get the batch of keys
			let batch = res?;
			// Process each key in the batch
			for key in batch {
				yield_now!();
				// Decode the key
				let ref_key = Ref::decode_key(&key)?;
				// Obtain the remote field definition
				let Some(fd) =
					txn.get_tb_field(ns, db, ref_key.ft.as_ref(), ref_key.ff.as_ref()).await?
				else {
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
							let record = RecordId {
								table: ref_key.ft.into_owned(),
								key: ref_key.fk.into_owned(),
							};

							bail!(Error::DeleteRejectedByReference(rid.to_sql(), record.to_sql(),));
						}
						// Delete the remote record which referenced this record
						ReferenceDeleteStrategy::Cascade => {
							let record_id = RecordId {
								table: ref_key.ft.into_owned(),
								key: ref_key.fk.into_owned(),
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
									Error::RefsUpdateFailure(rid.to_sql(), e.to_string())
								})?;
						}
						// Delete only the reference on the remote record
						ReferenceDeleteStrategy::Unset => {
							let opt = opt.clone().with_perms(false);
							let record = RecordId {
								table: ref_key.ft.into_owned(),
								key: ref_key.fk.into_owned(),
							};

							if let Some(doc) =
								record.clone().select_document(stk, ctx, &opt, None).await?
							{
								let doc = Value::Object(doc);
								let data = match doc.pick(&fd.name) {
									Value::RecordId(_) => {
										Some(Data::UnsetExpression(vec![fd.name.clone()]))
									}
									Value::Array(_) | Value::Set(_) => {
										Some(Data::SetExpression(vec![Assignment {
											place: fd.name.clone(),
											operator: AssignOperator::Subtract,
											value: Expr::Literal(Literal::RecordId(
												rid.clone().into_literal(),
											)),
										}]))
									}
									Value::None => None,
									v => {
										fail!(
											"Expected either a record id, array, set or none, found {}",
											v.to_sql()
										)
									}
								};

								if data.is_some() {
									// Setup the update statement
									let stm = UpdateStatement {
										what: vec![Expr::Literal(Literal::RecordId(
											record.into_literal(),
										))],
										data,
										..UpdateStatement::default()
									};

									// Execute the update statement
									stm.compute(stk, ctx, &opt, None)
										.await
										// Wrap any error in an error explaining what went wrong
										.map_err(|e| {
											Error::RefsUpdateFailure(rid.to_sql(), e.to_string())
										})?;
								}
							}
						}
						// Process a custom delete strategy
						ReferenceDeleteStrategy::Custom(v) => {
							// Value for the `$reference` variable is the current record
							let reference = Value::from(rid.clone());
							// Value for the document is the remote record
							let this = RecordId {
								table: ref_key.ft.into_owned(),
								key: ref_key.fk.into_owned(),
							};

							// Set the `$reference` variable in the context
							let mut ctx = Context::new(ctx);
							ctx.add_value("reference", reference.into());
							let ctx = ctx.freeze();

							// Disable permissions
							let opt = opt.clone().with_perms(false);

							// Construct the document for the compute method
							let doc = CursorDoc::new(
								Some(Arc::new(this.clone())),
								None,
								Value::RecordId(this),
							);

							// Compute the custom instruction.
							stk.run(|stk| v.compute(stk, &ctx, &opt, Some(&doc)))
								.await
								.catch_return()
								// Wrap any error in an error explaining what went wrong
								.map_err(|e| {
									Error::RefsUpdateFailure(rid.to_sql(), e.to_string())
								})?;
						}
					}
				}
			}
		}

		// After all references have been processed, delete them
		txn.delr(range).await?;

		// Carry on
		Ok(())
	}
}
