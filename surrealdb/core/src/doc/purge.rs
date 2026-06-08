use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;

use crate::catalog::FieldDefinition;
use crate::catalog::providers::TableProvider;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
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
use crate::key::graph;
use crate::key::r#ref::Ref;
use crate::kvs::{NORMAL_BATCH_SIZE, ScanLimit};
use crate::val::{RecordId, TableName, Value};

impl Document {
	/// Purges a record from the datastore along with all its associated metadata.
	///
	/// This is the main purge function that orchestrates the complete deletion of a record,
	/// including the record data itself, any graph edges, and field references. It is called
	/// during DELETE operations after the document has been marked for deletion.
	///
	/// The purge process involves:
	/// 1. Deleting the record data from the key-value store
	/// 2. If the record is an edge record, removing the 4 graph edge pointers
	/// 3. Deleting any records connected to this record via graph edges (like `DELETE record<->`)
	/// 4. Processing field references according to their configured deletion strategies
	///
	/// This function only executes if the document has been modified.
	pub(super) async fn purge_record_data(
		&mut self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Check if changed
		if !self.is_modified() {
			return Ok(());
		}
		// Get the transaction
		let txn = ctx.tx();
		// Get the record id
		if let Some(rid) = self.id.clone() {
			// Get the namespace id
			let ns = self.doc_ctx.ns().namespace_id;
			// Get the database id
			let db = self.doc_ctx.db().database_id;
			// Purge the record data
			txn.del_record(ns, db, &rid.table, &rid.key).await?;
			// Mark this row as having mutated the KV store so the
			// iterator bumps the per-statement affected-row counter.
			self.mutated = true;
			// Check if this is an edge record
			if self.initial.doc.is_edge() {
				self.purge_pointers(ctx, rid.as_ref()).await?;
			}
			// Purge any edges connected to this record
			self.purge_edges(stk, ctx, opt, rid.as_ref()).await?;
			// Purge any references connected to this record
			self.purge_references(stk, ctx, opt, rid.as_ref()).await?;
		}
		// Carry on
		Ok(())
	}

	/// Deletes the graph edge pointers when the record being deleted is itself an edge record.
	///
	/// When a record is an edge (graph relation) between two records, it has 4 associated
	/// graph pointers that must be cleaned up:
	/// 1. Left pointer edge: from the `in` record pointing outward
	/// 2. Left inner edge: from this edge record pointing to the `in` record
	/// 3. Right inner edge: from this edge record pointing to the `out` record
	/// 4. Right pointer edge: from the `out` record pointing inward
	///
	/// These pointers are stored separately in the key-value store and must be explicitly
	/// deleted to maintain consistency.
	///
	/// This function is only called if the current record is an edge.
	async fn purge_pointers(&self, ctx: &FrozenContext, rid: &RecordId) -> Result<()> {
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace id
		let ns = self.doc_ctx.ns().namespace_id;
		// Get the database id
		let db = self.doc_ctx.db().database_id;
		// Get the in record id
		let l = self.initial.doc.as_ref().pick(&IN);
		let Value::RecordId(ref l) = l else {
			fail!("Expected a record id for the `in` field, found {}", l.to_sql());
		};
		// Get the out record id
		let r = self.initial.doc.as_ref().pick(&OUT);
		let Value::RecordId(ref r) = r else {
			fail!("Expected a record id for the `out` field, found {}", r.to_sql());
		};
		// The four keys deleted below mirror the encoding written by
		// `store_edges_data` for a single relation — linking the `in` vertex
		// (`l`), the edge record (`rid`), and the `out` vertex (`r`):
		//
		//              ltr (target = r)         pointer
		//          ┌─────────────────────┬─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
		//          │                     ▼                  ▼
		//     ┌────┴─────┐   etl  ┌────────────┐  etr   ┌──────────┐
		//     │   left   │───────▶│ rid (edge) │───────▶│  right   │
		//     └──────────┘   in   └────────────┘  out   └────┬─────┘
		//           ▼                    ▼                   │
		//           └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ┴───────────────────┘
		//                  pointer         rtl (source = l)
		//
		// `ltr` / `rtl` are vertex-side ("pointer") keys: stored on the
		// IN / OUT vertex with the opposite endpoint embedded so that
		// `->edge->vertex` (or its mirror) range scans can resolve the
		// far vertex without reading the edge record.
		//
		// `etl` / `etr` are edge-side ("inner") keys: their adjacency
		// already names the vertex in (ft, fk), so they keep the legacy
		// layout without an embedded target — same across both variants.
		let etl = graph::new(ns, db, &rid.table, &rid.key, Dir::In, l);
		let etr = graph::new(ns, db, &rid.table, &rid.key, Dir::Out, r);
		// Vertex-side keys are written in exactly one of two layouts and
		// the record's `RecordType::Edge { variant }` stamp tells us
		// which. Variant 1 records (legacy or pre-target-vertex layout)
		// only ever wrote `crate::key::graph::new` keys; variant 2
		// records (target-vertex-bearing layout) only ever wrote
		// `crate::key::graph::new_pointer` keys, because the RELATE
		// writer pre-deletes the legacy form before writing the new
		// one. So we delete only the layout that actually exists on
		// disk, halving the txn ops compared to probing both formats.
		let variant = self.initial.doc.edge_variant().unwrap_or_default();
		// Detect which variant the edge is currently
		match variant {
			1 => {
				let ltr = graph::new(ns, db, &l.table, &l.key, Dir::Out, rid);
				let rtl = graph::new(ns, db, &r.table, &r.key, Dir::In, rid);
				futures::try_join!(txn.del(&ltr), txn.del(&etl), txn.del(&etr), txn.del(&rtl))?;
			}
			_ => {
				let ltr = graph::new_pointer(ns, db, &l.table, &l.key, Dir::Out, rid, r);
				let rtl = graph::new_pointer(ns, db, &r.table, &r.key, Dir::In, rid, l);
				futures::try_join!(txn.del(&ltr), txn.del(&etl), txn.del(&etr), txn.del(&rtl))?;
			}
		}
		// Carry on
		Ok(())
	}

	/// Deletes all records that are connected to this record via graph edges.
	///
	/// This function scans for any graph edges pointing to or from the record being deleted,
	/// and if any exist, executes a `DELETE FROM record:id<->` statement to remove all
	/// connected records.
	///
	/// To optimize performance, this function first checks if any edges exist by fetching
	/// only the first key in the graph edge range. If no edges are found, the DELETE
	/// statement is skipped entirely, avoiding unnecessary overhead.
	///
	/// The cascade runs with the caller's permissions so that an edge table's
	/// `PERMISSIONS FOR delete` clause cannot be bypassed by deleting one of its
	/// endpoint vertices. Edges the caller is not allowed to delete are left in
	/// place, matching the outcome of a direct `DELETE edge:id` denied by the
	/// same clause.
	async fn purge_edges(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
	) -> Result<()> {
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace id
		let ns = self.doc_ctx.ns().namespace_id;
		// Get the database id
		let db = self.doc_ctx.db().database_id;
		// Get the key range of the graph keys
		let prefix = crate::key::graph::prefix(ns, db, &rid.table, &rid.key)?;
		let suffix = crate::key::graph::suffix(ns, db, &rid.table, &rid.key)?;
		// Open a cursor over the graph edge range so we can peek the first key.
		let mut cursor =
			txn.open_keys_cursor(prefix..suffix, ScanDirection::Forward, 0, None).await?;
		// Check if there are any edges to purge by fetching at most one key.
		let batch = cursor.next_batch(ScanLimit::Count(1)).await?;
		// Only proceed if there are edges for this record.
		if !batch.is_empty() {
			// Create a `DELETE FROM record:id<->` statement. `Part::Lookup`
			// boxes its payload after the recent enum-variant slimming on
			// main (#209), so wrap the `Lookup` accordingly.
			let stm = DeleteStatement {
				what: vec![Expr::Idiom(Idiom(vec![
					Part::Start(Expr::Literal(Literal::RecordId(rid.clone().into_literal()))),
					Part::Lookup(Box::new(Lookup {
						kind: LookupKind::Graph(Dir::Both),
						..Default::default()
					})),
				]))],
				..Default::default()
			};
			// Execute the delete statement. We deliberately do NOT disable
			// permissions here: an edge table's `PERMISSIONS FOR delete`
			// clause must still apply even when the cascade is triggered by
			// deleting one of the endpoint vertices, otherwise an actor with
			// vertex-delete permission could erase edges they are not
			// allowed to remove directly.
			stm.compute(stk, ctx, opt, None).await?;
		}
		// Carry on
		Ok(())
	}

	/// Processes field references according to their configured deletion strategies.
	///
	/// When a record is deleted, other records may reference it through DEFINE FIELD with
	/// a REFERENCE clause. This function handles each incoming reference according to its
	/// ON DELETE strategy:
	///
	/// - **IGNORE**: No action taken on the referencing record
	/// - **REJECT**: Aborts the delete operation with an error
	/// - **CASCADE**: Deletes the referencing record (recursive deletion)
	/// - **UNSET**: Removes the reference field from the referencing record, or removes this record
	///   from an array/set of references
	/// - **CUSTOM**: Executes a custom instruction defined in the schema
	///
	/// After processing all references, this function deletes all reference keys for this
	/// record from the key-value store.
	///
	/// This function runs with permissions disabled to ensure referential integrity
	/// operations can complete regardless of user permissions.
	async fn purge_references(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
	) -> Result<()> {
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace id
		let ns = self.doc_ctx.ns().namespace_id;
		// Get the database id
		let db = self.doc_ctx.db().database_id;
		// Get the key range of the reference keys
		let prefix = crate::key::r#ref::prefix(ns, db, &rid.table, &rid.key)?;
		let suffix = crate::key::r#ref::suffix(ns, db, &rid.table, &rid.key)?;
		let range = prefix..suffix;
		// Cache the last field definition to avoid redundant lookups
		let mut prev: Option<(TableName, String, Arc<FieldDefinition>)> = None;
		// Track whether any reference key was actually observed; if none
		// were, the trailing range delete is a no-op we can skip.
		let mut saw_reference_key = false;
		// Obtain a cursor over the reference range.
		let mut cursor =
			txn.open_keys_cursor(range.clone(), ScanDirection::Forward, 0, None).await?;
		// Loop until no more entries
		loop {
			// Pull the next batch of reference keys from the cursor.
			let batch = cursor.next_batch(ScanLimit::Count(NORMAL_BATCH_SIZE)).await?;
			// Stop once the cursor is drained.
			if batch.is_empty() {
				break;
			}
			// Copy each borrowed key into an owned `Vec<u8>` up front so
			// that the downstream `await`s (which call back into the same
			// transaction via `get_tb_field` etc.) don't conflict with
			// the cursor's `&mut self` borrow.
			let keys: Vec<Vec<u8>> = batch.iter().map(|k| k.to_vec()).collect();
			// Process each key in the batch
			for key in keys {
				yield_now!();
				// We saw a reference key
				saw_reference_key = true;
				// Decode the key into a reference
				let key = Ref::decode_key(&key)?;
				// Extract the foreign table name
				let ft = key.ft.as_ref();
				// Extract the foreign field name
				let ff = key.ff.as_ref();
				// Get the reference field definition
				let fd = match prev {
					// If the field definition is in the cache, return it
					Some((ref cft, ref cff, ref cfd)) if ft == cft && ff == cff => Arc::clone(cfd),
					// Otherwise let's fetch it from the datastore cache
					_ => {
						// Fetch the field definition from the transaction
						let Some(fd) = txn.get_tb_field(ns, db, ft, ff, None).await? else {
							return Err(Error::FdNotFound {
								name: ff.to_string(),
							}
							.into());
						};
						// Store the field definition in the cache
						prev = Some((ft.clone(), ff.to_string(), Arc::clone(&fd)));
						// Return the field definition
						fd
					}
				};
				// Check if there is a reference defined on the field
				if let Some(reference) = &fd.reference {
					match &reference.on_delete {
						// Ignore this reference
						ReferenceDeleteStrategy::Ignore => (),
						// Reject the delete operation, as indicated by the reference
						ReferenceDeleteStrategy::Reject => {
							let record = RecordId {
								table: key.ft.into_owned(),
								key: key.fk.into_owned(),
							};

							bail!(Error::DeleteRejectedByReference(rid.to_sql(), record.to_sql(),));
						}
						// Delete the remote record which referenced this record
						ReferenceDeleteStrategy::Cascade => {
							let record_id = RecordId {
								table: key.ft.into_owned(),
								key: key.fk.into_owned(),
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
								table: key.ft.into_owned(),
								key: key.fk.into_owned(),
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
								table: key.ft.into_owned(),
								key: key.fk.into_owned(),
							};

							// Set the `$reference` variable in the context
							let mut ctx = Context::new_child(ctx);
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
		// After all references have been processed, remove the reference
		// keys we saw. Skip the range delete entirely when no reference
		// keys were observed — there's nothing to clear and the empty
		// range delete still records a transaction op.
		if saw_reference_key {
			txn.delr(range).await?;
		}
		// Carry on
		Ok(())
	}
}
