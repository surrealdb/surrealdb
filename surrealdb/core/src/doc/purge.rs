use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_datastore::values::graph::{AdjacencyValue, GraphFoldScope};
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, FieldDefinition, Relation, TableType};
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::{CursorDoc, Document, Error as DocError};
use crate::exe::FlowResultExt as _;
use crate::expr::data::Assignment;
use crate::expr::dir::Dir;
use crate::expr::lookup::LookupKind;
use crate::expr::paths::{IN, OUT};
use crate::expr::reference::ReferenceDeleteStrategy;
use crate::expr::statements::{DeleteStatement, UpdateStatement};
use crate::expr::{AssignOperator, Data, Expr, Idiom, Literal, Lookup, Part};
use crate::idx::adjacency::{AdjacencyScope, MergedAdjacencyCursor, vertex_table_folded};
use crate::key::KVKeyDecode;
use crate::key::schema::{
	GraphIdPrefix, GraphKey, GraphPointerKey, ReferenceIdPrefix, ReferenceKey,
};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE};
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
			// Purge the record's own inline cache keys, if its table
			// maintains any. The edge cascade below updates the caches of
			// *other* records; these are this record's own.
			crate::idx::inline_cache::purge_record_caches(&txn, ns, db, &rid).await?;
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
		let etl = GraphKey {
			ns,
			db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			dir: Dir::In,
			foreign_table: Cow::Borrowed(&l.table),
			foreign_key: Cow::Borrowed(&l.key),
		};
		let etr = GraphKey {
			ns,
			db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
			dir: Dir::Out,
			foreign_table: Cow::Borrowed(&r.table),
			foreign_key: Cow::Borrowed(&r.key),
		};
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
		if variant == 1 {
			let ltr = GraphKey {
				ns,
				db,
				tb: Cow::Borrowed(&l.table),
				id: Cow::Borrowed(&l.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&rid.table),
				foreign_key: Cow::Borrowed(&rid.key),
			};

			let rtl = GraphKey {
				ns,
				db,
				tb: Cow::Borrowed(&r.table),
				id: Cow::Borrowed(&r.key),
				dir: Dir::In,
				foreign_table: Cow::Borrowed(&rid.table),
				foreign_key: Cow::Borrowed(&rid.key),
			};
			futures::try_join!(
				txn.del_key(&ltr),
				txn.del_key(&etl),
				txn.del_key(&etr),
				txn.del_key(&rtl)
			)?;
			// A legacy-format edge can never sit in a live inline cache (a
			// backfill that observes one spills), so these calls settle to
			// no-ops; they run for uniformity with the pointer branch.
			crate::idx::inline_cache::record_edge_delete(&txn, ns, db, l, Dir::Out, rid).await?;
			crate::idx::inline_cache::record_edge_delete(&txn, ns, db, r, Dir::In, rid).await?;
		} else {
			let ltr = GraphPointerKey {
				ns,
				db,
				tb: Cow::Borrowed(&l.table),
				id: Cow::Borrowed(&l.key),
				dir: Dir::Out,
				foreign_table: Cow::Borrowed(&rid.table),
				foreign_key: Cow::Borrowed(&rid.key),
				target_table: Cow::Borrowed(&r.table),
				target_key: Cow::Borrowed(&r.key),
			};
			let rtl = GraphPointerKey {
				ns,
				db,
				tb: Cow::Borrowed(&r.table),
				id: Cow::Borrowed(&r.key),
				dir: Dir::In,
				foreign_table: Cow::Borrowed(&rid.table),
				foreign_key: Cow::Borrowed(&rid.key),
				target_table: Cow::Borrowed(&l.table),
				target_key: Cow::Borrowed(&l.key),
			};
			// A vertex table with folded adjacency may hold this edge in a
			// packed block, which a key deletion cannot reach: write a
			// tombstone over the pointer key instead — blind either way —
			// which the merged reader subtracts and the next fold consumes.
			// On a never-folded table the key deletion is exact, as always.
			// Both writes are blind: neither reads the pointer key first.
			let (l_folded, r_folded) = futures::try_join!(
				vertex_table_folded(&txn, ns, db, &l.table),
				vertex_table_folded(&txn, ns, db, &r.table)
			)?;
			let tombstone = AdjacencyValue::tombstone();
			// A tombstone is debt the next fold repays; queue that fold at
			// commit so it happens even if the scope is never read again.
			for (folded, vertex, dir) in [(l_folded, &l, Dir::Out), (r_folded, &r, Dir::In)] {
				if folded {
					txn.buffer_graph_fold_trigger(
						GraphFoldScope {
							ns,
							db,
							tb: vertex.table.clone(),
							id: vertex.key.clone(),
							dir,
						},
						ctx.node_id(),
					);
				}
			}
			match (l_folded, r_folded) {
				(false, false) => {
					futures::try_join!(txn.del_key(&ltr), txn.del_key(&rtl))?;
				}
				(true, false) => {
					futures::try_join!(txn.set_key(&ltr, &tombstone), txn.del_key(&rtl))?;
				}
				(false, true) => {
					futures::try_join!(txn.del_key(&ltr), txn.set_key(&rtl, &tombstone))?;
				}
				(true, true) => {
					futures::try_join!(
						txn.set_key(&ltr, &tombstone),
						txn.set_key(&rtl, &tombstone)
					)?;
				}
			}
			// A lightweight edge never wrote edge-side keys.
			if !matches!(
				&self.doc_ctx.tb()?.table_type,
				TableType::Relation(Relation {
					lightweight: true,
					..
				})
			) {
				futures::try_join!(txn.del_key(&etl), txn.del_key(&etr))?;
			}
			// Remove the edge from each endpoint's inline adjacency cache.
			// The cache sits above the merged (blocks ∪ delta) tiers, so the
			// removal is the same whether the key above was deleted or
			// tombstoned.
			crate::idx::inline_cache::record_edge_delete(&txn, ns, db, l, Dir::Out, rid).await?;
			crate::idx::inline_cache::record_edge_delete(&txn, ns, db, r, Dir::In, rid).await?;
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
	/// Unlike [`Self::purge_references`], this peek cannot be eliminated with a
	/// catalog-only check. Edges are created dynamically by `RELATE` with no
	/// static schema flag, and a record in *any* table — including a default
	/// `TableType::Any` table that was never declared a relation — can become a
	/// graph endpoint and gain adjacency keys. So the table catalog cannot
	/// prove a record has no edges. The only correct way to skip would be a
	/// durable, per-table "has any edges" hint maintained on edge write/delete;
	/// that is deferred because a hint cannot be made safe for databases that
	/// already contain edges from before it existed (an absent hint would have
	/// to mean "must scan", so it could never enable skipping for pre-existing
	/// data without a full migration). The per-record peek is therefore left in
	/// place; eliminating the references scan above already removes the larger,
	/// always-empty-in-the-common-case scan from the DELETE hot path.
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
		// Nothing can connect to a lightweight edge — it is rejected as a
		// graph endpoint — and its synthesized adjacency derives from its id
		// regardless of existence, so the peek below would see its own
		// endpoints and cascade a delete over them. Skip outright.
		if matches!(
			&self.doc_ctx.tb()?.table_type,
			TableType::Relation(Relation {
				lightweight: true,
				..
			})
		) {
			return Ok(());
		}
		// Get the transaction
		let txn = ctx.tx();
		// Get the namespace id
		let ns = self.doc_ctx.ns().namespace_id;
		// Get the database id
		let db = self.doc_ctx.db().database_id;
		// Peek the record's adjacency through the merged reader, which spans
		// the per-edge pointer keys and — once the table has folded — the
		// packed blocks, subtracting tombstones so a fully-deleted-but-not-
		// yet-folded vertex does not trigger a cascade over nothing.
		let folded = self.doc_ctx.tb()?.graph_folded;
		let scope = AdjacencyScope {
			ns,
			db,
			vertex: rid,
			dir: None,
			edge_table: None,
			fk_lower: None,
			delta_range: GraphIdPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&rid.table),
				id: Cow::Borrowed(&rid.key),
			}
			.range()?,
		};
		let mut cursor = MergedAdjacencyCursor::open(
			&txn,
			scope,
			folded,
			None,
			Some(ctx.get_index_stores().adjacency_resolve()),
		)
		.await?;
		// Check if there are any edges to purge by fetching at most one key:
		// only emptiness matters here, so nothing is decoded.
		let batch = cursor.next_batch_keys(1).await?;
		drop(cursor);
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
			// Execute the delete statement under the caller's own permission
			// state. An edge table's `PERMISSIONS FOR delete` clause must
			// still apply even when the cascade is triggered by deleting one
			// of the endpoint vertices, otherwise an actor with vertex-delete
			// permission could erase edges they are not allowed to remove
			// directly. `new_with_caller_perms` matters when this delete was
			// itself reached through a `REFERENCE ON DELETE CASCADE`, which
			// disables `perms` for the referencing record: that bypass exists
			// to complete referential integrity and must not extend to the
			// edge cascade.
			let opt = opt.new_with_caller_perms();
			crate::legacy::delete_statement_compute(&stm, stk, ctx, &opt, None).await?;
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
	///
	/// As an optimization, the reference range scan is skipped entirely when
	/// the schema proves no `DEFINE FIELD ... REFERENCE` anywhere in the
	/// database could target this record's table: a reference key is only ever
	/// written under a record's range while such a field exists, so with none
	/// present there is nothing to find or clean. This avoids one read
	/// round-trip per deleted record on the distributed backend for the common
	/// case of tables with no incoming references.
	async fn purge_references(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		rid: &RecordId,
	) -> Result<()> {
		let txn = ctx.tx();
		let ns = self.doc_ctx.ns().namespace_id;
		let db = self.doc_ctx.db().database_id;
		// Skip the scan when no reference field in the database can target this
		// table. A reference key under this record's range is only ever written
		// while some `DEFINE FIELD ... REFERENCE` can hold a record id of this
		// table, so if none can, there are no reference keys to process and the
		// scan (plus its trailing range delete) is pure overhead. This is a
		// catalog-only check served from the transaction cache, so it costs no
		// per-record round-trips. The gate is sound because every path that
		// stops a field from targeting a table also purges that table's
		// reference keys for it — `REMOVE FIELD`, `ALTER FIELD`, and
		// `DEFINE FIELD ... OVERWRITE` all call `purge_dropped_reference_keys` —
		// so "no current reference field can target this table" implies there
		// are no reference keys left to clean. (Should an inert key nonetheless
		// outlive its field definition — e.g. data from an older release —
		// skipping stays correct: with no field definition there is no ON DELETE
		// strategy to apply, and it avoids the `FdNotFound` the scan would
		// otherwise raise when decoding such an orphaned key.)
		if !txn.table_may_have_incoming_references(ns, db, &rid.table).await? {
			return Ok(());
		}

		let range = ReferenceIdPrefix {
			ns,
			db,
			tb: Cow::Borrowed(&rid.table),
			id: Cow::Borrowed(&rid.key),
		}
		.range()?;

		// Cache the last field definition to avoid redundant lookups
		let mut prev: Option<(TableName, String, Arc<FieldDefinition>)> = None;
		// Track whether any reference key was actually observed; if none
		// were, the trailing range delete is a no-op we can skip.
		let mut saw_reference_key = false;
		// Obtain a cursor over the reference range.
		let mut cursor = txn.open_keys_cursor(range.clone(), Direction::Forward, 0, None).await?;
		// Loop until no more entries
		loop {
			// Pull the next batch of reference keys from the cursor.
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			// Stop once the cursor is drained.
			if batch.is_empty() {
				break;
			}
			// Decode the batch into owned references up front, so that the
			// downstream `await`s (which call back into the same transaction
			// via `get_tb_field` etc.) don't conflict with the cursor's
			// `&mut self` borrow. Owning here rather than copying the bytes
			// also means the `into_owned` calls below move out of the decoded
			// key instead of cloning out of the batch.
			let keys = batch
				.iter()
				.map(|k| Ok(ReferenceKey::decode_key(k)?.into_owned()))
				.collect::<Result<Vec<_>>>()?;
			// Process each key in the batch
			for key in keys {
				yield_now!();
				// We saw a reference key
				saw_reference_key = true;
				// Extract the foreign table name
				let ft = key.foreign_table.as_ref();
				// Extract the foreign field name
				let ff = key.foreign_field.as_ref();
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
								table: key.foreign_table.into_owned(),
								key: key.foreign_key.into_owned(),
							};

							bail!(DocError::DeleteRejectedByReference(
								rid.to_sql(),
								record.to_sql(),
							));
						}
						// Delete the remote record which referenced this record
						ReferenceDeleteStrategy::Cascade => {
							let record_id = RecordId {
								table: key.foreign_table.into_owned(),
								key: key.foreign_key.into_owned(),
							};

							// Setup the delete statement
							let stm = DeleteStatement {
								what: vec![Expr::Literal(Literal::RecordId(
									record_id.into_literal(),
								))],
								..DeleteStatement::default()
							};
							// Execute the delete statement
							crate::legacy::delete_statement_compute(
								&stm,
								stk,
								ctx,
								&opt.new_for_reference_cascade(),
								None,
							)
							.await
							// Wrap any error in an error explaining what went wrong
							.map_err(|e| {
								DocError::RefsUpdateFailure(rid.to_sql(), e.to_string())
							})?;
						}
						// Delete only the reference on the remote record
						ReferenceDeleteStrategy::Unset => {
							let opt = opt.clone().with_perms(false);
							let record = RecordId {
								table: key.foreign_table.into_owned(),
								key: key.foreign_key.into_owned(),
							};

							if let Some(doc) = crate::legacy::record_id_select_document(
								record.clone(),
								stk,
								ctx,
								&opt,
								None,
							)
							.await?
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
									crate::legacy::update_statement_compute(
										&stm, stk, ctx, &opt, None,
									)
									.await
									// Wrap any error in an error explaining what went wrong
									.map_err(|e| {
										DocError::RefsUpdateFailure(rid.to_sql(), e.to_string())
									})?;
								}
							}
						}
						// Process a custom delete strategy
						ReferenceDeleteStrategy::Custom(expr) => {
							// Value for the `$reference` variable is the current record
							let reference = Value::from(rid.clone());
							// Value for the document is the remote record
							let this = RecordId {
								table: key.foreign_table.into_owned(),
								key: key.foreign_key.into_owned(),
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
							stk.run(|stk| {
								crate::legacy::expr_compute(expr, stk, &ctx, &opt, Some(&doc))
							})
							.await
							.catch_return()
							// Wrap any error in an error explaining what went wrong
							.map_err(|e| {
								DocError::RefsUpdateFailure(rid.to_sql(), e.to_string())
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
