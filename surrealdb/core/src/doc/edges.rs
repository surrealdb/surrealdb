use anyhow::{Result, ensure};
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{LATEST_EDGE_VARIANT, Relation, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{Document, Extras};
use crate::err::Error;
use crate::expr::Dir;
use crate::expr::paths::{IN, OUT};
use crate::key::graph;

impl Document {
	/// Stores edge data for relation records in the graph database.
	///
	/// This function handles the persistence of graph edges when a relation record is created
	/// or updated. It stores four graph keys that enable bidirectional traversal:
	/// - Left pointer edge: from the `in` record pointing to this relation
	/// - Left inner edge: from this relation pointing to the `in` record
	/// - Right inner edge: from this relation pointing to the `out` record
	/// - Right pointer edge: from the `out` record pointing to this relation
	///
	/// For enforced relations, it validates that both the `in` and `out` records exist
	/// before creating the edges. It also marks the record metadata as an edge type and
	/// stores the `in` and `out` fields on the document.
	pub(super) async fn store_edges_data(
		&mut self,
		ctx: &FrozenContext,
		opt: &Options,
	) -> Result<()> {
		// Get the table
		let tb = self.doc_ctx.tb()?;
		// Check if the table is DROP
		if tb.drop {
			return Ok(());
		}
		// Store the record edges
		if let Extras::Relate(l, r, _) = &self.extras {
			// Get the namespace id
			let ns = self.doc_ctx.ns().namespace_id;
			// Get the database id
			let db = self.doc_ctx.db().database_id;
			// Get the record id
			let rid = self.id()?;
			// Get the transaction
			let txn = ctx.tx();
			// For enforced relations, ensure that the edges exist
			if matches!(
				tb.table_type,
				TableType::Relation(Relation {
					enforced: true,
					..
				})
			) {
				// Check that the `in` record exists
				ensure!(
					txn.record_exists(ns, db, &l.table, &l.key, opt.version).await?,
					Error::IdNotFound {
						rid: l.to_sql(),
					}
				);
				// Check that the `out` record exists
				ensure!(
					txn.record_exists(ns, db, &r.table, &r.key, opt.version).await?,
					Error::IdNotFound {
						rid: r.to_sql(),
					}
				);
			}
			// The four keys written below together model a single relation,
			// linking the `in` vertex (`l`), the edge record (`rid`), and the
			// `out` vertex (`r`):
			//
			//              ltr (target = r)         pointer
			//          ┌─────────────────────┬─ ─ ─ ─ ─ ─ ─ ─ ─ ┐
			//          │                     ▼                  ▼
			//     ┌────┴─────┐   etl  ┌────────────┐  etr   ┌──────────┐
			//     │   left   │───────▶│ rid (edge) │───────▶│  right   │
			//     └──────────┘   in   └────────────┘  out   └────┬─────┘
			//           ▼                    ▼                   │
			//           └ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─┴───────────────────┘
			//                  pointer         rtl (source = l)
			//
			// `ltr` / `rtl` are vertex-side ("pointer") keys: stored on the
			// IN / OUT vertex with the opposite endpoint embedded so that
			// `->edge->vertex` (or its mirror) range scans can resolve the
			// far vertex without reading the edge record.
			//
			// `etl` / `etr` are edge-side ("inner") keys: their adjacency
			// already names the vertex in (ft, fk), so they keep the legacy
			// layout without an embedded target — same across variants.
			let etl = graph::new(ns, db, &rid.table, &rid.key, Dir::In, l);
			let etr = graph::new(ns, db, &rid.table, &rid.key, Dir::Out, r);
			// Dispatch on the layout currently on disk — sourced from
			// `initial`, not `current`, because `default_record_data`
			// has already advanced `current`'s stamp to the latest
			// variant. Older variants need their stale vertex-side keys
			// deleted before the current layout is written.
			let variant = self.initial.doc.edge_variant().unwrap_or(LATEST_EDGE_VARIANT);
			// Detect which variant the edge was originally
			match variant {
				1 => {
					// Delete the legacy vertex-side keys
					let ltr_legacy = graph::new(ns, db, &l.table, &l.key, Dir::Out, &rid);
					let rtl_legacy = graph::new(ns, db, &r.table, &r.key, Dir::In, &rid);
					futures::try_join!(txn.del(&ltr_legacy), txn.del(&rtl_legacy))?;
					// Write the current target-vertex-bearing keys
					let ltr = graph::new_pointer(ns, db, &l.table, &l.key, Dir::Out, &rid, r);
					let rtl = graph::new_pointer(ns, db, &r.table, &r.key, Dir::In, &rid, l);
					futures::try_join!(
						txn.set(&ltr, &()),
						txn.set(&etl, &()),
						txn.set(&etr, &()),
						txn.set(&rtl, &()),
					)?;
				}
				_ => {
					let ltr = graph::new_pointer(ns, db, &l.table, &l.key, Dir::Out, &rid, r);
					let rtl = graph::new_pointer(ns, db, &r.table, &r.key, Dir::In, &rid, l);
					futures::try_join!(
						txn.set(&ltr, &()),
						txn.set(&etl, &()),
						txn.set(&etr, &()),
						txn.set(&rtl, &()),
					)?;
				}
			}
			// Reset `in` / `out` to the canonical RELATE endpoints so a
			// user-supplied document body can't override the edge's
			// graph endpoints.
			self.current.doc.to_mut().put(&IN, l.clone().into());
			self.current.doc.to_mut().put(&OUT, r.clone().into());
		}
		// Carry on
		Ok(())
	}
}
