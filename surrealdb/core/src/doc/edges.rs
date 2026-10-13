use std::borrow::Cow;

use anyhow::{Result, ensure};
use surrealdb_datastore::values::graph::AdjacencyValue;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{LATEST_EDGE_VARIANT, Relation, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::{Document, Error, Extras};
use crate::expr::Dir;
use crate::expr::paths::{IN, OUT};
use crate::key::schema::{GraphKey, GraphPointerKey};

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
	/// before creating the edges — except under `OPTION IMPORT`, where the check is
	/// deferred because a restore can reach an edge table before its endpoints. It also
	/// marks the record metadata as an edge type and stores the `in` and `out` fields on
	/// the document.
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
			// For enforced relations, ensure that both endpoints exist.
			//
			// Skipped while replaying an export. An export orders tables by
			// name, so an edge table is restored before the vertex tables it
			// points at whenever its name sorts earlier, and enforcing here
			// would reject every one of its edges. Enforcement is an
			// admission check on the write path, not an invariant any reader
			// depends on, so deferring it during a restore costs nothing that
			// a query can observe. The sibling checks in `process_table_fields`,
			// `process_table_events`, `process_table_views` and
			// `process_changefeeds` are deferred the same way, for the same
			// reason: an export is replayed as a whole or not at all.
			if !opt.import
				&& matches!(
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
			// A lightweight relation cannot itself be a graph endpoint: its
			// "records" are synthesized from adjacency, so hanging classic
			// edges off them would write pointer keys into the lightweight
			// table's own graph subspace — the exact state its emptiness
			// probes and record-less scans define as impossible.
			for endpoint in [l, r] {
				if let Some(def) = txn.get_tb(ns, db, &endpoint.table, None).await?
					&& matches!(
						&def.table_type,
						TableType::Relation(Relation {
							lightweight: true,
							..
						})
					) {
					anyhow::bail!(crate::exec::Error::Thrown(format!(
						"a LIGHTWEIGHT relation's edges cannot be graph endpoints: {}",
						endpoint.to_sql()
					)));
				}
			}
			// A lightweight relation stores each edge as its two vertex-side
			// pointer keys alone: no record, no edge-side keys. Readers
			// synthesize the record from the canonical `[in, out]` id.
			let lightweight = matches!(
				&tb.table_type,
				TableType::Relation(Relation {
					lightweight: true,
					..
				})
			);
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
			// Dispatch on the layout currently on disk — sourced from
			// `initial`, not `current`, because `default_record_data`
			// has already advanced `current`'s stamp to the latest
			// variant. Older variants need their stale vertex-side keys
			// deleted before the current layout is written. Lightweight
			// edges only ever exist in the latest layout.
			let variant = self.initial.doc.edge_variant().unwrap_or(LATEST_EDGE_VARIANT);
			// Detect which variant the edge was originally
			if !lightweight && variant == 1 {
				let ltr_legacy = GraphKey {
					ns,
					db,
					tb: Cow::Borrowed(&l.table),
					id: Cow::Borrowed(&l.key),
					dir: Dir::Out,
					foreign_table: Cow::Borrowed(&rid.table),
					foreign_key: Cow::Borrowed(&rid.key),
				};

				let rtl_legacy = GraphKey {
					ns,
					db,
					tb: Cow::Borrowed(&r.table),
					id: Cow::Borrowed(&r.key),
					dir: Dir::In,
					foreign_table: Cow::Borrowed(&rid.table),
					foreign_key: Cow::Borrowed(&rid.key),
				};
				futures::try_join!(txn.del_key(&ltr_legacy), txn.del_key(&rtl_legacy))?;
			}
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

			// A table with inline fields embeds their values into the pointer
			// values from the first write; without any, this is the same
			// empty-encoding live value pointer keys have always carried.
			let live = if lightweight {
				AdjacencyValue::live()
			} else {
				let inline = super::inline::inline_fields(self.doc_ctx.fd()?);
				if inline.is_empty() {
					AdjacencyValue::live()
				} else {
					let section = super::inline::encode_inline_section(
						self.current.doc.as_ref(),
						&inline,
						tb.graph_inline_gen,
						ctx.config.idx.graph_inline_props_cap,
					)?;
					AdjacencyValue {
						tombstone: false,
						sections: vec![section],
					}
				}
			};
			if lightweight {
				futures::try_join!(txn.set_key(&ltr, &live), txn.set_key(&rtl, &live))?;
			} else {
				futures::try_join!(
					txn.set_key(&ltr, &live),
					txn.set_key(&etl, &()),
					txn.set_key(&etr, &()),
					txn.set_key(&rtl, &live),
				)?;
			}
			// Fold the edge into each endpoint's inline adjacency cache —
			// after the key writes above, so a first-write backfill's scan
			// (which runs in this transaction) already includes them.
			let resolve = Some(ctx.get_index_stores().adjacency_resolve());
			crate::idx::inline_cache::record_edge_write(
				&txn,
				ns,
				db,
				l,
				Dir::Out,
				&rid,
				r,
				resolve,
			)
			.await?;
			crate::idx::inline_cache::record_edge_write(&txn, ns, db, r, Dir::In, &rid, l, resolve)
				.await?;
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
