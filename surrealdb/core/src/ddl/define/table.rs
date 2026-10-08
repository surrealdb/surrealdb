use std::borrow::Cow;

use anyhow::Result;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, TableDefinition, TableType};
use crate::key::schema::RecordPrefix;
use crate::kvs::Transaction;

/// Rejects a table-type change away from `RELATION` while the table still
/// carries `INLINE` fields: their payloads ride the edges' adjacency
/// values, which only relation records have, so the fields must be
/// un-inlined (or removed) first.
pub(crate) async fn validate_inline_fields_survive_type_change(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: Option<&TableDefinition>,
	def: &TableDefinition,
) -> Result<()> {
	let was_relation = existing.is_some_and(|e| matches!(e.table_type, TableType::Relation(_)));
	if !was_relation || matches!(def.table_type, TableType::Relation(_)) {
		return Ok(());
	}
	let has_inline = txn.all_tb_fields(ns, db, &def.name, None).await?.iter().any(|fd| fd.inline);
	anyhow::ensure!(
		!has_inline,
		crate::exec::Error::Thrown(format!(
			"the relation `{}` still has INLINE fields; remove or un-inline them before \
			 changing its type",
			def.name
		))
	);
	Ok(())
}

/// Validates a table definition that declares — or previously declared — a
/// lightweight relation, normalising what the flag implies.
///
/// A lightweight relation's edges are vertex-side adjacency keys alone, so
/// the definition must rule out everything that assumes records exist:
/// endpoints must be explicit (the record-less scan enumerates the `IN`
/// tables) and may only grow while edges exist, enforcement is implied
/// (vertex deletion is the only cleanup path), and the record-consuming
/// clauses — views, changefeeds, DROP, SCHEMAFULL — are rejected. Turning
/// the flag on over an existing table requires it to be provably empty of
/// records, graph keys and user schema objects; turning it off is never
/// allowed, because the stored edges have no records to fall back to.
pub(crate) async fn validate_lightweight_table(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: Option<&TableDefinition>,
	def: &mut TableDefinition,
) -> Result<()> {
	use surrealdb_types::ToSql;

	use crate::exec::Error as ExecError;
	use crate::kvs::lightweight::lightweight_relation;

	let was = existing.and_then(|e| lightweight_relation(&e.table_type)).cloned();
	let is_lightweight = matches!(&def.table_type, TableType::Relation(rel) if rel.lightweight);
	if !is_lightweight {
		// Clearing the flag is allowed only while the relation is provably
		// empty: its stored edges have no records to fall back to, so a
		// non-empty relation losing the flag would strand them. The empty
		// escape also makes a table recoverable if a racing writer ever
		// slips an edge past the becoming-lightweight probes below (see
		// their comment).
		if let Some(old) = was {
			anyhow::ensure!(
				crate::kvs::lightweight::lightweight_relation_is_empty(
					txn, ns, db, &def.name, &old
				)
				.await?,
				ExecError::Thrown(
					"a LIGHTWEIGHT relation can only be redefined as something else while it \
					 holds no edges: delete them first"
						.to_owned()
				)
			);
		}
		return Ok(());
	}
	validate_lightweight_clauses(def)?;
	let TableType::Relation(rel) = &def.table_type else {
		unreachable!("is_lightweight implies a relation");
	};
	let rel = rel.clone();
	match (existing, was) {
		(_, Some(old)) => {
			// Narrowing IN or OUT would orphan edges the scan and the auto
			// in/out field checks then no longer cover; allowed only while
			// the relation is provably empty.
			let narrowed = old.from.iter().any(|t| !rel.from.contains(t))
				|| old.to.iter().any(|t| !rel.to.contains(t));
			if narrowed {
				anyhow::ensure!(
					crate::kvs::lightweight::lightweight_relation_is_empty(
						txn, ns, db, &def.name, &old
					)
					.await?,
					ExecError::Thrown(
						"a LIGHTWEIGHT relation's IN and OUT tables can only grow while it \
						 holds edges"
							.to_owned()
					)
				);
			}
		}
		(Some(_), None) => {
			// Becoming lightweight: the table must be provably empty — no
			// records, no graph keys of its own, no user schema objects —
			// because nothing migrates existing state into the record-less
			// form.
			//
			// The proof reads at this DDL transaction's snapshot, so — as
			// with every schema-shape flip in this executor — a writer
			// concurrently inserting into the pre-flip table can commit
			// beside it with a disjoint write set. The empty-only clearing
			// escape above keeps that window recoverable: delete the
			// slipped edges and redefine.
			let records = RecordPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&def.name),
			};
			let graph = crate::key::schema::GraphPrefix {
				ns,
				db,
				tb: Cow::Borrowed(&def.name),
			};
			let has_records = !txn.keys_raw(records.range()?, 1, 0, None).await?.is_empty();
			let has_graph = !txn.keys_raw(graph.range()?, 1, 0, None).await?.is_empty();
			anyhow::ensure!(
				!has_records && !has_graph,
				ExecError::Thrown(
					"only an empty table can become a LIGHTWEIGHT relation: it still holds \
					 records or graph keys"
						.to_owned()
				)
			);
			let user_fields = txn.all_tb_fields(ns, db, &def.name, None).await?.iter().any(|fd| {
				let name = fd.name.to_sql();
				name != "in" && name != "out"
			});
			let has_events = !txn.all_tb_events(ns, db, &def.name, None).await?.is_empty();
			let has_indexes = !txn.all_tb_indexes(ns, db, &def.name, None).await?.is_empty();
			anyhow::ensure!(
				!user_fields && !has_events && !has_indexes,
				ExecError::Thrown(
					"only a table without fields, events or indexes can become a LIGHTWEIGHT \
					 relation: its edges store no records for them to apply to"
						.to_owned()
				)
			);
		}
		(None, None) => {}
	}
	Ok(())
}

/// The checks a LIGHTWEIGHT relation's definition must pass on its own
/// clauses, which read no catalog: explicit endpoints, none of the
/// record-consuming clauses, and the enforcement it implies, which this sets.
/// A definition that is not a lightweight relation passes unchanged.
pub(crate) fn validate_lightweight_clauses(def: &mut TableDefinition) -> Result<()> {
	use crate::exec::Error as ExecError;

	let TableType::Relation(rel) = &mut def.table_type else {
		return Ok(());
	};
	if !rel.lightweight {
		return Ok(());
	}
	anyhow::ensure!(
		!rel.from.is_empty() && !rel.to.is_empty(),
		ExecError::Thrown(
			"a LIGHTWEIGHT relation requires explicit IN and OUT tables: the record-less scan \
			 enumerates its edges from the IN tables' adjacency"
				.to_owned()
		)
	);
	// Vertex deletion is the only cleanup path a record-less edge has, so
	// dangling endpoints would be permanent garbage: enforcement is implied.
	rel.enforced = true;
	for (rejected, why) in [
		(def.drop, "DROP"),
		(def.view.is_some(), "AS"),
		(def.changefeed.is_some(), "CHANGEFEED"),
		(def.schemafull, "SCHEMAFULL"),
	] {
		anyhow::ensure!(
			!rejected,
			ExecError::Thrown(format!(
				"a LIGHTWEIGHT relation cannot be declared {why}: its edges store no records"
			))
		);
	}
	Ok(())
}
