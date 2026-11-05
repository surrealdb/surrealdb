use std::borrow::Cow;

use anyhow::Result;
use surrealdb_types::ToSql;

use crate::catalog::providers::TableProvider;
use crate::catalog::{DatabaseId, NamespaceId, Record, StoredRecord, TableDefinition, TableType};
use crate::exec::ExecutionContext;
use crate::key::schema::{
	FieldPrefix, ForeignTablePrefix, LayoutPrefix, RecordKey, RecordPrefix, TableConformanceKey,
	TableMigrationsKey, TblRoot,
};
use crate::key::{KVKey, KVKeyDecode, KVSubspace, RawRange};
use crate::kvs::{NORMAL_BATCH_SIZE, Transaction};
use crate::val::{RecordId, TableName};

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

/// Refuses a change of a table's type that a record the table holds does not
/// admit: `RELATION` while it holds a record that is no edge, `NORMAL` while it
/// holds an edge. A record keeps its kind, and a write stores a record only in
/// a table whose type admits its kind, so a table whose new type excluded one
/// of its own records would hold a record no write could have stored — one its
/// export writes back by its own kind and its import refuses.
///
/// `ANY` admits both kinds, so a change to it is never refused. A kind the
/// previous type already excluded is not looked for, and a table with no
/// previous definition holds no record. A definition that is a view is not
/// judged: the statement that defines it replaces the table's records with the
/// view's.
///
/// The records are read in pages sized to the fuel left, a unit charged per
/// record, up to the first one refused. Whether a record is an edge is read off
/// its envelope's header where it has one, so a record is decoded only when it
/// was stored in the `revision` encoding.
pub(crate) async fn validate_records_survive_type_change(
	exec: &ExecutionContext,
	txn: &Transaction,
	existing: Option<&TableDefinition>,
	def: &TableDefinition,
) -> Result<()> {
	let Some(existing) = existing else {
		return Ok(());
	};
	let refuse_records = existing.allows_normal() && !def.allows_normal();
	let refuse_edges = existing.allows_relation() && !def.allows_relation();
	if def.view.is_some() || !(refuse_records || refuse_edges) {
		return Ok(());
	}
	let (ns, db, table) = (existing.namespace_id, existing.database_id, &def.name);
	let records = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(table),
	};
	let mut next = Some(records.range()?);
	while let Some(range) = next {
		let size = crate::exec::vm::fuel::batch(NORMAL_BATCH_SIZE);
		let page = txn.batch_keys_vals_raw(range, size, None).await?;
		next = page.next.map(|range| records.raw(range));
		if page.result.is_empty() {
			break;
		}
		for (key, value) in &page.result {
			let edge = match StoredRecord::parse(value)? {
				Some(stored) => stored.is_edge(),
				// The `revision` encoding names no layout generation, so it
				// decodes without the table's layouts.
				None => Record::decode_stored(value, None, record_id(table, key)?)?.is_edge(),
			};
			let record = || record_id(table, key).map(|id| id.to_sql());
			if refuse_records && !edge {
				anyhow::bail!(crate::exec::Error::TableTypeRelationOverRecord {
					table: table.as_str().to_owned(),
					record: record()?,
				});
			}
			if refuse_edges && edge {
				anyhow::bail!(crate::exec::Error::TableTypeNormalOverEdge {
					table: table.as_str().to_owned(),
					record: record()?,
				});
			}
		}
		crate::exec::vm::fuel::tick(exec, crate::exec::vm::fuel::units(page.result.len()))
			.await
			.map_err(|flow| {
				flow.into_error_or(|other| {
					anyhow::anyhow!("the scan of the records was interrupted by {other}")
				})
			})?;
	}
	Ok(())
}

/// Deletes every key beneath the root of the table `table` of the database
/// `ns`, `db` except the registrations of the views that read it, under
/// `keep_fields` its field definitions, and under `keep_records` its records
/// and the layout state they are decoded through (its layout descriptors,
/// generation ring and conformance record). The definitions of the table's
/// indexes, events and subscriptions go, with its index entries and the
/// references and edges stored under its records.
///
/// `keep_fields` holds when the definition being replaced is itself a view: a
/// view redefined over a new query keeps the fields declared on it, as a plain
/// table's `OVERWRITE` does. A table that becomes a view starts with none of
/// the old table's fields.
///
/// A registration (`ForeignTableKey`) is written by the view that reads the
/// table, and stays for as long as that view's definition reads it. `DEFINE
/// TABLE ... AS SELECT` clears the table twice: keeping the records, which it
/// then retires through the view table's order so the views reading it take
/// their removal, and then, with no record left, keeping the registrations
/// alone.
pub(crate) async fn clear_view_table(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	keep_fields: bool,
	keep_records: bool,
) -> Result<()> {
	let tb = Cow::Borrowed(table);
	let root = TblRoot {
		ns,
		db,
		tb: tb.clone(),
	}
	.range_subtree()?;
	// Each region kept, as the bounds `[start, end)` of its keys.
	let bounds = |range: RawRange| {
		let (lo, hi) = range.bounds();
		(lo.to_vec(), hi.to_vec())
	};
	let mut kept = vec![bounds(
		ForeignTablePrefix {
			ns,
			db,
			tb: tb.clone(),
		}
		.range_subtree()?
		.into(),
	)];
	if keep_fields {
		kept.push(bounds(
			FieldPrefix {
				ns,
				db,
				tb: tb.clone(),
			}
			.range_subtree()?
			.into(),
		));
	}
	if keep_records {
		kept.push(bounds(
			RecordPrefix {
				ns,
				db,
				tb: tb.clone(),
			}
			.range_subtree()?,
		));
		kept.push(bounds(
			LayoutPrefix {
				ns,
				db,
				tb: tb.clone(),
			}
			.range_subtree()?
			.into(),
		));
		let ring = TableMigrationsKey {
			ns,
			db,
			tb: tb.clone(),
		}
		.encode_key()?;
		let conformance = TableConformanceKey {
			ns,
			db,
			tb,
		}
		.encode_key()?;
		// A single key is kept as itself up to its immediate successor.
		for key in [ring, conformance] {
			let start = key.as_slice().to_vec();
			kept.push((start, key.next().into_vec()));
		}
	}
	kept.sort();
	let (start, end) = root.bounds();
	let mut from = start.to_vec();
	for (lo, hi) in &kept {
		txn.delr(root.part(&from, lo)).await?;
		from.clone_from(hi);
	}
	txn.delr(root.part(&from, end)).await
}

/// Refuses the view `view` of the database `ns`, `db` reading `sources` where
/// one of them is `view` itself, or reads `view` directly or through other
/// views, as the registrations of the views stand in `txn`. Each row such a
/// view wrote would reach the view again through its own maintenance.
pub(crate) async fn refuse_view_cycle(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	view: &TableName,
	sources: &[TableName],
) -> Result<()> {
	let mut readers = std::collections::HashSet::new();
	let mut pending = vec![view.clone()];
	while let Some(table) = pending.pop() {
		for reader in txn.all_tb_views(ns, db, &table, None).await?.iter() {
			if readers.insert(reader.name.clone()) {
				pending.push(reader.name.clone());
			}
		}
	}
	if let Some(source) = sources.iter().find(|source| *source == view || readers.contains(*source))
	{
		let through = match source == view {
			true => "itself".to_owned(),
			false => format!("`{source}`, a view that reads `{view}`"),
		};
		anyhow::bail!(crate::exec::Error::Query {
			message: format!("Cannot define the view `{view}` over {through}"),
		});
	}
	Ok(())
}

/// The id of the record of `table` stored at `key`.
fn record_id(table: &TableName, key: &[u8]) -> Result<RecordId> {
	Ok(RecordId {
		table: table.clone(),
		key: RecordKey::decode_key(key)?.id.into_owned(),
	})
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

/// What a table becomes that its record id, with named components, cannot be.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TableConversion {
	/// A relation table, whose record id is its own key beside `in` and `out`.
	Relation,
	/// A view, whose record ids are derived from its query.
	View,
}

impl TableConversion {
	/// What the table becomes, as the refusal names it.
	fn describes(self) -> &'static str {
		match self {
			Self::Relation => "a relation table",
			Self::View => "a view",
		}
	}
}

/// Refuses turning the table `table` into `to` while its record id names
/// components. A table with no `id` definition, or one that names nothing,
/// passes.
pub(crate) async fn refuse_named_id_conversion(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &crate::val::TableName,
	to: TableConversion,
) -> Result<()> {
	anyhow::ensure!(
		crate::ddl::key_scan::stored_key(txn, ns, db, table).await?.is_none(),
		crate::exec::Error::IdComponentsTableConversion {
			table: table.as_str().to_owned(),
			to: to.describes().to_owned(),
		}
	);
	Ok(())
}

/// [`refuse_named_id_conversion`] for the table `previous` altered into `def`:
/// turning a table whose record id names components into a relation or a view
/// is refused. A definition that stays what `previous` was passes without a
/// catalog read, since a relation or a view never has a record id that names
/// components.
pub(crate) async fn validate_named_id_survives_conversion(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	previous: &TableDefinition,
	def: &TableDefinition,
) -> Result<()> {
	let was_relation = matches!(previous.table_type, TableType::Relation(_));
	let to = match (&def.table_type, &def.view) {
		(TableType::Relation(_), _) if !was_relation => TableConversion::Relation,
		(_, Some(_)) if previous.view.is_none() => TableConversion::View,
		_ => return Ok(()),
	};
	refuse_named_id_conversion(txn, ns, db, &def.name, to).await
}
