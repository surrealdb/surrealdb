use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{
	self, DatabaseId, Error as CatalogError, NamespaceId, Relation, TableType, ViewDefinition,
};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::ddl::key_scan::{
	ReadContext, RecordReads, first_of, holds_rows, record_reads, view_reads,
};
use crate::exec::Error as ExecError;
use crate::exec::field_rules::{
	define_field_nesting_restrictions, define_field_statement_validate_reference_options,
	implied_definition, implied_definitions, implied_definitions_dropped,
};
use crate::expr::statements::define::DefineKind;
use crate::expr::{Base, Idiom, Kind};
use crate::iam::{Action, ResourceKind};
use crate::kvs::Transaction;
use crate::val::{TableName, Value};

/// The check `DEFINE FIELD` makes while it forms a `COMPUTED` field's
/// definition: no index of the table covers the field.
pub(crate) async fn define_field_definition_check_indexed(
	definition: &catalog::FieldDefinition,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<()> {
	if definition.computed.is_some() {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		define_field_definition_check_indexed_computed(&ctx.tx(), ns, db, definition).await?;
	}
	Ok(())
}

/// Define the field `definition` describes, as `DEFINE FIELD` does once its
/// definition is formed: `kind` decides what an existing definition of the
/// field does.
pub(crate) async fn define_field_definition_apply(
	kind: &DefineKind,
	definition: &catalog::FieldDefinition,
	migration: crate::exec::migrate::FieldMigration<'_>,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Field, Base::Db)?;

	// Validate any GRAPHQL_ALIAS at definition time so typos surface here
	// rather than silently falling back at schema-generation time.
	crate::ddl::define::validate_graphql_alias(&definition.graphql_alias, "field")?;

	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;

	// Validate computed options
	crate::ddl::define_field_statement_validate_computed_options(ns, db, ctx.tx(), definition)
		.await?;

	// The field's clauses hold no effect a schema clause must not have. An
	// import restores definitions as they were stored, in export order, so a
	// callee may be defined after the field that names it.
	if !opt.import {
		crate::exec::clause_effects::ClauseCheck {
			txn: &ctx.tx(),
			ns,
			db,
			registry: ctx.function_registry(),
		}
		.refuse_field(None, definition)
		.await?;
	}

	// Validate computed field dependencies for cycles
	crate::ddl::define_field_statement_validate_computed_cycles(ns, db, ctx.tx(), definition)
		.await?;

	// Validate reference options
	crate::exec::field_rules::define_field_statement_validate_reference_options(definition)?;

	// Disallow mismatched types
	crate::ddl::define_field_statement_disallow_mismatched_types(&ctx.tx(), ns, db, definition)
		.await?;

	// Validate id field restrictions
	crate::exec::field_rules::id_field_restrictions(definition)?;

	// Validate the record id components the definition names, or the field
	// it defines under the name of one
	crate::ddl::define_field_statement_validate_key_components(&ctx.tx(), ns, db, kind, definition)
		.await?;

	// Validate FLEXIBLE restrictions
	crate::ddl::define_field_statement_validate_flexible_restrictions(
		&ctx.tx(),
		ns,
		db,
		definition,
	)
	.await?;

	// Fetch the transaction
	let txn = ctx.tx();

	let tb = txn.get_or_add_tb(Some(ctx), ns_name, db_name, &definition.table, None).await?;
	let tb_name = tb.name.clone();

	// A lightweight relation's edges store no records for a field definition to
	// apply to; the auto `in`/`out` fields are written by the table
	// definition directly, never through this statement.
	anyhow::ensure!(
		crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_none(),
		crate::exec::Error::Thrown(
			"a LIGHTWEIGHT relation cannot carry a field definition: its edges store no records"
				.to_owned(),
		)
	);

	// INLINE embeds the field's value into the edge's adjacency payloads;
	// it only means something on stored edges. It is skipped on import, so
	// an export whose table is no longer a relation still replays.
	if definition.inline && !opt.import {
		anyhow::ensure!(
			existing_table_is_relation(&txn, ns, db, &definition.table).await?,
			crate::exec::Error::Thrown(
				"INLINE requires an existing TYPE RELATION table: the value is embedded into \
				 the edges' adjacency payloads"
					.to_owned(),
			)
		);
		let name = definition.name.to_raw_string();
		anyhow::ensure!(
			!matches!(name.as_str(), "id" | "in" | "out"),
			crate::exec::Error::Thrown(
				"the `id`, `in` and `out` fields already live in the adjacency keys and cannot \
				 be INLINE"
					.to_owned(),
			)
		);
		anyhow::ensure!(
			definition.name.len() == 1,
			crate::exec::Error::Thrown("only a top-level field can be INLINE".to_owned(),)
		);
		anyhow::ensure!(
			definition.computed.is_none(),
			crate::exec::Error::Thrown(
				"a COMPUTED field cannot be INLINE: its value is derived at read time, so \
				 there is nothing stored to embed"
					.to_owned(),
			)
		);
		// The payload's value count is a single byte, so a table carries at
		// most 255 INLINE fields; reject the overflowing definition here
		// rather than failing every subsequent edge write.
		let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;
		let others = fields.iter().filter(|f| f.inline && f.name != definition.name).count();
		anyhow::ensure!(
			others < u8::MAX as usize,
			crate::exec::Error::Thrown(
				"a table cannot carry more than 255 INLINE fields: the adjacency payload's \
				 value count is a single byte"
					.to_owned(),
			)
		);
	}

	// Get the name of the field. Use the resolved name (with parameterized
	// indices substituted) so duplicate detection matches what `put_tb_field`
	// will store; otherwise a second DEFINE FIELD with the same resolved
	// path silently overwrites the first.
	let fd = catalog::FieldDefinition::storage_name(&definition.name);
	// Check if the definition exists
	let existing = txn.get_tb_field(ns, db, &tb_name, &fd, None).await?;
	if let Some(existing) = &existing {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(CatalogError::FdAlreadyExists {
						name: existing.name.to_sql(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => {
				return Ok(Value::None);
			}
		}
	}

	// A `MATERIALIZE INTO` clause fills a destination its table declares, and a
	// definition that replaces a destination is refused. An import restores
	// definitions as they were.
	if !opt.import {
		if existing.is_some() {
			crate::ddl::materialize::refuse_changing_destination(
				&txn,
				ns,
				db,
				&tb_name,
				&definition.name,
			)
			.await?;
		}
		crate::ddl::materialize::validate(&txn, ns, db, definition).await?;
	}

	// Refuse a field nested in something that cannot hold it, from either
	// end. An import skips this check, so exported definitions still replay.
	if !opt.import {
		let fields = txn.all_tb_fields(ns, db, &tb_name, None).await?;
		define_field_nesting_restrictions(&fields, definition)?;
	}

	// A record id that names its components must admit every record the table
	// holds; the scan is skipped when the stored `id` already has the same
	// structure. A definition that leaves the table with no structure is held
	// to the order the records it already holds are stored in instead.
	if definition.live_key().is_some() || definition.name.is_id() {
		let exec = crate::ddl::frame(ctx, opt, None).await;
		match definition.live_key() {
			Some(key) => {
				crate::ddl::key_scan::validate_record_keys(&exec, &txn, ns, db, &tb_name, key)
					.await?;
			}
			None => {
				crate::ddl::key_scan::refuse_dropping_unordered_keys_for(
					&exec, &txn, ns, db, definition,
				)
				.await?;
			}
		}
	}

	// A restore defines the table before it holds a record, so an import has
	// no group an aggregated view could have keyed by an earlier derivation.
	if !opt.import {
		define_field_statement_validate_computed_views(
			&txn,
			ns,
			db,
			existing.as_deref(),
			definition,
		)
		.await?;
	}

	// The rules of a table that enforces migrations, and the statement's migration, are judged
	// once every other check of the definition has passed: either can mint a step, and a refusal
	// leaves no step behind for a transaction its caller goes on to commit.
	let exec = crate::ddl::frame(ctx, opt, None).await;
	let stored = migration
		.before_store(&exec, opt.import, ns, db, &tb_name, definition, existing.as_deref())
		.await?;
	if stored == crate::exec::migrate::FieldStore::AlreadyRun {
		return Ok(Value::None);
	}

	// Process the statement
	txn.put_tb_field(ns, db, &tb_name, definition).await?;
	crate::exec::migrate::FieldMigration::after_store(
		stored,
		&exec,
		opt.import,
		ns,
		db,
		&tb_name,
		definition,
		existing.as_deref(),
	)
	.await?;

	// Overwriting an existing reference field can drop target tables it
	// referenced (the REFERENCE clause removed, or the record kind narrowed or
	// changed), whose stranded keys are purged so the DELETE reference-purge
	// gate stays sound; a field that starts or retargets its REFERENCE over
	// stored records gets the keys of the links they hold. Skipped during
	// import, which restores reference keys verbatim.
	if !opt.import {
		let frame = crate::ddl::frame(ctx, opt, None).await;
		crate::exec::reference_purge::reconcile_reference_keys(
			&frame,
			ns,
			db,
			&tb_name,
			existing.as_deref(),
			definition,
		)
		.await?;
		crate::exec::materialize::backfill(&frame, existing.as_deref(), definition, true).await?;
	}

	// Refresh the table cache
	let mut tb = catalog::TableDefinition {
		cache_fields_ts: Uuid::now_v7(),
		..(*tb).clone()
	};

	// If this is an `in` field then check relation definitions
	if fd.as_str() == "in" {
		// The table is marked as TYPE RELATION
		if let TableType::Relation(ref relation) = tb.table_type {
			// Check if a field TYPE has been specified
			if let Some(kind) = definition.field_kind.as_ref() {
				let Kind::Record(field_kind) = kind else {
					bail!(ExecError::Thrown("in field on a relation must be a record".into(),))
				};

				// Add the TYPE to the DEFINE TABLE statement
				if *field_kind != relation.from {
					// Refresh the table cache
					tb.table_type = TableType::Relation(Relation {
						from: field_kind.clone(),
						..relation.clone()
					});
					txn.replace_tb(ns_name, db_name, &tb).await?;
					// Clear the cache
					txn.clear_cache();
					// Ok all good
					return Ok(Value::None);
				}
			}
		}
	}

	// If this is an `out` field then check relation definitions
	if fd.as_str() == "out" {
		// The table is marked as TYPE RELATION
		if let TableType::Relation(ref relation) = tb.table_type {
			// Check if a field TYPE has been specified
			if let Some(kind) = definition.field_kind.as_ref() {
				// The `out` field must be a record type
				let Kind::Record(field_kind) = kind else {
					bail!(ExecError::Thrown("out field on a relation must be a record".into(),))
				};
				// Add the TYPE to the DEFINE TABLE statement
				if *field_kind != relation.to {
					// Refresh the table cache
					tb.table_type = TableType::Relation(Relation {
						to: field_kind.clone(),
						..relation.clone()
					});
					txn.replace_tb(ns_name, db_name, &tb).await?;
					// Clear the cache
					txn.clear_cache();
					// Ok all good
					return Ok(Value::None);
				}
			}
		}
	}

	// The generation identifies the payload shape: the inline field set's
	// names, order, and types. Bump it only when this definition changes
	// that shape — the field joins or leaves the inline set, or an inline
	// field's declared type changes — so readers fall back until a write
	// restamps the payloads. Clauses that never enter a payload
	// (PERMISSIONS, COMMENT, ASSERT, DEFAULT, VALUE) leave written
	// payloads valid: permissions are re-checked at plan time, and the
	// payload stores whatever values the record stores.
	let was_inline = existing.as_ref().is_some_and(|e| e.inline);
	let inline_toggled = definition.inline != was_inline;
	let inline_kind_changed = definition.inline
		&& was_inline
		&& existing.as_ref().is_some_and(|e| e.field_kind != definition.field_kind);
	if inline_toggled || inline_kind_changed {
		tb.graph_inline_gen = tb.graph_inline_gen.wrapping_add(1);
	}

	// The implied subtype fields are written before the table, so the
	// catalog version `replace_tb` stamps last names a state that holds them.
	// An import restores the reference keys of the ones dropped verbatim.
	crate::ddl::define_field_statement_process_recursive_definitions(
		ns,
		db,
		Arc::clone(&txn),
		existing.as_deref(),
		definition,
		!opt.import,
	)
	.await?;

	txn.replace_tb(ns_name, db_name, &tb).await?;

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}

/// Refuse `definition` when its kind is not one each field it is nested in
/// admits at its place
/// ([`parent_kind_restrictions`](crate::exec::field_rules::parent_kind_restrictions)),
/// reading the table's fields from the catalog of namespace `ns` and database
/// `db` through `txn`.
///
/// `DEFINE FIELD` makes this check of every definition, and `ALTER FIELD` of
/// every definition whose `TYPE` it sets. It runs under an import too, so it
/// admits every definition an export writes, the ones a kind implies for its
/// elements among them.
///
/// # Errors
///
/// The refusal of `parent_kind_restrictions`; the error of a catalog read that
/// fails.
pub(crate) async fn define_field_statement_disallow_mismatched_types(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	let fds = txn.all_tb_fields(ns, db, &definition.table, None).await?;
	crate::exec::field_rules::parent_kind_restrictions(&fds, definition)
}

/// Delete the definitions of the fields of table `tb` that `dropped` holds,
/// each under its storage name ([`catalog::FieldDefinition::storage_name`]),
/// and with `purge_references` the reference keys they wrote.
pub(crate) async fn del_tb_field_definitions(
	txn: &Transaction,
	(ns, db): (NamespaceId, DatabaseId),
	tb: &TableName,
	dropped: &[catalog::FieldDefinition],
	purge_references: bool,
) -> Result<()> {
	for fd in dropped {
		txn.del_tb_field(ns, db, tb, &catalog::FieldDefinition::storage_name(&fd.name)).await?;
		if purge_references {
			crate::exec::reference_purge::purge_dropped_reference_keys(txn, ns, db, tb, fd, None)
				.await?;
		}
	}
	Ok(())
}

pub(crate) async fn define_field_statement_validate_flexible_restrictions(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	if !definition.flexible {
		return Ok(());
	}
	let tb = txn.get_tb(ns, db, &definition.table, None).await?;
	crate::exec::field_rules::flexible_restrictions(definition, tb.as_deref())
}

/// Write the definitions `definition`'s kind implies for the elements of an
/// array or set ([`implied_definitions`]), each over the one defined there
/// ([`implied_definition`]), and delete the ones the kind of `previous`, the
/// definition `definition` replaces, implied and that go with it
/// ([`implied_definitions_dropped`]): with `purge_references`, the reference
/// keys a deleted one wrote go too. A kind of `array` with no element kind
/// implies nothing: its elements are already `any`.
///
/// Each implied definition is written as an explicit one is, through
/// [`TableProvider::put_tb_field`]: under its storage name, so a later
/// `DEFINE`, `ALTER` or `REMOVE` of the path finds it, with the table's
/// catalog version stamped, and with the transaction's cached list of the
/// table's fields dropped, so a read of them later in the transaction holds
/// it.
pub(crate) async fn define_field_statement_process_recursive_definitions(
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	previous: Option<&catalog::FieldDefinition>,
	definition: &catalog::FieldDefinition,
	purge_references: bool,
) -> Result<()> {
	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;
	for (name, kind) in implied_definitions(&definition.name, definition.field_kind.as_ref()) {
		let existing = fields.iter().find(|fd| fd.name == name);
		let implied = implied_definition(definition, existing, name, kind);
		txn.put_tb_field(ns, db, &definition.table, &implied).await?;
	}
	let dropped = implied_definitions_dropped(&fields, previous, definition);
	let dropped: Vec<catalog::FieldDefinition> =
		fields.iter().filter(|fd| dropped.contains(&fd.name)).cloned().collect();
	del_tb_field_definitions(&txn, (ns, db), &definition.table, &dropped, purge_references).await
}

pub(crate) async fn define_field_statement_validate_computed_options(
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// Find all existing field definitions
	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;
	if definition.computed.is_some() {
		// Ensure the field is not the `id` field
		ensure!(!definition.name.is_id(), ExecError::IdFieldKeywordConflict("COMPUTED".into()));

		// Ensure the field is top-level
		ensure!(
			definition.name.len() == 1,
			ExecError::ComputedNestedField(definition.name.to_sql())
		);

		// Ensure there are no conflicting clauses
		ensure!(definition.value.is_none(), ExecError::ComputedKeywordConflict("VALUE".into()));
		ensure!(definition.assert.is_none(), ExecError::ComputedKeywordConflict("ASSERT".into()));
		ensure!(
			definition.reference.is_none(),
			ExecError::ComputedKeywordConflict("REFERENCE".into())
		);
		ensure!(
			matches!(definition.default, catalog::DefineDefault::None),
			ExecError::ComputedKeywordConflict("DEFAULT".into())
		);
		ensure!(!definition.readonly, ExecError::ComputedKeywordConflict("READONLY".into()));

		// Ensure no nested fields exist
		for field in fields.iter() {
			if field.name.starts_with(&definition.name) && field.name != definition.name {
				bail!(ExecError::ComputedNestedFieldConflict(
					definition.name.to_sql(),
					field.name.to_sql()
				));
			}
		}
	} else {
		// Ensure no parent fields are computed
		for field in fields.iter() {
			// A derived component is computed from the id; a field under its name
			// is refused as a clash.
			if field.is_key_part() {
				continue;
			}
			if field.computed.is_some()
				&& definition.name.starts_with(&field.name)
				&& field.name != definition.name
			{
				bail!(ExecError::ComputedParentFieldConflict(
					definition.name.to_sql(),
					field.name.to_sql()
				));
			}
		}
	}

	Ok(())
}

/// Validate that defining this field does not create a dependency cycle.
///
/// Builds a dependency graph from the value-producing clauses of all fields on
/// the table plus the field being defined, then runs iterative DFS to detect
/// cycles. `DEFAULT`, `VALUE` and `COMPUTED` take part, because each produces a
/// value the write path may have to produce another field's value from.
/// `ASSERT` does not: it validates a value rather than producing one, so two
/// fields asserting against each other is ordinary.
/// Only same-table dependencies are checked; cycles across tables are not detected.
pub(crate) async fn define_field_statement_validate_computed_cycles(
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// Only relevant for fields that produce a value from other fields
	if !definition.has_production_clause() {
		return Ok(());
	}

	// Fields are named by the key a record holds them under, which is how a
	// clause names a field it reads. A nested field is read by nobody, so no
	// edge leads to it and it cannot be on a cycle.
	let Some(field_name) = definition.name.field_name() else {
		return Ok(());
	};

	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;

	// Build adjacency list: field_name -> the fields its clauses read.
	// Deps are always extracted on the fly (they are not stored).
	// BTreeMap ensures deterministic iteration order for consistent cycle error messages.
	let mut graph: std::collections::BTreeMap<String, Vec<String>> =
		std::collections::BTreeMap::new();

	for fd in fields.iter() {
		// A derived component reads only the id, which no clause of its own
		// can reach: it is a leaf of the graph.
		if fd.is_key_part() {
			continue;
		}
		if !fd.has_production_clause() {
			continue;
		}
		let Some(name) = fd.name.field_name() else {
			continue;
		};
		// Skip the field being defined; its new definition is inserted below.
		if name == field_name {
			continue;
		}
		graph.insert(name.to_owned(), fd.production_dependencies());
	}

	// Insert/replace the field being defined with its freshly-extracted deps
	graph.insert(field_name.to_owned(), definition.production_dependencies());

	// Iterative DFS cycle detection.
	// States: 0 = unvisited, 1 = in current path, 2 = fully visited
	let mut state: std::collections::BTreeMap<&str, u8> = std::collections::BTreeMap::new();
	for key in graph.keys() {
		state.insert(key.as_str(), 0);
	}

	// For each unvisited node, run DFS
	for start in graph.keys() {
		if state.get(start.as_str()) == Some(&2) {
			continue;
		}

		// Stack holds (node, index_into_neighbors)
		let mut stack: Vec<(&str, usize)> = vec![(start.as_str(), 0)];
		// Track the path for error reporting
		let mut path: Vec<&str> = vec![start.as_str()];
		state.insert(start.as_str(), 1);

		while let Some((node, idx)) = stack.last_mut() {
			let neighbors = graph.get(*node).map(|v| v.as_slice()).unwrap_or(&[]);
			if *idx < neighbors.len() {
				let neighbor = neighbors[*idx].as_str();
				*idx += 1;

				// Only check neighbors that have a clause of their own (in the graph)
				if !graph.contains_key(neighbor) {
					continue;
				}

				match state.get(neighbor) {
					Some(1) => {
						// Found a cycle! Build the cycle path for the error message.
						let cycle_start = path.iter().position(|&n| n == neighbor).unwrap_or(0);
						// The names are shown as a statement writes them
						let shown = |name: &str| Idiom::field(name).to_raw_string();
						let cycle: Vec<String> =
							path[cycle_start..].iter().map(|s| shown(s)).collect();
						let cycle_str = format!("{} -> {}", cycle.join(" -> "), shown(neighbor));
						bail!(ExecError::ComputedFieldCycle(cycle_str));
					}
					Some(0) | None => {
						// Unvisited: push onto stack
						state.insert(neighbor, 1);
						path.push(neighbor);
						stack.push((neighbor, 0));
					}
					_ => {
						// Already fully visited (state 2), skip
					}
				}
			} else {
				// Done with this node's neighbors
				state.insert(node, 2);
				path.pop();
				stack.pop();
			}
		}
	}

	Ok(())
}

/// Whether `definition` over `existing` can change the value the field derives
/// for a record the table already holds: the field becomes `COMPUTED`, stops
/// being `COMPUTED` (a removed field is replaced by a definition that is not),
/// has its body changed, or is `COMPUTED` under another kind its derived value
/// is coerced to.
///
/// A stored field keeps the value it was written with, so no clause of it
/// changes what a record holds.
pub(crate) fn derived_value_changes(
	existing: Option<&catalog::FieldDefinition>,
	definition: &catalog::FieldDefinition,
) -> bool {
	if existing.and_then(|existing| existing.computed.as_ref()) != definition.computed.as_ref() {
		return true;
	}
	definition.computed.is_some()
		&& existing.is_some_and(|existing| existing.field_kind != definition.field_kind)
}

/// Refuse `definition` when it changes the value a field derives
/// ([`derived_value_changes`]) that an aggregated view of the table depends on,
/// and the table holds a record.
///
/// An aggregated view keys the row of a group by the values its clauses derived
/// when the records of the group were written, and an update or delete of a
/// record leaves the group its stored state derives to under the definitions in
/// force then. A changed derivation sends the record to a group the view holds
/// no row for, or takes a contribution out of stats that never held it, and
/// neither can be repaired from the view's rows. Rebuilding the view is a scan
/// of the table in the defining transaction, which `DEFINE FIELD` does not
/// make: the view is removed and defined again around the change. A table that
/// holds no record has no group to disagree with.
///
/// A view depends on the field when a clause of it (its fields, condition or
/// groups) spells the field's name or the name of a `COMPUTED` field that reads
/// it, directly or through others, or reads the record by names it computes
/// ([`view_reads`]). A `COMPUTED` field that reads the record by names it
/// computes may read the field, so a view that spells it is refused as one that
/// reads by names it computes is. A materialized view keys its row by the
/// record's own key and reads no earlier image, so it never depends on the
/// field.
///
/// The records are those the defining transaction's snapshot holds
/// ([`holds_rows`]).
///
/// # Errors
///
/// [`ExecError::ComputedFieldReadByView`] or
/// [`ExecError::ComputedFieldReadByViewUnresolved`]; the error of a catalog or
/// storage read that fails.
pub(crate) async fn define_field_statement_validate_computed_views(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: Option<&catalog::FieldDefinition>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	if !derived_value_changes(existing, definition) {
		return Ok(());
	}
	// A `COMPUTED` field is top-level, and so is the field it replaces.
	let Some(field) = definition.name.field_name() else {
		return Ok(());
	};
	let table = &definition.table;
	let mut views = Vec::new();
	for view in txn.all_tb_views(ns, db, table, None).await?.iter() {
		if let Some(
			aggregated @ ViewDefinition::Aggregated {
				..
			},
		) = &view.view
		{
			views.push((view.name.clone(), view_reads(aggregated)));
		}
	}
	if views.is_empty() {
		return Ok(());
	}
	let fields = txn.all_tb_fields(ns, db, table, None).await?;
	let computed: Vec<(&str, RecordReads)> = fields
		.iter()
		.filter(|fd| !fd.is_key_part())
		.filter_map(|fd| {
			let body = fd.computed.as_ref()?;
			Some((fd.name.field_name()?, record_reads(body, ReadContext::Record)))
		})
		.collect();
	// A view that spells a name that moves is reported before one that may depend
	// on the field only through a read by names that cannot be determined: its
	// own, or that of a `COMPUTED` field it spells.
	let moved = moved_with(field, &computed, false);
	let reader = views
		.iter()
		.find(|(_, reads)| first_of(&moved, &reads.names).is_some())
		.map(|(view, _)| (view, true))
		.or_else(|| {
			let moved = moved_with(field, &computed, true);
			views
				.iter()
				.find(|(_, reads)| reads.unresolved || first_of(&moved, &reads.names).is_some())
				.map(|(view, _)| (view, false))
		});
	let Some((view, spelled)) = reader else {
		return Ok(());
	};
	if !holds_rows(txn, ns, db, table).await? {
		return Ok(());
	}
	let (field, table, view) =
		(field.to_owned(), table.as_str().to_owned(), view.as_str().to_owned());
	if spelled {
		bail!(ExecError::ComputedFieldReadByView {
			field,
			table,
			view,
		})
	}
	bail!(ExecError::ComputedFieldReadByViewUnresolved {
		field,
		table,
		view,
	})
}

/// The names whose derived value moves with the field `field`: its own, and
/// that of each `COMPUTED` field of `computed` that reads one of them, directly
/// or through others. With `opaque`, a `COMPUTED` field that may read fields of
/// the record by names it does not spell ([`RecordReads::unresolved`]) moves
/// as well, since the field may be one it reads.
fn moved_with(field: &str, computed: &[(&str, RecordReads)], opaque: bool) -> Vec<String> {
	let mut moved = vec![field.to_owned()];
	loop {
		let before = moved.len();
		for (name, reads) in computed {
			if !moved.iter().any(|moved| moved == *name)
				&& ((opaque && reads.unresolved)
					|| reads.names.iter().any(|read| moved.contains(read)))
			{
				moved.push((*name).to_owned());
			}
		}
		if moved.len() == before {
			return moved;
		}
	}
}

/// Whether the named table already exists as a relation — the only shape
/// whose records have adjacency payloads for an `INLINE` field to ride.
async fn existing_table_is_relation(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> Result<bool> {
	Ok(txn
		.get_tb(ns, db, table, None)
		.await?
		.is_some_and(|tb| matches!(tb.table_type, TableType::Relation(_))))
}

/// Refuse a computed `definition` that an index of its table covers, as
/// `DEFINE FIELD` does while it forms the definition, reading the catalog
/// through `txn`.
pub(crate) async fn define_field_definition_check_indexed_computed(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	if definition.computed.is_some() {
		for ix in txn.all_tb_indexes(ns, db, &definition.table, None).await?.iter() {
			if ix.cols.iter().any(|col| col.starts_with(&definition.name)) {
				bail!(ExecError::ComputedFieldCannotBeIndexed {
					index: ix.name.to_string(),
					field: definition.name.to_raw_string(),
				});
			}
		}
	}
	Ok(())
}

/// The checks `DEFINE FIELD` and `ALTER FIELD` make of the record id components
/// `definition` names, or of a field `definition` defines under the name of one,
/// against the catalog of namespace `ns` and database `db`, read through `txn`.
///
/// For the `id` field naming components, in the order it makes them:
/// - the table is not a relation or a view, whose record ids have no components to name;
/// - no field of the table has a name a component takes;
/// - no index or view reads, as a stored field, a name the record id adds;
/// - nothing on the table reads a name the record id removes, renames or moves. Retyping a
///   component under the same name and position is allowed: the scan of the stored records proves
///   they already conform.
/// - the definition's own `ASSERT`, `DEFAULT` and permissions read no name the record id removes or
///   renames. A name it moves still resolves, by name, in the definition that moves it.
///
/// For any other field, the name must not be the root of a component's name.
///
/// `kind` decides what an existing `id` does: `IF NOT EXISTS` over it leaves it
/// alone, so nothing is checked. A table with no named `id` and a definition
/// that names none checks nothing.
///
/// # Errors
///
/// The refusal naming the conflict, as an [`ExecError`]; the error of a catalog
/// read that fails.
pub(crate) async fn define_field_statement_validate_key_components(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &DefineKind,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	use crate::ddl::key_scan::{self, Readers};

	let table = &definition.table;
	let existing_id = txn.get_tb_field(ns, db, table, "id", None).await?;
	let old_key = existing_id.as_deref().and_then(catalog::FieldDefinition::live_key);
	if !definition.name.is_id() {
		if let (Some(root), Some(key)) = (key_scan::root_name(&definition.name), old_key) {
			ensure!(
				key.names.as_ref().is_none_or(|names| names.position(root).is_none()),
				ExecError::IdComponentClash {
					name: root.to_owned(),
					table: table.as_str().to_owned(),
				}
			);
		}
		return Ok(());
	}
	if matches!(kind, DefineKind::IfNotExists) && existing_id.is_some() {
		return Ok(());
	}
	let new_key = definition.live_key();
	if let Some(names) = new_key.and_then(|key| key.names.as_ref()) {
		if let Some(tb) = txn.get_tb(ns, db, table, None).await? {
			ensure!(
				!matches!(tb.table_type, TableType::Relation(_)),
				ExecError::IdComponentsOnRelation(table.as_str().to_owned())
			);
			ensure!(tb.view.is_none(), ExecError::IdComponentsOnView(table.as_str().to_owned()));
		}
		let fields = txn.all_tb_fields(ns, db, table, None).await?;
		for name in names.iter() {
			let shadowed = fields.iter().any(|field| {
				!field.is_key_part()
					&& !field.name.is_id()
					&& key_scan::root_name(&field.name) == Some(name)
			});
			ensure!(
				!shadowed,
				ExecError::IdComponentShadowsField {
					name: name.to_owned(),
					table: table.as_str().to_owned(),
				}
			);
		}
	}
	let change =
		key_scan::name_change(old_key.map(|key| key.as_ref()), new_key.map(|key| key.as_ref()));
	if !change.added.is_empty()
		&& let Some(reader) =
			key_scan::first_reader(txn, ns, db, table, &change.added, Readers::Structural).await?
	{
		bail!(key_scan::read_by_existing(table, reader));
	}
	if !change.removed.is_empty()
		&& let Some(reader) =
			key_scan::first_reader(txn, ns, db, table, &change.removed, Readers::All).await?
	{
		bail!(key_scan::in_use(table, reader));
	}
	let kept = |name: &str| {
		new_key
			.and_then(|key| key.names.as_ref())
			.is_some_and(|names| names.position(name).is_some())
	};
	let dropped: Vec<String> = change.removed.into_iter().filter(|name| !kept(name)).collect();
	if let Some(reader) = key_scan::id_clause_reader(definition, &dropped) {
		bail!(key_scan::in_use(table, reader));
	}
	Ok(())
}

/// The checks `DEFINE FIELD` makes of `definition` against the catalog of
/// namespace `ns` and database `db`, read through `txn`, before it judges the
/// record id components the definition names: the GraphQL alias, the computed
/// options and dependency cycles, the reference options, the kinds of the
/// fields the definition nests in or over, and the `id` field's restrictions.
/// The components are judged next
/// ([`define_field_statement_validate_key_components`]), then `FLEXIBLE`'s
/// restrictions ([`define_field_statement_validate_flexible_restrictions`]).
pub(crate) async fn define_field_definition_validate_clauses(
	txn: &Arc<Transaction>,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	crate::ddl::define::validate_graphql_alias(&definition.graphql_alias, "field")?;
	define_field_statement_validate_computed_options(ns, db, Arc::clone(txn), definition).await?;
	define_field_statement_validate_computed_cycles(ns, db, Arc::clone(txn), definition).await?;
	define_field_statement_validate_reference_options(definition)?;
	define_field_statement_disallow_mismatched_types(txn, ns, db, definition).await?;
	crate::exec::field_rules::id_field_restrictions(definition)?;
	Ok(())
}

#[cfg(all(test, feature = "kv-mem"))]
mod tests {
	use std::str::FromStr;

	use super::*;
	use crate::catalog::providers::DatabaseProvider as _;
	use crate::dbs::Session;
	use crate::expr::Idiom;
	use crate::kvs::{Datastore, TransactionType};

	/// The definitions a kind implies are in the table's field list for the
	/// rest of the transaction that writes them, with no clearing of the
	/// transaction cache: a statement that fails after writing them, in a
	/// transaction that goes on, leaves the next statement a list that holds
	/// them.
	#[tokio::test]
	async fn implied_definitions_are_listed_in_the_transaction_that_writes_them() {
		let ds = Datastore::new("memory").await.unwrap();
		let session = Session::owner().with_ns("test").with_db("test");
		let sql = "DEFINE NAMESPACE test; USE NS test; DEFINE DATABASE test; USE DB test; \
		           DEFINE TABLE t SCHEMAFULL;";
		for response in ds.execute(sql, &session, None).await.unwrap() {
			response.result.unwrap();
		}
		let txn = Arc::new(ds.transaction(TransactionType::Write).await.unwrap());
		let database = txn.expect_db_by_name("test", "test").await.unwrap();
		let (ns, db) = (database.namespace_id, database.database_id);
		let definition = catalog::FieldDefinition {
			name: Idiom::from_str("`select`").unwrap(),
			table: TableName::from("t"),
			field_kind: Some(Kind::Array(Box::new(Kind::String), None)),
			..Default::default()
		};

		define_field_statement_process_recursive_definitions(
			ns,
			db,
			Arc::clone(&txn),
			None,
			&definition,
			true,
		)
		.await
		.unwrap();

		let listed = txn.all_tb_fields(ns, db, &definition.table, None).await.unwrap();
		let names: Vec<String> = listed.iter().map(|fd| fd.name.to_raw_string()).collect();
		txn.cancel().await.unwrap();
		assert_eq!(names, vec!["select.*"]);
	}
}
