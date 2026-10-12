use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, DatabaseId, Error as CatalogError, NamespaceId, Relation, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exec::Error as ExecError;
use crate::expr::statements::define::DefineKind;
use crate::expr::{Base, Kind, KindLiteral, Part};
use crate::iam::{Action, ResourceKind};
use crate::key::schema::FieldKey;
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

	// Validate computed field dependencies for cycles
	crate::ddl::define_field_statement_validate_computed_cycles(ns, db, ctx.tx(), definition)
		.await?;

	// Validate reference options
	crate::ddl::define_field_statement_validate_reference_options(definition)?;

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
	// it only means something on stored edges. Skipped during import: an
	// export of a since-demoted table must still replay.
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
	let fd = definition.name.to_raw_string();
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

	// A record id that names its components must admit every record the table
	// holds; the scan is skipped when the stored `id` already has the same
	// structure.
	if let Some(key) = definition.live_key() {
		let exec = crate::ddl::frame(ctx, opt, None).await;
		crate::ddl::key_scan::validate_record_keys(&exec, &txn, ns, db, &tb_name, key).await?;
	}

	// Process the statement
	txn.put_tb_field(ns, db, &tb_name, definition).await?;

	// Overwriting an existing reference field can drop target tables it used
	// to reference (the REFERENCE clause removed, or the record kind narrowed
	// or changed); purge the now-stranded reference keys so the DELETE
	// reference-purge gate stays sound. Skipped during import, which restores
	// reference keys verbatim.
	if !opt.import
		&& let Some(existing) = &existing
	{
		crate::exec::reference_purge::purge_dropped_reference_keys(
			&txn,
			ns,
			db,
			&tb_name,
			existing,
			Some(definition),
		)
		.await?;
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
	crate::ddl::define_field_statement_process_recursive_definitions(
		ns,
		db,
		Arc::clone(&txn),
		definition,
	)
	.await?;

	txn.replace_tb(ns_name, db_name, &tb).await?;

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}

pub(crate) async fn define_field_statement_disallow_mismatched_types(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	let fds = txn.all_tb_fields(ns, db, &definition.table, None).await?;

	if let Some(self_kind) = &definition.field_kind {
		for fd in fds.iter() {
			// A derived component has no type of its own to nest under: a field
			// under its name is refused as a clash.
			if fd.is_key_part() {
				continue;
			}
			if definition.name.starts_with(&fd.name)
				&& definition.name != fd.name
				&& let Some(fd_kind) = &fd.field_kind
			{
				let path = definition.name[fd.name.len()..].to_vec();
				if !fd_kind.allows_nested_kind(&path, self_kind) {
					bail!(ExecError::MismatchedFieldTypes {
						name: definition.name.to_sql(),
						kind: self_kind.to_sql(),
						existing_name: fd.name.to_sql(),
						existing_kind: fd_kind.to_sql(),
					});
				}
			}
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

pub(crate) async fn define_field_statement_process_recursive_definitions(
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// Find all existing field definitions
	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await.ok();
	// Process possible recursive_definitions
	if let Some(mut cur_kind) = definition.field_kind.as_ref().and_then(|x| x.inner_kind()) {
		let mut name = definition.name.clone();
		loop {
			// Check if the subtype is an `any` type
			if let Kind::Any = cur_kind {
				// There is no need to add a subtype
				// field definition if the type is
				// just specified as an `array`. This
				// is because the following query:
				//  DEFINE FIELD foo ON bar TYPE array;
				// already implies that the immediate
				// subtype is an any:
				//  DEFINE FIELD foo[*] ON bar TYPE any;
				// so we skip the subtype field.
				break;
			}
			// Get the kind of this sub field
			let new_kind = cur_kind.inner_kind();
			// Add a new subtype
			name.0.push(Part::All);
			// Get the field name
			let fd = name.to_sql();
			// Set the subtype `DEFINE FIELD` definition
			let key = FieldKey {
				ns,
				db,
				tb: Cow::Borrowed(&definition.table),
				fd: Cow::Borrowed(&fd),
			};
			let val = if let Some(existing) =
				fields.as_ref().and_then(|x| x.iter().find(|x| x.name == name))
			{
				catalog::FieldDefinition {
					field_kind: Some(cur_kind.clone()),
					flexible: existing.flexible || definition.flexible,
					..existing.clone()
				}
			} else {
				catalog::FieldDefinition {
					name: name.clone(),
					table: definition.table.clone(),
					field_kind: Some(cur_kind.clone()),
					flexible: definition.flexible,
					..Default::default()
				}
			};
			txn.set_key(&key, &val.to_stored()).await?;
			// Process to any sub field
			if let Some(new_kind) = new_kind {
				cur_kind = new_kind;
			} else {
				break;
			}
		}
	}

	Ok(())
}

pub(crate) async fn define_field_statement_validate_computed_options(
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// Find all existing field definitions
	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;
	if let Some(computed) = definition.computed.as_ref() {
		// A COMPUTED body is evaluated on every read of the field, under a
		// frame that refuses writes at runtime. A write written directly into
		// the body can therefore never succeed; it only turns every later read
		// of the field into an error. Reject it here so the failure lands on
		// the definition instead.
		//
		// `contains_mutation` walks this body alone — subqueries, idiom parts,
		// blocks, closure bodies and call arguments. A call to a user-defined
		// function is opaque to it: the callee's body lives in the catalog and
		// may take the writing branch only on inputs this field never supplies.
		// Those are left to the runtime frame, which refuses the write at the
		// point it is actually reached.
		ensure!(!computed.contains_mutation(), ExecError::ComputedWrite(definition.name.to_sql()));

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
/// Only checks same-table dependencies (cross-table cycles are future work).
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

	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;
	let field_name = definition.name.to_raw_string();

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
		let name = fd.name.to_raw_string();
		// Skip the field being (re)defined -- we'll use the new definition below
		if name == field_name {
			continue;
		}
		graph.insert(name, fd.production_dependencies());
	}

	// Insert/replace the field being defined with its freshly-extracted deps
	graph.insert(field_name, definition.production_dependencies());

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
						let cycle: Vec<String> =
							path[cycle_start..].iter().map(|s| (*s).to_string()).collect();
						let cycle_str = format!("{} -> {}", cycle.join(" -> "), neighbor);
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

pub(crate) fn define_field_statement_validate_reference_options(
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// If a reference is defined, the field must be a record
	if definition.reference.is_some() {
		ensure!(
			definition.name.len() == 1,
			ExecError::ReferenceNestedField(definition.name.to_sql())
		);

		fn valid(kind: &Kind, outer: bool) -> bool {
			match kind {
				Kind::None | Kind::Record(_) => true,
				Kind::Array(kind, _) | Kind::Set(kind, _) => outer && valid(kind, false),
				Kind::Literal(KindLiteral::Array(kinds)) => {
					outer && kinds.iter().all(|k| valid(k, false))
				}
				_ => false,
			}
		}

		let is_record_id = match definition.field_kind.as_ref() {
			Some(Kind::Either(kinds)) => kinds.iter().all(|k| valid(k, true)),
			Some(Kind::Array(kind, _)) | Some(Kind::Set(kind, _)) => match kind.as_ref() {
				Kind::Either(kinds) => kinds.iter().all(|k| valid(k, true)),
				Kind::Record(_) => true,
				_ => false,
			},
			Some(Kind::Literal(KindLiteral::Array(kinds))) => kinds.iter().all(|k| valid(k, true)),
			Some(Kind::Record(_)) => true,
			_ => false,
		};

		ensure!(
			is_record_id,
			ExecError::ReferenceTypeConflict(
				definition.field_kind.as_ref().unwrap_or(&Kind::Any).to_sql()
			)
		);
	}

	Ok(())
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
