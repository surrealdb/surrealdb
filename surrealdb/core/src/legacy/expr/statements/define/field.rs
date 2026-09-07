use std::borrow::Cow;
use std::sync::Arc;

use anyhow::{Result, bail, ensure};
use reblessive::tree::Stk;
use surrealdb_types::ToSql;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, DatabaseId, Error as CatalogError, NamespaceId, Relation, TableType};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::exec::Error as ExecError;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::field::{
	DefineDefault, DefineFieldStatement, kind_contains_object,
};
use crate::expr::{Base, Idiom, Kind, KindLiteral, Part, RecordIdKeyLit};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::KVKeyDecode;
use crate::key::schema::{FieldKey, RefCachePrefix, ReferenceKey, ReferencePrefix};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE, Transaction};
use crate::legacy::{expr_to_ident, expr_to_idiom};
use crate::val::{TableName, Value};

pub(crate) async fn define_field_statement_to_definition(
	this: &DefineFieldStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<catalog::FieldDefinition> {
	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	let name: Idiom = expr_to_idiom(stk, ctx, opt, doc, &this.name, "field name").await?;
	let table: TableName =
		expr_to_ident(stk, ctx, opt, doc, &this.what, "table name").await?.into();
	// Computed fields cannot be indexed. Check if any existing index references
	// this field (or has it as a prefix for sub-field paths).
	if this.computed.is_some() {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		for ix in ctx.tx().all_tb_indexes(ns, db, &table, None).await?.iter() {
			if ix.cols.iter().any(|col| col.starts_with(&name)) {
				bail!(ExecError::ComputedFieldCannotBeIndexed {
					index: ix.name.to_string(),
					field: name.to_raw_string(),
				})
			}
		}
	}

	Ok(catalog::FieldDefinition {
		name,
		table,
		field_kind: this.field_kind.clone(),
		flexible: this.flexible,
		readonly: this.readonly,
		inline: this.inline,
		value: this.value.clone(),
		assert: this.assert.clone(),
		computed: this.computed.clone(),
		default: match &this.default {
			DefineDefault::None => catalog::DefineDefault::None,
			DefineDefault::Set(x) => catalog::DefineDefault::Set(x.clone()),
			DefineDefault::Always(x) => catalog::DefineDefault::Always(x.clone()),
		},
		select_permission: this.permissions.select.clone(),
		create_permission: this.permissions.create.clone(),
		update_permission: this.permissions.update.clone(),
		comment,
		reference: this.reference.clone(),
		auth_limit: AuthLimit::new_from_auth(opt.auth.as_ref()).into(),
		graphql_alias: this.graphql_alias.clone(),
		graphql_deprecated: this.graphql_deprecated.clone(),
	})
}

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineFieldStatement::compute", skip_all)]
pub(crate) async fn define_field_statement_compute(
	this: &DefineFieldStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	let definition =
		crate::legacy::define_field_statement_to_definition(this, stk, ctx, opt, doc).await?;

	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Field, Base::Db)?;

	// Validate any GRAPHQL_ALIAS at definition time so typos surface here
	// rather than silently falling back at schema-generation time.
	crate::legacy::expr::statements::define::validate_graphql_alias(&this.graphql_alias, "field")?;

	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;

	// Validate computed options
	crate::legacy::define_field_statement_validate_computed_options(
		this,
		ns,
		db,
		ctx.tx(),
		&definition,
	)
	.await?;

	// Validate computed field dependencies for cycles
	crate::legacy::define_field_statement_validate_computed_cycles(
		this,
		ns,
		db,
		ctx.tx(),
		&definition,
	)
	.await?;

	// Validate reference options
	crate::legacy::define_field_statement_validate_reference_options(this, &definition)?;

	// Disallow mismatched types
	crate::legacy::define_field_statement_disallow_mismatched_types(this, ctx, ns, db, &definition)
		.await?;

	// Validate id field restrictions
	validate_id_field_restrictions(&definition)?;

	// Validate FLEXIBLE restrictions
	crate::legacy::define_field_statement_validate_flexible_restrictions(
		this,
		ctx,
		ns,
		db,
		&definition,
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
		match this.kind {
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

	// Process the statement
	txn.put_tb_field(ns, db, &tb_name, &definition).await?;

	// Overwriting an existing reference field can drop target tables it used
	// to reference (the REFERENCE clause removed, or the record kind narrowed
	// or changed); purge the now-stranded reference keys so the DELETE
	// reference-purge gate stays sound. Skipped during import, which restores
	// reference keys verbatim.
	if !opt.import
		&& let Some(existing) = &existing
	{
		purge_dropped_reference_keys(&txn, ns, db, &tb_name, existing, Some(&definition)).await?;
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
			if let Some(kind) = this.field_kind.as_ref() {
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
			if let Some(kind) = this.field_kind.as_ref() {
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

	txn.replace_tb(ns_name, db_name, &tb).await?;

	// Process possible recursive defitions
	crate::legacy::define_field_statement_process_recursive_definitions(
		this,
		ns,
		db,
		Arc::clone(&txn),
		&definition,
	)
	.await?;

	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}

pub(crate) async fn define_field_statement_disallow_mismatched_types(
	this: &DefineFieldStatement,
	ctx: &FrozenContext,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	let fds = ctx.tx().all_tb_fields(ns, db, &definition.table, None).await?;

	if let Some(self_kind) = &this.field_kind {
		for fd in fds.iter() {
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
	this: &DefineFieldStatement,
	ctx: &FrozenContext,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	if this.flexible {
		ensure!(
			this.field_kind.as_ref().is_some_and(kind_contains_object),
			ExecError::Thrown("FLEXIBLE can only be used with types containing object".into())
		);

		// Get the table definition
		let txn = ctx.tx();
		let Some(tb) = txn.get_tb(ns, db, &definition.table, None).await? else {
			bail!(CatalogError::TbNotFound {
				name: definition.table.clone(),
			});
		};

		// FLEXIBLE can only be used in SCHEMAFULL tables
		ensure!(
			tb.schemafull,
			ExecError::Thrown("FLEXIBLE can only be used in SCHEMAFULL tables".into())
		);
	}

	Ok(())
}

pub(crate) async fn define_field_statement_process_recursive_definitions(
	this: &DefineFieldStatement,
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// Find all existing field definitions
	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await.ok();
	// Process possible recursive_definitions
	if let Some(mut cur_kind) = this.field_kind.as_ref().and_then(|x| x.inner_kind()) {
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
	this: &DefineFieldStatement,
	ns: NamespaceId,
	db: DatabaseId,
	txn: Arc<Transaction>,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// Find all existing field definitions
	let fields = txn.all_tb_fields(ns, db, &definition.table, None).await?;
	if let Some(computed) = this.computed.as_ref() {
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
		ensure!(this.value.is_none(), ExecError::ComputedKeywordConflict("VALUE".into()));
		ensure!(this.assert.is_none(), ExecError::ComputedKeywordConflict("ASSERT".into()));
		ensure!(this.reference.is_none(), ExecError::ComputedKeywordConflict("REFERENCE".into()));
		ensure!(
			matches!(this.default, DefineDefault::None),
			ExecError::ComputedKeywordConflict("DEFAULT".into())
		);
		ensure!(!this.readonly, ExecError::ComputedKeywordConflict("READONLY".into()));

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
	_this: &DefineFieldStatement,
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
	this: &DefineFieldStatement,
	definition: &catalog::FieldDefinition,
) -> Result<()> {
	// If a reference is defined, the field must be a record
	if this.reference.is_some() {
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

		let is_record_id = match this.field_kind.as_ref() {
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
				this.field_kind.as_ref().unwrap_or(&Kind::Any).to_sql()
			)
		);
	}

	Ok(())
}

/// Purge the reference keys a field wrote under target tables it no longer
/// references after a schema change (`REMOVE FIELD`, `ALTER FIELD`, or
/// `DEFINE FIELD ... OVERWRITE`).
///
/// Reference keys are stored under the *referenced* (target) record's range,
/// keyed by the referencing `(table, field)` rather than under the field's own
/// definition (see `Document::process_reference_clause`). So when a field's
/// `REFERENCE` clause is dropped, or its record kind is narrowed or changed,
/// the keys it wrote for the now-unreachable target tables are not removed by
/// rewriting the field definition. Left behind they are orphaned: a later
/// `DELETE` of a referenced record skips the purge scan (because
/// `table_may_have_incoming_references` reports that no current field can
/// target that table), so on record-id reuse or re-definition the stale key
/// could resurface in a `<~` reference lookup or drive the wrong `ON DELETE`
/// action. Cleaning them at the schema change keeps that DELETE purge gate
/// sound: "no current reference field can target this table" then truly
/// implies "no reference keys exist for it".
///
/// `old` is the field definition before the change; `new` is the definition
/// after it (`None` when the field is being removed). Only target tables that
/// `old` could reference but `new` cannot are scanned. This runs on rare DDL,
/// so scanning the candidate target ranges is acceptable and avoids
/// maintaining a reverse index.
/// Enforce the clauses that are not permitted on the `id` field. Shared by
/// DEFINE FIELD and ALTER FIELD so both reject the same set on `id`: `VALUE`,
/// `REFERENCE`, `COMPUTED`, `DEFAULT ALWAYS`, `READONLY`, `FLEXIBLE`, and any
/// `TYPE` that is not a valid record-id key kind.
///
/// A plain `DEFAULT` and an `ASSERT` are allowed: both are applied to the
/// record-id key at key-generation time in `Document::generate_record_id`
/// (the `ASSERT` binds the key to `$key`). Operates on the catalog definition
/// so DEFINE and ALTER validate the same resolved state.
pub(crate) fn validate_id_field_restrictions(def: &catalog::FieldDefinition) -> Result<()> {
	if !def.name.is_id() {
		return Ok(());
	}
	// `VALUE`, `REFERENCE`, and `COMPUTED` are meaningless or unsafe on an
	// immutable primary key.
	ensure!(def.value.is_none(), ExecError::IdFieldKeywordConflict("VALUE".into()));
	ensure!(def.reference.is_none(), ExecError::IdFieldKeywordConflict("REFERENCE".into()));
	ensure!(def.computed.is_none(), ExecError::IdFieldKeywordConflict("COMPUTED".into()));
	// A plain `DEFAULT` supplies the id when none is given; `DEFAULT ALWAYS`
	// would recompute it on every update, which is nonsensical for an
	// immutable id.
	ensure!(
		!matches!(def.default, catalog::DefineDefault::Always(_)),
		ExecError::IdFieldKeywordConflict("DEFAULT ALWAYS".into())
	);
	// The id is implicitly immutable (`READONLY` is redundant) and a record-id
	// key is not an object (`FLEXIBLE` is meaningless).
	ensure!(!def.readonly, ExecError::IdFieldKeywordConflict("READONLY".into()));
	ensure!(!def.flexible, ExecError::IdFieldKeywordConflict("FLEXIBLE".into()));
	// The declared `TYPE` must be representable as a record-id key.
	if let Some(kind) = &def.field_kind {
		ensure!(
			RecordIdKeyLit::kind_supported(kind),
			ExecError::IdFieldUnsupportedKind(kind.to_sql())
		);
	}
	Ok(())
}

pub(crate) async fn purge_dropped_reference_keys(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	ft: &TableName,
	old: &catalog::FieldDefinition,
	new: Option<&catalog::FieldDefinition>,
) -> Result<()> {
	// Only a field that previously declared a REFERENCE wrote reference keys.
	if old.reference.is_none() {
		return Ok(());
	}
	// `ff` is the referencing field name exactly as `process_reference_clause`
	// encoded it into each key's `ff` slot.
	let ff = old.name.to_sql();
	let old_kind: Option<&Kind> = old.field_kind.as_ref();
	let new_kind: Option<&Kind> = new.and_then(|n| n.field_kind.as_ref());
	for target in txn.all_tb(ns, db, None).await?.iter() {
		let target = target.name.clone();
		let target = &target;
		// Reference keys live under their target table, so only tables the old
		// kind could hold a record of can carry this field's keys (an untyped
		// `record` could target any table).
		let old_can_target = old_kind.is_none_or(|k| k.reference_can_target(target));
		if !old_can_target {
			continue;
		}
		// Keep the keys the new definition still references.
		let new_can_target = new.is_some_and(|n| {
			n.reference.is_some() && new_kind.is_none_or(|k| k.reference_can_target(target))
		});
		if new_can_target {
			continue;
		}
		// Collect the matching keys first, then delete them, so the range is
		// never mutated while the cursor is still scanning it.
		let range = ReferencePrefix {
			ns,
			db,
			tb: Cow::Borrowed(target),
		}
		.range()?;
		let mut orphaned: Vec<Vec<u8>> = Vec::new();
		let mut cursor = txn.open_keys_cursor(range, Direction::Forward, 0, None).await?;
		loop {
			let batch = cursor.next_batch(NORMAL_BATCH_SIZE).await?;
			if batch.is_empty() {
				break;
			}
			for raw in batch.iter() {
				let key = ReferenceKey::decode_key(raw)?;
				if key.foreign_table.as_ref() == ft && key.foreign_field.as_ref() == ff.as_str() {
					orphaned.push(raw.to_vec());
				}
			}
		}
		drop(cursor);
		for raw in &orphaned {
			let key = ReferenceKey::decode_key(raw)?;
			txn.del_key(&key).await?;
		}
		// The deletions above happen behind the target table's inline
		// reference caches, so those caches — if the table maintains any —
		// are now stale. Drop the table's whole cache subspace in this
		// transaction; caches re-materialise lazily. Invalidate-to-missing
		// here is one range delete, where cache-accurate maintenance would
		// be a read-modify-write per orphaned key.
		if !orphaned.is_empty() {
			let key = RefCachePrefix {
				ns,
				db,
				tb: Cow::Borrowed(target),
			};
			txn.del_prefix_key(&key).await?;
		}
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
