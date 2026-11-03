use std::ops::Deref;
use std::sync::Arc;

use anyhow::{Result, ensure};
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Error as CatalogError};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::exec::Error as ExecError;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::field::{AlterDefault, AlterFieldStatement};
use crate::expr::statements::define::DefineKind;
use crate::iam::AuthLimit;
use crate::val::{TableName, Value};

/// The namespace and database an `ALTER FIELD` alters in, by name and by id.
struct Scope<'a> {
	ns_name: &'a str,
	db_name: &'a str,
	ns: catalog::NamespaceId,
	db: catalog::DatabaseId,
}

/// Reads the definition of the field `idiom` on table `what` that an `ALTER FIELD` alters, with
/// the scope it is altered in. Returns `None` when `if_exists` is set and it is not defined.
///
/// Refuses when `idiom` names a component of a record id, which is a derived field with no
/// definition to alter, and when `what` is a lightweight relation, whose fields are managed
/// by its table definition.
async fn alter_field_read<'a>(
	idiom: &crate::expr::Idiom,
	what: &TableName,
	if_exists: bool,
	ctx: &FrozenContext,
	opt: &'a Options,
) -> Result<Option<(catalog::FieldDefinition, Scope<'a>)>> {
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let scope = Scope {
		ns_name,
		db_name,
		ns,
		db,
	};
	// Fetch the transaction
	let txn = ctx.tx();
	let name = catalog::FieldDefinition::storage_name(idiom);
	// A component of a named record id is a derived field: it has no definition
	// to alter.
	crate::ddl::key_scan::refuse_component_name(&txn, ns, db, what, idiom).await?;
	// Get the table definition
	let df = match txn.get_tb_field(ns, db, what, &name, None).await? {
		Some(tb) => tb.deref().clone(),
		None => {
			if if_exists {
				return Ok(None);
			}

			return Err(CatalogError::FdNotFound {
				name,
			}
			.into());
		}
	};

	// A lightweight relation's only fields are the in/out pair. Their types are the relation's
	// IN/OUT contract, which the record-less scan and the endpoint checks rely on. Only the
	// table definition may change them.
	if let Some(tb) = txn.get_tb(ns, db, what, None).await? {
		ensure!(
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_none(),
			ExecError::Thrown(
				"a LIGHTWEIGHT relation's fields cannot be altered: its in/out contract is \
				 managed by the table definition"
					.to_owned(),
			)
		);
	}
	Ok(Some((df, scope)))
}

/// Applies the clauses of `this` to the field `idiom` of table `what`, as `ALTER FIELD` does.
pub(crate) async fn alter_field_statement_apply(
	this: &AlterFieldStatement,
	idiom: crate::expr::Idiom,
	what: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let Some((mut df, scope)) = alter_field_read(&idiom, &what, this.if_exists, ctx, opt).await?
	else {
		return Ok(Value::None);
	};
	// Keep a copy of the definition from before the changes. It is compared with the altered
	// definition to find the reference target tables that the change drops.
	let old_definition = df.clone();

	// The type and the names of the record id components it carries are set
	// together, so the definition's key structure always matches its type.
	match this.kind {
		AlterKind::Set(ref k) => {
			let names = match this.key_names {
				AlterKind::Set(ref names) => Some(names.clone()),
				AlterKind::Drop | AlterKind::None => None,
			};
			df.set_type(Some(k.clone()), names);
		}
		AlterKind::Drop => df.set_type(None, None),
		AlterKind::None => {}
	}
	match this.flexible {
		AlterKind::Set(_) => df.flexible = true,
		AlterKind::Drop => df.flexible = false,
		AlterKind::None => {}
	}

	match this.readonly {
		AlterKind::Set(_) => df.readonly = true,
		AlterKind::Drop => df.readonly = false,
		AlterKind::None => {}
	}

	match this.inline {
		AlterKind::Set(_) => df.inline = true,
		AlterKind::Drop => df.inline = false,
		AlterKind::None => {}
	}

	match this.value {
		AlterKind::Set(ref k) => {
			df.value = Some(k.clone());
			df.value_body = Some(catalog::LogicalBody::of(k));
		}
		AlterKind::Drop => {
			df.value = None;
			df.value_body = None;
		}
		AlterKind::None => {}
	}

	match this.assert {
		AlterKind::Set(ref k) => {
			df.assert = Some(k.clone());
			df.assert_body = Some(catalog::LogicalBody::of(k));
		}
		AlterKind::Drop => {
			df.assert = None;
			df.assert_body = None;
		}
		AlterKind::None => {}
	}

	match this.default {
		AlterDefault::None => {}
		AlterDefault::Drop => {
			df.default = catalog::DefineDefault::None;
			df.default_body = None;
		}
		AlterDefault::Always(ref expr) => {
			df.default = catalog::DefineDefault::Always(expr.clone());
			df.default_body = Some(catalog::LogicalBody::of(expr));
		}
		AlterDefault::Set(ref expr) => {
			df.default = catalog::DefineDefault::Set(expr.clone());
			df.default_body = Some(catalog::LogicalBody::of(expr));
		}
	}

	if let Some(permissions) = &this.permissions {
		df.select_permission = permissions.select.clone();
		df.select_body = catalog::LogicalPermissionBody::of(&permissions.select);
		df.create_permission = permissions.create.clone();
		df.update_permission = permissions.update.clone();
	}

	match this.comment {
		AlterKind::Set(ref k) => df.comment = Some(k.clone()),
		AlterKind::Drop => df.comment = None,
		AlterKind::None => {}
	}

	match this.reference {
		AlterKind::Set(ref k) => df.reference = Some(k.clone()),
		AlterKind::Drop => df.reference = None,
		AlterKind::None => {}
	}

	match this.sql_type {
		AlterKind::Set(ref k) => df.sql_type = Some(k.clone()),
		AlterKind::Drop => df.sql_type = None,
		AlterKind::None => {}
	}

	let kind_changed = !matches!(this.kind, AlterKind::None);
	let reference_altered = !matches!(this.reference, AlterKind::None);
	let migration = crate::exec::migrate::FieldMigration {
		request: this.migrate.as_ref().map(crate::exec::migrate::request_of).transpose()?,
		kept: crate::exec::migrate::KeptClauses {
			value: kind_changed && matches!(this.value, AlterKind::None),
			assert: kind_changed && matches!(this.assert, AlterKind::None),
		},
	};
	alter_field_write(
		df,
		old_definition,
		(kind_changed, reference_altered, migration),
		(&scope, &what),
		ctx,
		opt,
	)
	.await
}

/// Applies the clauses of a statement that holds them as one definition to the field `idiom`
/// of table `what`. Only the fields named in `fields` are copied from `altered`.
///
/// `altered` is that definition, with the defaults of the stored type where a clause is not
/// set. `kind_changed` is true when the statement sets or drops the type.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn alter_field_definition_apply(
	altered: &catalog::FieldDefinition,
	fields: &[String],
	(kind_changed, if_exists): (bool, bool),
	migration: crate::exec::migrate::FieldMigration<'_>,
	idiom: crate::expr::Idiom,
	what: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let Some((mut df, scope)) = alter_field_read(&idiom, &what, if_exists, ctx, opt).await? else {
		return Ok(Value::None);
	};
	let old_definition = df.clone();
	for field in fields {
		match field.as_str() {
			// The type and the record id structure it names are set together.
			"field_kind" => {
				df.field_kind.clone_from(&altered.field_kind);
				df.key.clone_from(&altered.key);
			}
			"flexible" => df.flexible = altered.flexible,
			"readonly" => df.readonly = altered.readonly,
			"inline" => df.inline = altered.inline,
			"value" => {
				df.value.clone_from(&altered.value);
				df.value_body.clone_from(&altered.value_body);
			}
			"assert" => {
				df.assert.clone_from(&altered.assert);
				df.assert_body.clone_from(&altered.assert_body);
			}
			"default" => {
				df.default.clone_from(&altered.default);
				df.default_body.clone_from(&altered.default_body);
			}
			"select_permission" => {
				df.select_permission.clone_from(&altered.select_permission);
				df.select_body.clone_from(&altered.select_body);
			}
			"create_permission" => df.create_permission.clone_from(&altered.create_permission),
			"update_permission" => df.update_permission.clone_from(&altered.update_permission),
			"comment" => df.comment.clone_from(&altered.comment),
			"reference" => df.reference.clone_from(&altered.reference),
			"sql_type" => df.sql_type.clone_from(&altered.sql_type),
			other => anyhow::bail!(
				"an altered field clause `{other}` that no clause of ALTER FIELD names"
			),
		}
	}
	let reference_altered = fields.iter().any(|field| field == "reference");
	alter_field_write(
		df,
		old_definition,
		(kind_changed, reference_altered, migration),
		(&scope, &what),
		ctx,
		opt,
	)
	.await
}

/// Validates the definition `df` that an `ALTER FIELD` produces from `old_definition`, judges it
/// against the rules of a table that enforces migrations and commits the statement's migration,
/// then writes it, updates the definitions it implies, and clears the table cache.
///
/// `kind_changed` is true when the statement sets or drops the type, and `reference_altered`
/// when it sets or drops `REFERENCE`.
async fn alter_field_write(
	mut df: catalog::FieldDefinition,
	old_definition: catalog::FieldDefinition,
	(kind_changed, reference_altered, migration): (
		bool,
		bool,
		crate::exec::migrate::FieldMigration<'_>,
	),
	(scope, what): (&Scope<'_>, &TableName),
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	let Scope {
		ns_name,
		db_name,
		ns,
		db,
	} = *scope;
	let txn = ctx.tx();
	let name = df.name.to_raw_string();
	// Recompute auth_limit from the current principal to prevent privilege escalation
	df.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

	// The `id` field refuses the same clauses as DEFINE FIELD: VALUE, REFERENCE, COMPUTED,
	// DEFAULT ALWAYS, READONLY, FLEXIBLE and non-key TYPEs. The check runs on the merged
	// definition, so any ALTER that sets one of these is refused.
	crate::exec::field_rules::id_field_restrictions(&df)?;

	// Every type the merged definition names, in its kind and its clauses, is one the database
	// defines, as `DEFINE FIELD` requires.
	let named = crate::exec::vm::schema_host::field_named_types(&df);
	if !named.is_empty() {
		let subject = format!("The field '{name}' on the table '{what}'");
		crate::exec::vm::schema_host::check_types_defined(&txn, ns, db, &named, &subject).await?;
	}

	// A statement that sets `REFERENCE`, or changes the type of a field whose `REFERENCE` it
	// keeps, has the merged definition judged as `DEFINE FIELD` judges a reference: a top-level
	// field whose kind holds only records. Import skips this check, so exported definitions load
	// as they were written.
	if (kind_changed || reference_altered) && !opt.import {
		crate::exec::field_rules::define_field_statement_validate_reference_options(&df)?;
	}

	// A field a `MATERIALIZE INTO` clause fills is not altered, and the clause the merged
	// definition holds is judged against its destination. Import restores definitions as they
	// were written.
	if !opt.import {
		crate::ddl::materialize::refuse_changing_destination(
			&txn,
			ns,
			db,
			what,
			&old_definition.name,
		)
		.await?;
		crate::ddl::materialize::validate(&txn, ns, db, &df).await?;
	}

	// The record id components that the merged definition names are checked as
	// `DEFINE FIELD OVERWRITE` checks them. Only a statement that sets or drops the type of
	// `id` can change them. A field under a component's name is refused above.
	if df.name.is_id() && kind_changed {
		crate::ddl::define_field_statement_validate_key_components(
			&txn,
			ns,
			db,
			&DefineKind::Overwrite,
			&df,
		)
		.await?;
	}

	// A `TYPE` the statement sets is judged against every field the field is nested in, as
	// `DEFINE FIELD` judges a new one, and refused with the same error. An import is judged as
	// `DEFINE FIELD` judges one: an export writes each field as `DEFINE FIELD OVERWRITE`, never as
	// `ALTER FIELD`, so no export replays through this check.
	if kind_changed {
		crate::ddl::define_field_statement_disallow_mismatched_types(&txn, ns, db, &df).await?;
	}

	// INLINE embeds the field's value into the edge's adjacency payloads;
	// the same restrictions as DEFINE FIELD, validated against the fully
	// resolved definition.
	if df.inline && !opt.import {
		let is_relation = txn
			.get_tb(ns, db, what, None)
			.await?
			.is_some_and(|tb| matches!(tb.table_type, catalog::TableType::Relation(_)));
		ensure!(
			is_relation,
			ExecError::Thrown(
				"INLINE requires an existing TYPE RELATION table: the value is embedded into \
				 the edges' adjacency payloads"
					.to_owned(),
			)
		);
		ensure!(
			!matches!(name.as_str(), "id" | "in" | "out"),
			ExecError::Thrown(
				"the `id`, `in` and `out` fields already live in the adjacency keys and cannot \
				 be INLINE"
					.to_owned(),
			)
		);
		ensure!(
			df.name.len() == 1,
			ExecError::Thrown("only a top-level field can be INLINE".to_owned())
		);
		ensure!(
			df.computed.is_none(),
			ExecError::Thrown(
				"a COMPUTED field cannot be INLINE: its value is derived at read time, so \
				 there is nothing stored to embed"
					.to_owned(),
			)
		);
		// The adjacency payload stores its value count in one byte, so a table holds at most
		// 255 INLINE fields. The change is rejected here, before an edge write can fail on it.
		let fields = txn.all_tb_fields(ns, db, what, None).await?;
		let others = fields.iter().filter(|f| f.inline && f.name != df.name).count();
		ensure!(
			others < u8::MAX as usize,
			ExecError::Thrown(
				"a table cannot carry more than 255 INLINE fields: the adjacency payload's \
				 value count is a single byte"
					.to_owned(),
			)
		);
	}

	if df.flexible {
		let tb = txn.get_tb(ns, db, what, None).await?;
		crate::exec::field_rules::flexible_restrictions(&df, tb.as_deref())?;
	}

	// A `TYPE` the statement alters follows the rules `DEFINE FIELD` applies to a new one.
	// The fields nested in the field are checked against it, and the definitions it implies
	// for the elements of an array or set are written with it. Import skips this check, so
	// exported definitions load as they were written. The fields it is nested in are judged
	// above, an import included.
	if kind_changed && !opt.import {
		let fields = txn.all_tb_fields(ns, db, what, None).await?;
		crate::exec::field_rules::define_field_nesting_restrictions(&fields, &df)?;
	}

	// A record id that names its components must admit every record the table
	// holds; the scan is skipped when the stored `id` already has the same
	// structure. A definition that leaves the table with no structure is held
	// to the order the records it already holds are stored in instead.
	if df.live_key().is_some() || df.name.is_id() {
		let exec = crate::ddl::frame(ctx, opt, None).await;
		match df.live_key() {
			Some(key) => {
				crate::ddl::key_scan::validate_record_keys(&exec, &txn, ns, db, what, key).await?;
			}
			None => {
				crate::ddl::key_scan::refuse_dropping_unordered_keys_for(&exec, &txn, ns, db, &df)
					.await?;
			}
		}
	}

	// Altering the kind of a `COMPUTED` field changes the value it derives.
	if !opt.import {
		crate::ddl::define_field_statement_validate_computed_views(
			&txn,
			ns,
			db,
			Some(&old_definition),
			&df,
		)
		.await?;
	}

	// A clause the statement changed holds no effect a schema clause must not
	// have; one it left as it was is judged where it runs.
	if !opt.import {
		crate::exec::clause_effects::ClauseCheck {
			txn: &txn,
			ns,
			db,
			registry: ctx.function_registry(),
		}
		.refuse_field(Some(&old_definition), &df)
		.await?;
	}

	// The rules of a table that enforces migrations, and the statement's migration, are judged
	// once every other check of the definition has passed: either can mint a step, and a refusal
	// leaves no step behind for a transaction its caller goes on to commit.
	let exec = crate::ddl::frame(ctx, opt, None).await;
	let stored =
		migration.before_store(&exec, opt.import, ns, db, what, &df, Some(&old_definition)).await?;
	if stored == crate::exec::migrate::FieldStore::AlreadyRun {
		return Ok(Value::None);
	}
	txn.put_tb_field(ns, db, what, &df).await?;
	crate::exec::migrate::FieldMigration::after_store(
		stored,
		&exec,
		opt.import,
		ns,
		db,
		what,
		&df,
		Some(&old_definition),
	)
	.await?;
	if kind_changed {
		crate::ddl::define_field_statement_process_recursive_definitions(
			ns,
			db,
			Arc::clone(&txn),
			Some(&old_definition),
			&df,
			!opt.import,
		)
		.await?;
	}
	// Dropping REFERENCE, or narrowing or changing the record kind, can leave reference keys
	// under target tables that the field no longer references; those keys are purged so that
	// DELETE's reference checks stay correct. Adding REFERENCE, or changing the kind of one,
	// writes the keys of the links the stored records hold. Import skips both, since it restores
	// reference keys as they were stored.
	if !opt.import {
		crate::exec::reference_purge::reconcile_reference_keys(
			&exec,
			ns,
			db,
			what,
			Some(&old_definition),
			&df,
		)
		.await?;
		crate::exec::materialize::backfill(&exec, Some(&old_definition), &df, false).await?;
	}
	// Refresh the table cache
	let Some(tb) = txn.get_tb(ns, db, what, None).await? else {
		return Err(CatalogError::TbNotFound {
			name: what.clone(),
		}
		.into());
	};
	// The generation identifies the payload shape: the inline field set's
	// names, order, and types. Bump it only when this ALTER changes that
	// shape — the field joins or leaves the inline set, or an inline
	// field's declared type changes — so readers fall back until a write
	// restamps the payloads. Clauses that never enter a payload
	// (PERMISSIONS, COMMENT, ASSERT, DEFAULT, VALUE) leave written
	// payloads valid: permissions are re-checked at plan time, and the
	// payload stores whatever values the record stores.
	let inline_toggled = df.inline != old_definition.inline;
	let inline_kind_changed =
		df.inline && old_definition.inline && df.field_kind != old_definition.field_kind;
	txn.replace_tb(
		ns_name,
		db_name,
		&catalog::TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			graph_inline_gen: if inline_toggled || inline_kind_changed {
				tb.graph_inline_gen.wrapping_add(1)
			} else {
				tb.graph_inline_gen
			},
			..(*tb).clone()
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
