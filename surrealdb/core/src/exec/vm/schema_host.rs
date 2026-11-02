//! The schema work a program leaves to the host where a name it defines,
//! alters or removes under is known only as the program runs.
//!
//! A statement whose names are written has its checks decided when it is
//! lowered. One whose field path, table or index column is evaluated cannot
//! have: the program resolves the name first, and these services then do
//! what the statement does with a name it has resolved — the same code, so
//! that its refusals, the keys it writes and the order it does both in are
//! one. Each takes plain arguments (the resolved names, the definition, the
//! session's context and options) and plans nothing.

use crate::catalog::{AccessDefinition, DatabaseId, NamespaceId, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::Idiom;
use crate::expr::statements::alter::AlterFieldStatement;
use crate::expr::statements::define::DefineKind;
use crate::kvs::Transaction;
use crate::val::{TableName, Value};

/// The refusal an altered access definition meets, as `ALTER ACCESS` raises it
/// before the definition is written back.
pub(super) fn check_access_altered(access: &AccessDefinition) -> anyhow::Result<()> {
	crate::ddl::alter::access::alter_access_definition_validate(access)
}

/// The paths the strings of `value` name, as `type::fields(value)` names them.
pub(super) fn type_fields_paths(value: Value) -> anyhow::Result<Vec<Idiom>> {
	crate::ddl::scalar::type_fields_paths(value)
}

/// The path the string `value` names, as `type::field(value)` names it.
pub(super) fn type_field_path(value: Value) -> anyhow::Result<Idiom> {
	crate::ddl::scalar::type_field_path(value)
}

/// `idiom` with the evaluated values given to its subscripts that were
/// written as expressions.
pub(super) fn substitute_evaluated(idiom: Idiom, evaluated: Vec<Value>) -> anyhow::Result<Idiom> {
	crate::ddl::scalar::substitute_evaluated(idiom, evaluated)
}

/// The path the text in `value` names, refusing a value that is not text.
pub(super) fn parse_path(value: Value, into: &str) -> anyhow::Result<Idiom> {
	crate::ddl::scalar::parse_path(value, into)
}

/// `DEFINE FIELD` once its definition is formed: its check against the
/// indexes the table holds, then the field, its nested fields and the table
/// written. `kind` decides what a field that exists does.
pub(super) async fn define_field(
	kind: &DefineKind,
	definition: &crate::catalog::FieldDefinition,
	migration: crate::exec::migrate::FieldMigration<'_>,
	ctx: &FrozenContext,
	opt: &Options,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(
		crate::ddl::define::field::define_field_definition_check_indexed(definition, ctx, opt),
	)
	.await?;
	crate::exec::vm::descriptor::schema(crate::ddl::define::field::define_field_definition_apply(
		kind, definition, migration, ctx, opt,
	))
	.await?;
	Ok(())
}

/// `REMOVE FIELD` once the field and its table are named.
pub(super) async fn remove_field(
	if_exists: bool,
	name: Idiom,
	table: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::remove::field::remove_field_statement_apply(
		if_exists, name, table, ctx, opt,
	))
	.await?;
	Ok(())
}

/// `ALTER FIELD` once the field and its table are named.
pub(super) async fn alter_field(
	statement: &AlterFieldStatement,
	name: Idiom,
	table: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::alter::field::alter_field_statement_apply(
		statement, name, table, ctx, opt,
	))
	.await?;
	Ok(())
}

/// `ALTER FIELD` once the field and its table are named, for a statement that holds its clauses
/// as the definition they form.
#[allow(clippy::too_many_arguments)]
pub(super) async fn alter_field_clauses(
	altered: &crate::catalog::FieldDefinition,
	fields: &[String],
	(kind_changed, if_exists): (bool, bool),
	migration: crate::exec::migrate::FieldMigration<'_>,
	name: Idiom,
	table: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::alter::field::alter_field_definition_apply(
		altered,
		fields,
		(kind_changed, if_exists),
		migration,
		name,
		table,
		ctx,
		opt,
	))
	.await?;
	Ok(())
}

/// The check `DEFINE INDEX` makes of one column against the table it
/// indexes.
pub(super) async fn check_index_column(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	schemafull: bool,
	index: &str,
	column: &Idiom,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::define::index::define_index_check_column(
		txn, ns, db, table, schemafull, index, column,
	))
	.await
}

/// The checks `ALTER TABLE` makes of the table as altered against the table as
/// it was, and against the records it holds; `altered` is normalised as they
/// normalise it.
pub(super) async fn check_table_altered(
	ctx: &crate::exec::ExecutionContext,
	txn: &Transaction,
	previous: &TableDefinition,
	altered: &mut TableDefinition,
) -> anyhow::Result<()> {
	let (ns, db) = (previous.namespace_id, previous.database_id);
	crate::exec::vm::descriptor::schema(crate::ddl::define::table::validate_lightweight_table(
		txn,
		ns,
		db,
		Some(previous),
		altered,
	))
	.await?;
	crate::exec::vm::descriptor::schema(
		crate::ddl::define::table::validate_inline_fields_survive_type_change(
			txn,
			ns,
			db,
			Some(previous),
			altered,
		),
	)
	.await?;
	crate::exec::vm::descriptor::schema(
		crate::ddl::define::table::validate_named_id_survives_conversion(
			txn, ns, db, previous, altered,
		),
	)
	.await?;
	crate::exec::vm::descriptor::schema(
		crate::ddl::define::table::validate_records_survive_type_change(
			ctx,
			txn,
			Some(previous),
			altered,
		),
	)
	.await
}

/// The checks a field statement makes of `definition`, the field it forms in
/// the database `ns`, `db`: the record id components it names, and whether it
/// clashes with one.
/// They are judged as a `DEFINE FIELD` of `kind` judges them, against the
/// catalog and the table's view rows as `txn` holds them.
pub(super) async fn check_key_components(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &DefineKind,
	definition: &crate::catalog::FieldDefinition,
) -> anyhow::Result<()> {
	let txn = ctx.txn();
	crate::exec::vm::descriptor::schema(crate::ddl::define_field_statement_validate_key_components(
		&txn, ns, db, kind, definition,
	))
	.await
}

/// The order the records of the table `definition` defines the `id` of are
/// stored in, which holds a definition that leaves that table with no record
/// id structure, judged against the catalog of the database `ns`, `db` as
/// `ctx` holds it. A definition that keeps a structure, or defines any other
/// field, reads no record.
pub(super) async fn refuse_dropping_key_structure(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &crate::catalog::FieldDefinition,
) -> anyhow::Result<()> {
	let txn = ctx.txn();
	crate::exec::vm::descriptor::schema(crate::ddl::key_scan::refuse_dropping_unordered_keys_for(
		ctx, &txn, ns, db, definition,
	))
	.await
}

/// What refuses a field statement of the definition `definition`, replacing
/// `existing` when the field has one, in the database `ns`, `db`: a change to
/// the value the field derives that an aggregated view of its table depends on,
/// while the table holds a record, as `txn` holds the catalog and the table.
pub(super) async fn check_computed_views(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: Option<&crate::catalog::FieldDefinition>,
	definition: &crate::catalog::FieldDefinition,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::define_field_statement_validate_computed_views(
		txn, ns, db, existing, definition,
	))
	.await
}

/// What refuses the `MATERIALIZE INTO` clause of the field `field`, as `DEFINE FIELD` and
/// `ALTER FIELD` judge it against the catalog of the database `ns`, `db`.
pub(super) async fn validate_materialize(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	field: &crate::catalog::FieldDefinition,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::materialize::validate(txn, ns, db, field)).await
}

/// What refuses a change to the field `name` of the table `table` of the database `ns`, `db`
/// while a `MATERIALIZE INTO` clause fills it.
pub(super) async fn refuse_changing_destination(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	name: &Idiom,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::materialize::refuse_changing_destination(
		txn, ns, db, table, name,
	))
	.await
}

/// What refuses the removal of the table `table` of the database `ns`, `db` while a
/// `MATERIALIZE INTO` clause fills one of its fields.
pub(super) async fn refuse_removing_filled_table(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::materialize::refuse_removing_table(
		txn, ns, db, table,
	))
	.await
}

/// What refuses `REMOVE FIELD id` on the table `table` of the database `ns`,
/// `db`: a definition of the table that reads a component of its record id,
/// as `txn` holds the catalog and the rows of the table's views.
pub(super) async fn refuse_removing_id(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> anyhow::Result<()> {
	let txn = ctx.txn();
	crate::exec::vm::descriptor::schema(crate::ddl::key_scan::refuse_removing_field(
		ctx,
		&txn,
		ns,
		db,
		table,
		&Idiom::field("id"),
	))
	.await
}

/// What refuses turning the table `table` of the database `ns`, `db` into `to`: a
/// record id that names its components, as `txn` holds the table's `id`.
pub(super) async fn refuse_named_id_conversion(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	to: crate::ddl::define::table::TableConversion,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::define::table::refuse_named_id_conversion(
		txn, ns, db, table, to,
	))
	.await
}

/// The check `DEFINE FIELD` and `ALTER FIELD` make of the records of `table` in
/// the database `ns`, `db` against the record id structure `key`: every stored
/// record id and body must fit it, unless the stored `id` already has `key`.
pub(super) async fn validate_record_keys(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	key: &std::sync::Arc<crate::catalog::KeyStructure>,
) -> anyhow::Result<()> {
	let txn = ctx.txn();
	crate::exec::vm::descriptor::schema(crate::ddl::key_scan::validate_record_keys(
		ctx, &txn, ns, db, table, key,
	))
	.await
}

/// Purges from their endpoints the adjacency of every edge the table `table`
/// of the database `ns`, `db` holds, before `REMOVE TABLE` deletes the table's
/// own keyspace.
pub(super) async fn purge_edge_pointers(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	expunge: bool,
) -> anyhow::Result<()> {
	let txn = ctx.txn();
	crate::exec::vm::descriptor::schema(crate::ddl::remove::table::purge_edge_pointers(
		ctx, &txn, ns, db, table, expunge,
	))
	.await
}

/// What `DEFINE FIELD` and `ALTER FIELD` refuse of the `REFERENCE` clause of the
/// field `field`, judged as their bodies judge it.
pub(super) fn validate_reference_options(
	field: &crate::catalog::FieldDefinition,
) -> anyhow::Result<()> {
	crate::exec::field_rules::define_field_statement_validate_reference_options(field)
}

/// Deletes every key beneath the root of the table `table` of the database
/// `ns`, `db` but the registrations of the views that read it, under
/// `keep_fields` its field definitions and, under `keep_records`, its records
/// and the layout state they are decoded through,
/// before `DEFINE TABLE ... AS SELECT` fills the view.
pub(super) async fn clear_view_table(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	keep_fields: bool,
	keep_records: bool,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::define::table::clear_view_table(
		txn,
		ns,
		db,
		table,
		keep_fields,
		keep_records,
	))
	.await
}

/// What refuses the view `table` of the database `ns`, `db` reading `sources`:
/// a source that is the view, or that reads it through the views registered on
/// it.
pub(super) async fn refuse_view_cycle(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	sources: &[TableName],
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::define::table::refuse_view_cycle(
		txn, ns, db, table, sources,
	))
	.await
}

/// What refuses removing table `name` from the database `ns`, `db`. A non-empty
/// LIGHTWEIGHT relation that uses `name` as an endpoint is reported first, then
/// a non-empty LIGHTWEIGHT `name` itself. Edges are read through `txn`.
pub(super) async fn refuse_removing_lightweight(
	txn: &crate::kvs::Transaction,
	ns: crate::catalog::NamespaceId,
	db: crate::catalog::DatabaseId,
	name: &crate::val::TableName,
) -> anyhow::Result<Option<super::program::FieldRefusal>> {
	use crate::catalog::providers::TableProvider;
	use crate::kvs::lightweight::{lightweight_relation, lightweight_relation_is_empty};
	let (mut endpoint_refusal, mut own_refusal) = (None, None);
	for other in txn.all_tb(ns, db, None).await?.iter() {
		if other.name == *name {
			if let Some(rel) = lightweight_relation(&other.table_type)
				&& !lightweight_relation_is_empty(txn, ns, db, name, rel).await?
			{
				own_refusal = Some(super::program::FieldRefusal::Thrown(format!(
					"cannot remove the LIGHTWEIGHT relation `{name}` while it holds edges: delete \
					 them first (e.g. `DELETE {name}`)"
				)));
			}
		} else if let Some(rel) = lightweight_relation(&other.table_type)
			&& (rel.from.contains(name) || rel.to.contains(name))
			&& endpoint_refusal.is_none()
			&& !lightweight_relation_is_empty(txn, ns, db, &other.name, rel).await?
		{
			endpoint_refusal = Some(super::program::FieldRefusal::Thrown(format!(
				"cannot remove `{name}`: the LIGHTWEIGHT relation `{}` uses it as an endpoint and \
				 still holds edges — delete them first (e.g. `DELETE {}`)",
				other.name, other.name
			)));
		}
	}
	Ok(endpoint_refusal.or(own_refusal))
}

/// What refuses `definition`, a table definition, in the database `ns`, `db`,
/// as `txn` holds the table it redefines and that table's contents: as
/// `DEFINE TABLE` judges a LIGHTWEIGHT relation, the INLINE fields of a
/// relation changing its type, and a type the table's records do not admit.
pub(super) async fn refuse_redefining_table(
	ctx: &crate::exec::ExecutionContext,
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &TableDefinition,
) -> anyhow::Result<()> {
	use crate::catalog::providers::TableProvider;
	let existing = txn.get_tb(ns, db, &definition.name, None).await?;
	let mut definition = definition.clone();
	crate::ddl::define::table::validate_lightweight_table(
		txn,
		ns,
		db,
		existing.as_deref(),
		&mut definition,
	)
	.await?;
	if existing.is_some() {
		crate::ddl::define::table::validate_inline_fields_survive_type_change(
			txn,
			ns,
			db,
			existing.as_deref(),
			&definition,
		)
		.await?;
	}
	crate::ddl::define::table::validate_records_survive_type_change(
		ctx,
		txn,
		existing.as_deref(),
		&definition,
	)
	.await
}

/// Applies the edits of an `ALTER ENUM` to `definition`, in order.
///
/// An edit that names a label the enum does not hold, or adds one it does, is refused with the
/// enum named in the message; an `IF NOT EXISTS` add of a label the enum holds is skipped.
pub(super) fn edit_enum(
	definition: &mut crate::catalog::EnumDefinition,
	edits: &[crate::exec::ir::iface::statements::EnumEdit],
) -> anyhow::Result<()> {
	use crate::catalog::{EnumEditError, EnumPlacement};
	use crate::exec::ir::iface::statements::EnumEdit;
	fn refused(definition: &crate::catalog::EnumDefinition, error: EnumEditError) -> anyhow::Error {
		match error {
			EnumEditError::Exists(label) => {
				anyhow::anyhow!("The enum '{}' already has the value '{label}'", definition.name)
			}
			EnumEditError::Missing(label) => anyhow::anyhow!(
				"The enum '{}' has no value '{label}'; its values are {}",
				definition.name,
				valid_labels(definition)
			),
			EnumEditError::Full => anyhow::anyhow!(
				"The enum '{}' cannot take another value: an enum holds at most {} values over \
				 its life",
				definition.name,
				crate::catalog::ENUM_LABEL_IDENT_LIMIT - 1
			),
		}
	}
	for edit in edits {
		match edit {
			EnumEdit::Add {
				value,
				anchor,
				after,
				if_not_exists,
			} => {
				if *if_not_exists && definition.contains(value) {
					continue;
				}
				let placement = match (anchor.as_deref(), after) {
					(Some(anchor), false) => EnumPlacement::Before(anchor),
					(Some(anchor), true) => EnumPlacement::After(anchor),
					(None, _) => EnumPlacement::Last,
				};
				definition.add_label(value, placement).map_err(|e| refused(definition, e))?;
			}
			EnumEdit::Rename {
				from,
				to,
			} => {
				definition.rename_label(from, to).map_err(|e| refused(definition, e))?;
			}
		}
	}
	Ok(())
}

/// The labels of `definition` as an error lists them.
fn valid_labels(definition: &crate::catalog::EnumDefinition) -> String {
	definition.label_names().map(|label| format!("'{label}'")).collect::<Vec<_>>().join(", ")
}

/// The labels `edits` rename, each as `(label before the edits, label after them)`, for the
/// labels whose spelling ends different. The edits apply in order, so a label renamed twice is
/// one rename, and the pairs are read together against the values stored before the edits.
///
/// A label an `ADD VALUE` of the same list introduced holds no stored value, so renaming it
/// renames only that label and yields no pair. An `IF NOT EXISTS` add of a label the list has not
/// otherwise named may have been skipped over a label the enum held before the edits; it is read
/// as that stored label, so a later rename of it still rewrites the values that hold it.
fn composed_renames(
	edits: &[crate::exec::ir::iface::statements::EnumEdit],
) -> Vec<(String, String)> {
	use crate::exec::ir::iface::statements::EnumEdit;
	let mut renames: Vec<(String, String)> = Vec::new();
	// The current spelling of each label an add of this list introduced.
	let mut added: Vec<String> = Vec::new();
	for edit in edits {
		match edit {
			EnumEdit::Add {
				value,
				if_not_exists,
				..
			} => {
				let held = renames.iter().any(|(_, now)| now == value) || added.contains(value);
				// A label renamed away and not named since is absent, so an `IF NOT EXISTS`
				// add of it introduces it.
				let absent = renames.iter().any(|(before, _)| before == value);
				if !*if_not_exists || (!held && absent) {
					added.push(value.clone());
				}
			}
			EnumEdit::Rename {
				from,
				to,
			} => {
				if let Some(label) = added.iter_mut().find(|label| *label == from) {
					label.clone_from(to);
					continue;
				}
				match renames.iter_mut().find(|(_, now)| now == from) {
					Some((_, now)) => now.clone_from(to),
					None => renames.push((from.clone(), to.clone())),
				}
			}
		}
	}
	renames.retain(|(before, after)| before != after);
	renames
}

/// The text of an expression that reads the enum labels `expr` holds as `kind` declares them, with
/// the labels `renames` names replaced, or why the kind holds the enum where no rewrite reaches.
fn renamed_labels(
	kind: &crate::expr::Kind,
	expr: &str,
	enum_name: &str,
	renames: &[(String, String)],
) -> anyhow::Result<String> {
	use crate::expr::{EnumRef, Kind};
	let quoted = |label: &str| surrealdb_types::ToSql::to_sql(&Value::from(label.to_owned()));
	if let Kind::Enum(EnumRef::Named(name)) = kind
		&& name == enum_name
	{
		let mut text = String::new();
		for (before, after) in renames {
			text.push_str(&format!("IF {expr} = {} THEN {} ELSE ", quoted(before), quoted(after)));
		}
		text.push_str(expr);
		text.push_str(" END");
		return Ok(text);
	}
	if let Kind::Either(kinds) = kind {
		let mut current = expr.to_owned();
		for member in kinds.iter().filter(|member| member.references_enum(enum_name)) {
			current = renamed_labels(member, &current, enum_name, renames)?;
		}
		return Ok(current);
	}
	if let Kind::Array(inner, _) = kind {
		let element = renamed_labels(inner, "$v", enum_name, renames)?;
		return Ok(format!(
			"IF type::is_array({expr}) THEN array::map({expr}, |$v| {element}) ELSE {expr} END"
		));
	}
	if let Kind::Set(inner, _) = kind {
		let element = renamed_labels(inner, "$v", enum_name, renames)?;
		return Ok(format!(
			"IF type::is_set({expr}) THEN <set> array::map(<array> {expr}, |$v| {element}) ELSE {expr} END"
		));
	}
	anyhow::bail!(
		"the enum '{enum_name}' is named inside the type `{}`, which a rename does not reach",
		surrealdb_types::ToSql::to_sql(kind)
	)
}

/// The test, over the stored value `expr` of a field of `kind`, that it holds one of the labels
/// `renames` names before the edits: the value is one, or a list or a set that has one. `None` for
/// a type that holds the enum below the first level of a collection, which no such test reaches.
fn holds_renamed_label(
	kind: &crate::expr::Kind,
	expr: &str,
	renames: &[(String, String)],
	enum_name: &str,
) -> Option<String> {
	use crate::expr::{EnumRef, Kind};
	let labels = renames
		.iter()
		.map(|(before, _)| surrealdb_types::ToSql::to_sql(&Value::from(before.clone())))
		.collect::<Vec<_>>()
		.join(", ");
	if let Kind::Enum(EnumRef::Named(name)) = kind {
		return (name == enum_name).then(|| format!("{expr} IN [{labels}]"));
	}
	if let Kind::Either(kinds) = kind {
		let tests: Option<Vec<String>> = kinds
			.iter()
			.filter(|member| member.references_enum(enum_name))
			.map(|member| holds_renamed_label(member, expr, renames, enum_name))
			.collect();
		return tests.map(|tests| tests.join(" OR "));
	}
	if let Kind::Array(inner, _) | Kind::Set(inner, _) = kind {
		// An element holds a label at the first level when it is the enum, or a union whose only
		// members naming the enum are the enum itself (`option<enum<e>>`). A member that holds the
		// enum deeper (`array<enum<e>>`, an object) is beyond what `CONTAINSANY` tests.
		let is_enum = |k: &Kind| matches!(k, Kind::Enum(EnumRef::Named(name)) if name == enum_name);
		let element = if let Kind::Either(members) = &**inner {
			members.iter().filter(|member| member.references_enum(enum_name)).all(is_enum)
		} else {
			is_enum(inner)
		};
		return element.then(|| {
			format!(
				"(type::is_array({expr}) OR type::is_set({expr})) AND {expr} CONTAINSANY [{labels}]"
			)
		});
	}
	None
}

/// The parameter that marks the frame of an enum relabel ([`rewrite_enum_labels`]).
const RELABEL: &str = "__enum_relabel";

/// The value of the [`RELABEL`] marker. It is drawn once per process, so a variable a request
/// binds under the marker's name does not carry it.
static RELABEL_TOKEN: std::sync::LazyLock<Value> =
	std::sync::LazyLock::new(|| Value::Uuid(crate::val::Uuid(uuid::Uuid::new_v4())));

/// Whether the frame `exec` is the frame of the write that rewrites the labels of a renamed enum.
/// The field kernel evaluates no `VALUE` and no `DEFAULT` in it: the write changes the spelling of
/// stored labels and nothing a field clause derives.
pub(crate) fn is_relabel(exec: &crate::exec::ExecutionContext) -> bool {
	exec.ctx().value(RELABEL).is_some_and(|value| value == &*RELABEL_TOKEN)
}

/// Rewrites, for the renames `edits` make to the enum `name`, the stored values of every field of
/// the database `ns`/`db` whose type names the enum.
///
/// Each table with such a field is updated once, setting all of its fields together, because a
/// write judges every field of the record against the stored definition and a record holding the
/// old label in a field not yet rewritten would be refused. Only the records whose value of one
/// of those fields holds a renamed label are written.
///
/// The update is engine maintenance, not a write of the statement's actor: it runs with
/// permission enforcement suppressed and under the marker of [`crate::exec::materialize::mark`],
/// so a `READONLY` field is rewritten and no `EVENT` fires, and under [`is_relabel`], so no field's
/// `VALUE` or `DEFAULT` is evaluated. Index entries, views, live queries and the change feed follow
/// it as they follow any write, and each field's type and `ASSERT` judge the rewritten value. It
/// runs in the transaction of the `ALTER` after the edited definition is stored. A field that
/// names the enum where a rename does not reach (an object or a tuple type) refuses the `ALTER`
/// rather than leave records that fail their next write.
pub(super) async fn rewrite_enum_labels(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
	edits: &[crate::exec::ir::iface::statements::EnumEdit],
) -> anyhow::Result<()> {
	use crate::catalog::providers::TableProvider;
	let renames = composed_renames(edits);
	if renames.is_empty() {
		return Ok(());
	}
	let txn = ctx.txn();
	let mut statements = Vec::new();
	crate::exec::vm::descriptor::schema(async {
		for table in txn.all_tb(ns, db, None).await?.iter() {
			// A view recomputes from its sources, which are rewritten themselves.
			if table.view.is_some() {
				continue;
			}
			let mut assigned: Vec<(String, String)> = Vec::new();
			let mut holders: Vec<String> = Vec::new();
			for field in txn.all_tb_fields(ns, db, &table.name, None).await?.iter() {
				let Some(kind) = &field.field_kind else {
					continue;
				};
				if field.is_key_part() || !kind.references_enum(name) {
					continue;
				}
				let path = surrealdb_types::ToSql::to_sql(&field.name);
				let (target, element) = match path.strip_suffix("[*]").or(path.strip_suffix(".*")) {
					Some(base) => (base.to_owned(), true),
					None => (path, false),
				};
				if target.contains("[*]") || target.contains(".*") {
					anyhow::bail!(
						"the enum '{name}' is held by the field '{}' on the table '{}', whose path a rename does not reach",
						surrealdb_types::ToSql::to_sql(&field.name),
						table.name
					);
				}
				let value = if element {
					let renamed = renamed_labels(kind, "$v", name, &renames)?;
					format!(
						"IF type::is_array({target}) THEN array::map({target}, |$v| {renamed}) ELSE {target} END"
					)
				} else {
					renamed_labels(kind, &target, name, &renames)?
				};
				if !assigned.iter().any(|(held, _)| *held == target) {
					// An element field is tested as the list that holds the element.
					let tested = if element {
						crate::expr::Kind::Array(Box::new(kind.clone()), None)
					} else {
						kind.clone()
					};
					// A type whose labels no membership test reaches is tested by comparing the
					// stored value with its rewrite.
					let holder = holds_renamed_label(&tested, &target, &renames, name)
						.unwrap_or_else(|| format!("{value} != {target}"));
					holders.push(holder);
					assigned.push((target, value));
				}
			}
			if !assigned.is_empty() {
				let sets: Vec<String> =
					assigned.iter().map(|(target, value)| format!("{target} = {value}")).collect();
				let filter =
					holders.iter().map(|t| format!("({t})")).collect::<Vec<_>>().join(" OR ");
				statements.push(format!(
					"UPDATE {} SET {} WHERE {filter} RETURN NONE",
					surrealdb_types::ToSql::to_sql(&common::fmt::EscapeKwFreeIdent(table.name.as_str())),
					sets.join(", ")
				));
			}
		}
		Ok(())
	})
	.await?;
	if statements.is_empty() {
		return Ok(());
	}
	let text = statements.join("; ");
	let caps = ctx.capabilities();
	let config = crate::syn::ParserConfig::default();
	let ast = crate::syn::parse_with_capabilities(&text, &caps, &config)?;
	let mut expressions = Vec::with_capacity(ast.expressions.len());
	for expr in ast.expressions {
		match expr {
			surrealdb_expr::parser::TopLevelExpr::Expr(expr) => expressions.push(expr),
			surrealdb_expr::parser::TopLevelExpr::Begin(_)
			| surrealdb_expr::parser::TopLevelExpr::Cancel
			| surrealdb_expr::parser::TopLevelExpr::Commit
			| surrealdb_expr::parser::TopLevelExpr::Access(_)
			| surrealdb_expr::parser::TopLevelExpr::Kill(_)
			| surrealdb_expr::parser::TopLevelExpr::Live(_)
			| surrealdb_expr::parser::TopLevelExpr::Option(_)
			| surrealdb_expr::parser::TopLevelExpr::Use(_)
			| surrealdb_expr::parser::TopLevelExpr::Show(_) => {
				anyhow::bail!("the rewrite of the labels of the enum '{name}' is not a query")
			}
		}
	}
	let frame = ctx
		.with_perms_suppressed(true)
		.with_params([crate::exec::materialize::mark(), (RELABEL, RELABEL_TOKEN.clone())]);
	crate::exec::function::run_statements(&frame, 1, expressions).await?;
	Ok(())
}

/// Refuses the removal of the enum `name` while a field of the database `ns`/`db` has a type
/// that names it.
pub(super) async fn check_enum_unreferenced(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
) -> anyhow::Result<()> {
	use crate::catalog::providers::{DatabaseProvider, TableProvider};
	crate::exec::vm::descriptor::schema(async {
		for table in txn.all_tb(ns, db, None).await?.iter() {
			for field in txn.all_tb_fields(ns, db, &table.name, None).await?.iter() {
				if field.field_kind.as_ref().is_some_and(|kind| kind.references_enum(name)) {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot remove the enum '{name}': the field '{}' on the table '{}' has a type that names it",
						surrealdb_types::ToSql::to_sql(&field.name),
						table.name
					)));
				}
			}
		}
		for function in txn.all_db_functions(ns, db, None).await?.iter() {
			let signature = function
				.args
				.iter()
				.map(|(_, kind)| kind)
				.chain(function.returns.as_ref())
				.any(|kind| kind.references_enum(name));
			if signature || block_names_enum(&function.block, name) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot remove the enum '{name}': the function 'fn::{}' names it",
					function.name
				)));
			}
		}
		Ok(())
	})
	.await
}

/// Whether a type written in `block` names the enum `name`: a cast, the type of a `LET`, a
/// closure's parameter or return type, or a type a statement in the block declares. Text that
/// spells the type (a string literal, a comment) does not name it.
fn block_names_enum(block: &crate::expr::Block, name: &str) -> bool {
	use crate::expr::visit::{Visit as _, Visitor};
	use crate::expr::{Expr, Kind, PrefixOperator};
	/// Stops the walk (`Err`) at the first type that names the enum.
	struct Names<'a>(&'a str);
	impl Visitor for Names<'_> {
		type Error = ();
		fn visit_expr(&mut self, expr: &Expr) -> Result<(), ()> {
			// The walk reaches no cast's type of its own.
			if let Expr::Prefix {
				op: PrefixOperator::Cast(kind),
				..
			} = expr
			{
				self.visit_kind(kind)?;
			}
			expr.visit(self)
		}
		fn visit_kind(&mut self, kind: &Kind) -> Result<(), ()> {
			if kind.references_enum(self.0) {
				return Err(());
			}
			Ok(())
		}
	}
	Names(name).visit_block(block).is_err()
}

/// The refusal of a statement over the domain or composite type `name`: that it already exists
/// (`exists`), or that it does not. Worded as PostgreSQL words them, which the SQL wires map to the
/// codes `42710` and `42704`.
pub(super) fn user_type_error(name: &str, exists: bool) -> anyhow::Error {
	let message = if exists {
		format!("type \"{name}\" already exists")
	} else {
		format!("type \"{name}\" does not exist")
	};
	anyhow::Error::new(crate::exec::Error::InvalidStatement(message))
}

/// Refuses the removal of the domain or composite type `definition`, named `name`, of the database
/// `ns`/`db`: when it is not of the kind the statement names (a domain when `domain`), and while a
/// field of the database declares its PostgreSQL SQL type with it. Worded as PostgreSQL words the
/// refusals, which the SQL wires map to the codes `42809` and `2BP01`.
pub(super) async fn check_user_type_removable(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
	definition: &crate::catalog::UserTypeDefinition,
	domain: bool,
) -> anyhow::Result<()> {
	use crate::catalog::providers::TableProvider;
	if domain && definition.domain().is_none() {
		anyhow::bail!(crate::exec::Error::InvalidStatement(format!("\"{name}\" is not a domain")));
	}
	if !domain && definition.composite().is_none() {
		anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
			"\"{name}\" is not a composite type"
		)));
	}
	crate::exec::vm::descriptor::schema(async {
		for table in txn.all_tb(ns, db, None).await?.iter() {
			for field in txn.all_tb_fields(ns, db, &table.name, None).await?.iter() {
				if field.sql_type.as_ref().is_some_and(|sql_type| declares_user_type(sql_type, name))
				{
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"cannot drop type {name} because other objects depend on it: column {} of table {} depends on type {name}",
						surrealdb_types::ToSql::to_sql(&field.name),
						table.name
					)));
				}
			}
		}
		Ok(())
	})
	.await
}

/// Whether the declared SQL type `sql_type` is the PostgreSQL user-defined type `name`, or an
/// array of it. A user-defined type's name is its declared type's whole base, compared as
/// PostgreSQL folds an unquoted name.
pub(crate) fn declares_user_type(
	sql_type: &crate::expr::statements::define::SqlType,
	name: &str,
) -> bool {
	use crate::expr::statements::define::SqlTypeDialect;
	use crate::expr::statements::define::declared_type::DeclaredSqlType;
	sql_type.dialect == SqlTypeDialect::PostgreSql
		&& DeclaredSqlType::from_sql_type(sql_type)
			.is_ok_and(|declared| declared.base().eq_ignore_ascii_case(name))
}

/// Checks that every string `value` holds where `kind` names an enum is a label of that enum.
///
/// `value` has already been coerced to `kind`, which admits any string for a named enum, so the
/// labels in the catalog are the only thing left to judge. A union is satisfied by any of its
/// members that holds the value; a collection is judged element by element.
pub(crate) async fn check_named_enums(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &crate::expr::Kind,
	value: &Value,
) -> Result<(), crate::val::value::CoerceError> {
	use crate::catalog::providers::EnumProvider;
	use crate::expr::{EnumRef, Kind};
	let refuse = |into: String| crate::val::value::CoerceError::InvalidKind {
		from: value.clone(),
		into,
	};
	match (kind, value) {
		(Kind::Enum(EnumRef::Named(name)), Value::String(label)) => {
			let definition =
				crate::exec::vm::descriptor::schema(txn.get_db_enum(ns, db, name, None))
					.await
					.map_err(|e| refuse(format!("enum<{name}> ({e})")))?
					.ok_or_else(|| refuse(format!("enum<{name}>, which does not exist")))?;
			if definition.contains(label.as_str()) {
				Ok(())
			} else {
				Err(refuse(format!("enum<{name}>, one of {}", valid_labels(&definition))))
			}
		}
		(Kind::Either(kinds), _) => {
			let mut last = None;
			for member in kinds.iter().filter(|k| value.can_coerce_to_kind(k)) {
				match Box::pin(check_named_enums(txn, ns, db, member, value)).await {
					Ok(()) => return Ok(()),
					Err(error) => last = Some(error),
				}
			}
			match last {
				Some(error) => Err(error),
				None => Ok(()),
			}
		}
		(Kind::Array(inner, _) | Kind::Set(inner, _), Value::Array(items)) => {
			for item in items.iter() {
				Box::pin(check_named_enums(txn, ns, db, inner, item)).await?;
			}
			Ok(())
		}
		(Kind::Array(inner, _) | Kind::Set(inner, _), Value::Set(items)) => {
			for item in items.iter() {
				Box::pin(check_named_enums(txn, ns, db, inner, item)).await?;
			}
			Ok(())
		}
		_ => Ok(()),
	}
}

/// Judges `value`, already coerced or cast to `kind`, against the labels of every named enum the
/// kind holds, where `ctx` reaches the catalog. A kind that names no enum, or a context with no
/// database selected beside a kind that does, is judged by its coercion alone.
pub(crate) async fn check_enums_held(
	ctx: &crate::exec::ExecutionContext,
	kind: &crate::expr::Kind,
	value: &Value,
) -> Result<(), crate::val::value::CoerceError> {
	if !kind.has_named_enum() {
		return Ok(());
	}
	let Ok(database) = ctx.database() else {
		return Ok(());
	};
	check_named_enums(
		&ctx.txn(),
		database.ns_ctx.ns.namespace_id,
		database.db.database_id,
		kind,
		value,
	)
	.await
}

#[cfg(test)]
mod tests {
	use super::{block_names_enum, composed_renames};
	use crate::exec::ir::iface::statements::EnumEdit;

	fn rename(from: &str, to: &str) -> EnumEdit {
		EnumEdit::Rename {
			from: from.to_owned(),
			to: to.to_owned(),
		}
	}

	fn add(value: &str, if_not_exists: bool) -> EnumEdit {
		EnumEdit::Add {
			value: value.to_owned(),
			anchor: None,
			after: false,
			if_not_exists,
		}
	}

	fn pairs(edits: &[EnumEdit]) -> Vec<(String, String)> {
		composed_renames(edits)
	}

	fn owned(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
		pairs.iter().map(|(a, b)| ((*a).to_owned(), (*b).to_owned())).collect()
	}

	#[test]
	fn renames_compose_in_order() {
		assert_eq!(pairs(&[rename("a", "b"), rename("b", "c")]), owned(&[("a", "c")]));
		assert_eq!(pairs(&[rename("a", "b"), rename("b", "a")]), owned(&[]));
		assert_eq!(
			pairs(&[rename("a", "t"), rename("b", "a"), rename("t", "b")]),
			owned(&[("a", "b"), ("b", "a")])
		);
	}

	#[test]
	fn a_label_added_in_the_list_renames_no_stored_value() {
		assert_eq!(
			pairs(&[rename("a", "b"), add("a", false), rename("a", "c")]),
			owned(&[("a", "b")])
		);
		assert_eq!(pairs(&[add("x", false), rename("x", "y"), rename("y", "z")]), owned(&[]));
		// Renamed away, so an `IF NOT EXISTS` add introduces it.
		assert_eq!(
			pairs(&[rename("a", "b"), add("a", true), rename("a", "c")]),
			owned(&[("a", "b")])
		);
		// Held under its new spelling, so the add is skipped and the rename is of stored values.
		assert_eq!(
			pairs(&[rename("a", "b"), add("b", true), rename("b", "c")]),
			owned(&[("a", "c")])
		);
		// Not named before: possibly a stored label, so its rename rewrites stored values.
		assert_eq!(pairs(&[add("a", true), rename("a", "c")]), owned(&[("a", "c")]));
	}

	#[test]
	fn a_function_body_names_an_enum_only_through_a_type() {
		let names = |text: &str| block_names_enum(&crate::syn::block(text).unwrap(), "foo");
		assert!(!names("{ RETURN 'enum<foo>' }"));
		assert!(!names("{ RETURN \"enum<foo>\" }"));
		assert!(!names("{ RETURN <enum<bar>> $x }"));
		assert!(names("{ RETURN <enum<foo>> $x }"));
		assert!(names("{ RETURN <option<array<enum<foo>>>> $x }"));
		assert!(names("{ LET $x: enum<foo> = 'a'; RETURN $x }"));
		assert!(names("{ RETURN |$v: enum<foo>| $v }"));
		assert!(names("{ RETURN |$v| -> enum<foo> { $v } }"));
		assert!(names("{ IF true { RETURN [1, <enum<foo>> $x] } }"));
	}
}
