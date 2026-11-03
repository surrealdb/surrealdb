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
	let names = field_named_types(definition);
	// The types the field names are judged only where it is defined: an `IF NOT EXISTS` that meets
	// the field stores nothing.
	let judged = !names.is_empty()
		&& (!matches!(kind, DefineKind::IfNotExists)
			|| !field_exists(definition, ctx, opt).await?);
	if judged {
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		let subject = format!(
			"The field '{}' on the table '{}'",
			definition.name.to_raw_string(),
			definition.table.as_str()
		);
		check_types_defined(&ctx.tx(), ns, db, &names, &subject).await?;
	}
	crate::exec::vm::descriptor::schema(crate::ddl::define::field::define_field_definition_apply(
		kind, definition, migration, ctx, opt,
	))
	.await?;
	Ok(())
}

/// Whether the field `definition` defines is already defined on its table.
async fn field_exists(
	definition: &crate::catalog::FieldDefinition,
	ctx: &FrozenContext,
	opt: &Options,
) -> anyhow::Result<bool> {
	use crate::catalog::providers::TableProvider;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	let fd = crate::catalog::FieldDefinition::storage_name(&definition.name);
	let txn = ctx.tx();
	crate::exec::vm::descriptor::schema(async {
		Ok(txn.get_tb_field(ns, db, &definition.table, &fd, None).await?.is_some())
	})
	.await
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

/// Refuses the aggregated view `view` of the database `ns`, `db` when it keeps the `MIN` or
/// `MAX` of a field of a source table that holds the labels of a union type. Such a field orders
/// by declaration, not by number, and the view keeps its extremes as numbers.
pub(super) async fn refuse_union_extremes(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	view: &TableName,
	definition: &crate::catalog::ViewDefinition,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(async {
		use crate::catalog::aggregation::Aggregation;
		use crate::catalog::providers::TableProvider;
		let crate::catalog::ViewDefinition::Aggregated {
			analysis,
			tables,
			..
		} = definition
		else {
			return Ok(());
		};
		let types = types_by_name(txn, ns, db).await?;
		for aggregation in &analysis.aggregations {
			let (arg, function) = match aggregation {
				Aggregation::NumberMax(arg) => (*arg, "math::max"),
				Aggregation::NumberMin(arg) => (*arg, "math::min"),
				Aggregation::Count
				| Aggregation::CountValue(_)
				| Aggregation::Sum(_)
				| Aggregation::Mean(_)
				| Aggregation::StdDev(_)
				| Aggregation::Variance(_)
				| Aggregation::DatetimeMax(_)
				| Aggregation::DatetimeMin(_)
				| Aggregation::Accumulate(_) => continue,
			};
			let Some(crate::expr::Expr::Idiom(field)) = analysis.aggregate_arguments.get(arg)
			else {
				continue;
			};
			for table in tables {
				let fields = txn.all_tb_fields(ns, db, table, None).await?;
				let Some(kind) =
					fields.iter().find(|f| &f.name == field).and_then(|f| f.field_kind.as_ref())
				else {
					continue;
				};
				let mut members: Vec<&crate::expr::Kind> = vec![kind];
				if let crate::expr::Kind::Either(kinds) = kind {
					members = kinds.iter().collect();
				}
				let mut holds_labels = false;
				for member in members {
					if let crate::expr::Kind::Enum(_) = member {
						holds_labels = true;
					}
					// A union type, or an alias or a domain of one.
					if let crate::expr::Kind::Named(name) = member
						&& type_named(&types, name)
							.is_some_and(|ty| ty.union_behind(&|n| type_named(&types, n)).is_some())
					{
						holds_labels = true;
					}
				}
				if holds_labels {
					let field = field.to_raw_string();
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"The view '{view}' cannot keep {function}({field}): the field '{field}' on the \
						 table '{table}' holds the labels of a union type, which order by declaration \
						 and not by number"
					)));
				}
			}
		}
		Ok(())
	})
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

/// Applies the edits of an `ALTER TYPE` to the labels of `definition`, in order.
///
/// A type that is not a union of labels is refused. An edit that names a label the type does not
/// hold, or adds one it does, is refused with the type named in the message; an `IF NOT EXISTS`
/// add of a label the type holds is skipped.
pub(super) fn edit_type(
	definition: &mut crate::catalog::TypeDefinition,
	edits: &[crate::exec::ir::iface::statements::EnumEdit],
) -> anyhow::Result<()> {
	use crate::catalog::{EnumEditError, EnumLabels, EnumPlacement};
	use crate::exec::ir::iface::statements::EnumEdit;
	let name = definition.name.clone();
	let Some(labels) = definition.union_mut() else {
		anyhow::bail!(
			"The type '{name}' is not a union of string labels, so it has no values to alter"
		);
	};
	let refused = |labels: &EnumLabels, error: EnumEditError| -> anyhow::Error {
		match error {
			EnumEditError::Exists(label) => {
				anyhow::anyhow!("The type '{name}' already has the value '{label}'")
			}
			EnumEditError::Missing(label) => anyhow::anyhow!(
				"The type '{name}' has no value '{label}'; its values are {}",
				valid_labels(labels)
			),
			EnumEditError::Full => anyhow::anyhow!(
				"The type '{name}' cannot take another value: a union holds at most {} values over \
				 its life",
				crate::catalog::ENUM_LABEL_IDENT_LIMIT - 1
			),
		}
	};
	for edit in edits {
		match edit {
			EnumEdit::Add {
				value,
				anchor,
				after,
				if_not_exists,
			} => {
				if *if_not_exists && labels.contains(value) {
					continue;
				}
				let placement = match (anchor.as_deref(), after) {
					(Some(anchor), false) => EnumPlacement::Before(anchor),
					(Some(anchor), true) => EnumPlacement::After(anchor),
					(None, _) => EnumPlacement::Last,
				};
				if let Err(e) = labels.add_label(value, placement) {
					return Err(refused(labels, e));
				}
			}
			EnumEdit::Rename {
				from,
				to,
			} => {
				if let Err(e) = labels.rename_label(from, to) {
					return Err(refused(labels, e));
				}
			}
		}
	}
	Ok(())
}

/// The labels of `labels` as an error lists them.
fn valid_labels(labels: &crate::catalog::EnumLabels) -> String {
	labels.label_names().map(|label| format!("'{label}'")).collect::<Vec<_>>().join(", ")
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

/// The types of the database `ns`/`db`.
async fn types_by_name(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
) -> anyhow::Result<std::sync::Arc<[crate::catalog::TypeDefinition]>> {
	use crate::catalog::providers::TypeProvider;
	txn.all_db_types(ns, db, None).await
}

/// The definition of the type `name` among `types`.
fn type_named<'a>(
	types: &'a [crate::catalog::TypeDefinition],
	name: &str,
) -> Option<&'a crate::catalog::TypeDefinition> {
	types.iter().find(|definition| definition.name.as_str() == name)
}

/// `kind` with each type it names, other than the type `name`, whose body holds the type `name`
/// replaced by that body, itself read the same way through the types it names. A value of the
/// result holds the type `name` wherever a value of `kind` does, and there it is the type `name`
/// itself, so a field typed through an alias or a domain of a union is reached as one typed by
/// the union. `types` are the types of the database.
fn through_types(
	kind: &crate::expr::Kind,
	name: &str,
	types: &[crate::catalog::TypeDefinition],
) -> crate::expr::Kind {
	fn expand(
		kind: &crate::expr::Kind,
		name: &str,
		types: &[crate::catalog::TypeDefinition],
		depth: usize,
	) -> crate::expr::Kind {
		kind.replace_named_types(&mut |named| {
			if named == name || depth == 0 {
				return None;
			}
			let crate::catalog::TypeBody::Kind(body) = &type_named(types, named)?.body else {
				return None;
			};
			let expanded = expand(body, name, types, depth - 1);
			expanded.references_type(name).then_some(expanded)
		})
	}
	expand(kind, name, types, crate::catalog::UNION_ALIAS_DEPTH)
}

/// The text of an expression that reads the enum labels `expr` holds as `kind` declares them, with
/// the labels `renames` names replaced, or why the kind holds the enum where no rewrite reaches.
fn renamed_labels(
	kind: &crate::expr::Kind,
	expr: &str,
	enum_name: &str,
	renames: &[(String, String)],
) -> anyhow::Result<String> {
	use crate::expr::Kind;
	let quoted = |label: &str| surrealdb_types::ToSql::to_sql(&Value::from(label.to_owned()));
	if let Kind::Named(name) = kind
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
		for member in kinds.iter().filter(|member| member.references_type(enum_name)) {
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
		"the type '{enum_name}' is named inside the type `{}`, which a rename does not reach",
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
	use crate::expr::Kind;
	let labels = renames
		.iter()
		.map(|(before, _)| surrealdb_types::ToSql::to_sql(&Value::from(before.clone())))
		.collect::<Vec<_>>()
		.join(", ");
	if let Kind::Named(name) = kind {
		return (name == enum_name).then(|| format!("{expr} IN [{labels}]"));
	}
	if let Kind::Either(kinds) = kind {
		let tests: Option<Vec<String>> = kinds
			.iter()
			.filter(|member| member.references_type(enum_name))
			.map(|member| holds_renamed_label(member, expr, renames, enum_name))
			.collect();
		return tests.map(|tests| tests.join(" OR "));
	}
	if let Kind::Array(inner, _) | Kind::Set(inner, _) = kind {
		// An element holds a label at the first level when it is the enum, or a union whose only
		// members naming the enum are the enum itself (`option<enum<e>>`). A member that holds the
		// enum deeper (`array<enum<e>>`, an object) is beyond what `CONTAINSANY` tests.
		let is_enum = |k: &Kind| matches!(k, Kind::Named(name) if name == enum_name);
		let element = if let Kind::Either(members) = &**inner {
			members.iter().filter(|member| member.references_type(enum_name)).all(is_enum)
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
/// names the enum where a rename does not reach (an object or a tuple type), and a table whose
/// `id` field, or a field at a path inside `id`, holds the enum (through a tuple, an alias or a
/// domain too), refuse the `ALTER` rather than leave records that fail their next write.
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
		let types = types_by_name(&txn, ns, db).await?;
		for table in txn.all_tb(ns, db, None).await?.iter() {
			// A view recomputes from its sources, which are rewritten themselves.
			if table.view.is_some() {
				continue;
			}
			let mut assigned: Vec<(String, String)> = Vec::new();
			let mut holders: Vec<String> = Vec::new();
			for field in txn.all_tb_fields(ns, db, &table.name, None).await?.iter() {
				let Some(declared) = &field.field_kind else {
					continue;
				};
				// A field typed through an alias or a domain of the enum holds its labels too.
				let kind = &through_types(declared, name, &types);
				// The components of a record id are the `id` field's, judged with it.
				if field.is_key_part() || !kind.references_type(name) {
					continue;
				}
				// A record id is the record's key, which an `UPDATE` does not rewrite: a key
				// holding a renamed label, as the `id` field or at a path inside it, would hold a
				// label the type no longer has.
				if field.name.is_id() || field.name.is_id_subpath() {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot rename a value of the type '{name}': the record ids of the table '{}' hold it, and a rename does not rewrite a record id",
						table.name
					)));
				}
				let path = surrealdb_types::ToSql::to_sql(&field.name);
				let (target, element) = match path.strip_suffix("[*]").or(path.strip_suffix(".*")) {
					Some(base) => (base.to_owned(), true),
					None => (path, false),
				};
				if target.contains("[*]") || target.contains(".*") {
					anyhow::bail!(
						"the type '{name}' is held by the field '{}' on the table '{}', whose path a rename does not reach",
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
				anyhow::bail!("the rewrite of the labels of the type '{name}' is not a query")
			}
		}
	}
	let frame = ctx
		.with_perms_suppressed(true)
		.with_params([crate::exec::materialize::mark(), (RELABEL, RELABEL_TOKEN.clone())]);
	crate::exec::function::run_statements(&frame, 1, expressions).await?;
	Ok(())
}

/// Refuses `replacement`, about to overwrite the type of its name in the database `ns`/`db`,
/// while a stored value of a field whose kind holds the type is one the replacement does not
/// admit. Each value is judged as a write of it is once the replacement is stored: coerced to the
/// field's kind, then held to every named type the kind holds ([`check_named_types`]), with
/// `replacement` standing in for the stored type of its name. It runs before the replacement is
/// stored, so a refused statement leaves the stored definition as it was.
///
/// A field holds the type when its kind names it, or names a type whose body holds it. A field
/// under a wildcard (`map.*`, `items[*].n`, `xs[*]`) judges each value its path reaches in a
/// stored record, and a path that reaches no value judges none. A field at `id`, or at a path
/// inside it, judges the record's key rather than its record id. A view is skipped, as it
/// recomputes from its sources. The records are read in metered batches.
pub(super) async fn validate_type_held(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	replacement: &crate::catalog::TypeDefinition,
) -> anyhow::Result<()> {
	use crate::catalog::providers::TableProvider;
	let name = replacement.name.as_str();
	let txn = ctx.txn();
	let held = crate::exec::vm::descriptor::schema(async {
		let types = types_by_name(&txn, ns, db).await?;
		let mut held: Vec<(TableName, Vec<(Idiom, crate::expr::Kind)>)> = Vec::new();
		for table in txn.all_tb(ns, db, None).await?.iter() {
			if table.view.is_some() {
				continue;
			}
			let fields: Vec<(Idiom, crate::expr::Kind)> = txn
				.all_tb_fields(ns, db, &table.name, None)
				.await?
				.iter()
				.filter_map(|field| {
					let kind = field.field_kind.as_ref()?;
					kind_holds_type(kind, name, &types, 0)
						.then(|| (field.name.clone(), kind.clone()))
				})
				.collect();
			if !fields.is_empty() {
				held.push((table.name.clone(), fields));
			}
		}
		Ok::<_, anyhow::Error>(held)
	})
	.await?;
	let frame = ctx.with_perms_suppressed(true);
	for (table, fields) in held {
		validate_table_held(&frame, ns, db, &table, &fields, replacement).await?;
	}
	Ok(())
}

/// Whether `kind` holds the type `name`: names it, or names a type of `types` whose body holds it.
fn kind_holds_type(
	kind: &crate::expr::Kind,
	name: &str,
	types: &[crate::catalog::TypeDefinition],
	depth: usize,
) -> bool {
	if depth > crate::catalog::UNION_ALIAS_DEPTH {
		return false;
	}
	let mut holds = false;
	kind.each_named_type(&mut |named| {
		holds |= named == name
			|| type_named(types, named)
				.is_some_and(|other| kind_holds_type(&other.kind(), name, types, depth + 1));
	});
	holds
}

/// [`validate_type_held`] for the records of `table`, whose `fields` hold the type `replacement`
/// replaces.
async fn validate_table_held(
	ctx: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
	fields: &[(Idiom, crate::expr::Kind)],
	replacement: &crate::catalog::TypeDefinition,
) -> anyhow::Result<()> {
	use crate::key::{KVKeyDecode, KVSubspace};
	let name = replacement.name.as_str();
	let txn = ctx.txn();
	let layouts =
		crate::exec::vm::descriptor::schema(txn.table_layouts(ns, db, table, None)).await?;
	let records = crate::key::schema::RecordPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(table),
	};
	// A field at `id` or at a path inside it is judged by the record's key, not by its record id.
	let key_held = fields.iter().any(|(path, _)| path.is_id() || path.is_id_subpath());
	let mut next = Some(records.range()?);
	while let Some(range) = next {
		let size = crate::exec::vm::fuel::batch(crate::kvs::NORMAL_BATCH_SIZE);
		let page = txn.batch_keys_vals_raw(range, size, None).await?;
		next = page.next.map(|range| records.raw(range));
		if page.result.is_empty() {
			break;
		}
		for (key, bytes) in &page.result {
			let rid = crate::val::RecordId {
				table: table.clone(),
				key: crate::key::schema::RecordKey::decode_key(key)?.id.into_owned(),
			};
			let record_key = key_held.then(|| rid.key.clone().into_value());
			let document = crate::catalog::Record::decode_stored(bytes, Some(&layouts), rid)?.data;
			for (path, kind) in fields {
				let mut values = Vec::new();
				match (&record_key, path.0.split_first()) {
					(Some(record_key), Some((_, inside)))
						if path.is_id() || path.is_id_subpath() =>
					{
						held_at(record_key, inside, &mut values);
					}
					(Some(_) | None, Some(_) | None) => held_at(&document, &path.0, &mut values),
				}
				for value in values {
					let judged = match value.coerce_to_kind(kind) {
						Ok(value) => {
							named_types(
								ctx,
								ns,
								db,
								kind,
								value,
								Judge::coerce(Some(replacement)),
								0,
							)
							.await
						}
						Err(error) => Err(error),
					};
					if let Err(error) = judged {
						anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
							"Cannot change the type '{name}': the field '{}' on the table '{table}' holds a value it does not admit: {error}",
							surrealdb_types::ToSql::to_sql(path)
						)));
					}
				}
			}
		}
		crate::exec::vm::fuel::tick(ctx, crate::exec::vm::fuel::units(page.result.len()))
			.await
			.map_err(|flow| {
				flow.into_error_or(|other| {
					anyhow::anyhow!("the scan of the records was interrupted by {other}")
				})
			})?;
	}
	Ok(())
}

/// The values `path` reaches in `value`, pushed onto `out`. A wildcard (`*`) reaches each element
/// of an array or a set and each field of an object, and nothing of any other value; every other
/// part reaches what [`Value::pick`] reads through it, so a path without a wildcard reaches one
/// value, `NONE` where it is absent.
fn held_at(value: &Value, path: &[crate::expr::Part], out: &mut Vec<Value>) {
	let Some((part, rest)) = path.split_first() else {
		out.push(value.clone());
		return;
	};
	if let crate::expr::Part::All = part {
		if let Value::Array(items) = value {
			for item in items.iter() {
				held_at(item, rest, out);
			}
		} else if let Value::Set(items) = value {
			for item in items.iter() {
				held_at(item, rest, out);
			}
		} else if let Value::Object(object) = value {
			for item in object.values() {
				held_at(item, rest, out);
			}
		}
		return;
	}
	held_at(&value.pick(std::slice::from_ref(part)), rest, out);
}

/// Refuses `definition` when it is not the kind of SQL object `object` names: `DROP DOMAIN` of an
/// enum or a composite type, or `DROP TYPE` of a domain.
pub(super) fn check_type_object(
	definition: &crate::catalog::TypeDefinition,
	object: crate::expr::statements::remove::SqlTypeObject,
) -> anyhow::Result<()> {
	use crate::expr::statements::remove::SqlTypeObject;
	let name = &definition.name;
	match (object, definition.is_domain()) {
		(SqlTypeObject::Type, true) => {
			anyhow::bail!("The type '{name}' is a domain; remove it with DROP DOMAIN")
		}
		(SqlTypeObject::Domain, false) => {
			anyhow::bail!("The type '{name}' is not a domain; remove it with DROP TYPE")
		}
		(SqlTypeObject::Type, false) | (SqlTypeObject::Domain, true) => Ok(()),
	}
}

/// Refuses to `action` the type `name` (the verb of the refusal: "remove", "change the values
/// of") while something of the database `ns`/`db` names it: a field's type, clauses or
/// permissions, a table's permissions, view clauses or events, a function's signature, body or
/// permission, a parameter's permission, an API's handlers, fallback or permissions, a database
/// access method's `AUTHENTICATE`, `CONTEXT`, `SIGNUP` or `SIGNIN` clause, an index's `COUNT WHERE`
/// condition, the permission of a database bucket, a model or a module, the permission of the
/// database's API config, or another type. A field's clauses include its `REFERENCE`.
pub(super) async fn check_type_unreferenced(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	name: &str,
	action: &str,
) -> anyhow::Result<()> {
	use crate::catalog::providers::{
		ApiProvider, AuthorisationProvider, BucketProvider, DatabaseProvider, TableProvider,
		TypeProvider,
	};
	crate::exec::vm::descriptor::schema(async {
		for table in txn.all_tb(ns, db, None).await?.iter() {
			if let Some(view) = &table.view {
				let mut found = false;
				view.each_named_type(&mut |named| found |= named == name);
				if found {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot {action} the type '{name}': the view '{}' names it",
						table.name
					)));
				}
			}
			let permissions = &table.permissions;
			let table_permissions = [
				&permissions.select,
				&permissions.create,
				&permissions.update,
				&permissions.delete,
			];
			if table_permissions.into_iter().any(|permission| permission_names_type(permission, name)) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': a permission of the table '{}' names it",
					table.name
				)));
			}
			for event in txn.all_tb_events(ns, db, &table.name, None).await?.iter() {
				if expr_names_type(&event.when, name)
					|| event.then.iter().any(|expr| expr_names_type(expr, name))
				{
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot {action} the type '{name}': the event '{}' on the table '{}' names it",
						event.name, table.name
					)));
				}
			}
			for index in txn.all_tb_indexes(ns, db, &table.name, None).await?.iter() {
				if index_named_types(index).iter().any(|named| named == name) {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot {action} the type '{name}': the COUNT WHERE condition of the index '{}' on the table '{}' names it",
						index.name, table.name
					)));
				}
			}
			for field in txn.all_tb_fields(ns, db, &table.name, None).await?.iter() {
				if field.field_kind.as_ref().is_some_and(|kind| kind.references_type(name)) {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot {action} the type '{name}': the field '{}' on the table '{}' has a type that names it",
						surrealdb_types::ToSql::to_sql(&field.name),
						table.name
					)));
				}
				let default = match &field.default {
					surrealdb_catalog::DefineDefault::None => None,
					surrealdb_catalog::DefineDefault::Always(expr)
					| surrealdb_catalog::DefineDefault::Set(expr) => Some(expr),
				};
				let clauses = [default, field.value.as_ref(), field.assert.as_ref(), field.computed.as_ref()];
				let field_permissions =
					[&field.select_permission, &field.create_permission, &field.update_permission];
				if clauses
					.into_iter()
					.flatten()
					.chain(reference_exprs(field.reference.as_ref()))
					.any(|expr| expr_names_type(expr, name))
					|| field_permissions
						.into_iter()
						.any(|permission| permission_names_type(permission, name))
				{
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot {action} the type '{name}': an expression of the field '{}' on the table '{}' names it",
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
				.any(|kind| kind.references_type(name));
			if signature
				|| block_names_type(&function.block, name)
				|| permission_names_type(&function.permissions, name)
			{
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the function 'fn::{}' names it",
					function.name
				)));
			}
		}
		for param in txn.all_db_params(ns, db, None).await?.iter() {
			if permission_names_type(&param.permissions, name) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the permission of the parameter '${}' names it",
					param.name
				)));
			}
		}
		for api in txn.all_db_apis(ns, db, None).await?.iter() {
			let named = api.actions.iter().any(|handler| {
				expr_names_type(&handler.action, name)
					|| permission_names_type(&handler.config.permissions, name)
			}) || api.fallback.as_ref().is_some_and(|fallback| expr_names_type(fallback, name))
				|| permission_names_type(&api.config.permissions, name);
			if named {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the API '{}' names it",
					api.path
				)));
			}
		}
		for access in txn.all_db_accesses(ns, db, None).await?.iter() {
			let clauses = [
				("AUTHENTICATE", &access.authenticate),
				("CONTEXT", &access.context),
				("SIGNUP", &access.signup),
				("SIGNIN", &access.signin),
			];
			for (clause, expr) in clauses {
				if expr.as_ref().is_some_and(|expr| expr_names_type(expr, name)) {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"Cannot {action} the type '{name}': the {clause} clause of the access method '{}' names it",
						access.name
					)));
				}
			}
		}
		for bucket in txn.all_buckets(crate::catalog::BucketLevel::Db(ns, db), None).await?.iter() {
			if permission_names_type(&bucket.permissions, name) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the permission of the bucket '{}' names it",
					bucket.name
				)));
			}
		}
		for config in txn.all_db_configs(ns, db, None).await?.iter() {
			if config_named_types(config).iter().any(|named| named == name) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the permission of the API config names it"
				)));
			}
		}
		for model in txn.all_db_models(ns, db, None).await?.iter() {
			if permission_names_type(&model.permissions, name) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the permission of the model 'ml::{}<{}>' names it",
					model.name, model.version
				)));
			}
		}
		for module in txn.all_db_modules(ns, db, None).await?.iter() {
			if permission_names_type(&module.permissions, name) {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the permission of the module '{}' names it",
					module.get_storage_name()?
				)));
			}
		}
		for other in txn.all_db_types(ns, db, None).await?.iter() {
			if other.name.as_str() != name
				&& (other.referenced_types().iter().any(|n| n == name)
					|| permission_names_type(&other.permissions, name))
			{
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"Cannot {action} the type '{name}': the type '{}' names it",
					other.name
				)));
			}
		}
		Ok(())
	})
	.await
}

/// Reconciles `replacement`, a definition about to overwrite the stored type `existing` of the
/// database `ns`/`db`, when `existing` is a union of labels. Listing the same labels in the same
/// order keeps the stored labels, so each label's sort key and identity (and the PostgreSQL OID
/// derived from it) stay its own. Anything else is refused while a field, a function or another
/// type of the database names the type, because stored values may hold a label the replacement
/// drops and a cast may name one: `ALTER TYPE` adds and renames values in place. A type nothing
/// names takes the replacement, each label it keeps holding its identity and each new one taking
/// the next one free. A type that is not a union is replaced as it is given.
pub(super) async fn reconcile_type_overwrite(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	existing: &crate::catalog::TypeDefinition,
	replacement: &mut crate::catalog::TypeDefinition,
) -> anyhow::Result<()> {
	use crate::catalog::ENUM_LABEL_IDENT_LIMIT;
	let Some(held) = existing.union() else {
		return Ok(());
	};
	let name = existing.name.as_str();
	if replacement.union().is_some_and(|labels| held.label_names().eq(labels.label_names())) {
		if let Some(labels) = replacement.union_mut() {
			labels.labels.clone_from(&held.labels);
		}
		return Ok(());
	}
	check_type_unreferenced(txn, ns, db, name, "change the values of").await?;
	let Some(labels) = replacement.union_mut() else {
		return Ok(());
	};
	let mut next = held.next_ident();
	for label in &mut labels.labels {
		label.ident = match held.labels.iter().find(|kept| kept.label == label.label) {
			Some(kept) => kept.ident,
			None => {
				anyhow::ensure!(
					next < ENUM_LABEL_IDENT_LIMIT,
					"The type '{name}' cannot take another value: a union holds at most {} values over its life",
					ENUM_LABEL_IDENT_LIMIT - 1
				);
				next += 1;
				next - 1
			}
		};
	}
	Ok(())
}

/// Refuses the definition of `subject` (a field or a function of the database `ns`/`db`) when
/// one of `names`, the types its kinds name, is not a type the database defines.
pub(crate) async fn check_types_defined(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	names: &[String],
	subject: &str,
) -> anyhow::Result<()> {
	use crate::catalog::providers::TypeProvider;
	crate::exec::vm::descriptor::schema(async {
		for name in names {
			if txn.get_db_type(ns, db, name, None).await?.is_none() {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"{subject} cannot be defined: the type '{name}' it names does not exist"
				)));
			}
		}
		Ok(())
	})
	.await
}

/// The names of the types a definition names, each once, in the order they are first named.
#[derive(Default)]
struct NamedTypes(Vec<String>);

impl NamedTypes {
	fn name(&mut self, name: &str) {
		if !self.0.iter().any(|n| n == name) {
			self.0.push(name.to_owned());
		}
	}

	fn kind(&mut self, kind: &crate::expr::Kind) {
		kind.each_named_type(&mut |name| self.name(name));
	}

	fn expr(&mut self, expr: &crate::expr::Expr) {
		crate::expr::visit::each_named_type_in_expr(expr, &mut |name| self.name(name));
	}

	fn permission(&mut self, permission: &crate::catalog::Permission) {
		match permission {
			crate::catalog::Permission::None | crate::catalog::Permission::Full => {}
			crate::catalog::Permission::Specific(expr) => self.expr(expr),
		}
	}

	fn permissions(&mut self, permissions: &crate::catalog::Permissions) {
		for permission in
			[&permissions.select, &permissions.create, &permissions.update, &permissions.delete]
		{
			self.permission(permission);
		}
	}
}

/// The expressions of a field's `REFERENCE` clause: its `ON DELETE THEN` body and the `WHERE`
/// filter of its `MATERIALIZE`.
fn reference_exprs(
	reference: Option<&crate::expr::reference::Reference>,
) -> impl Iterator<Item = &crate::expr::Expr> {
	use crate::expr::reference::ReferenceDeleteStrategy;
	reference.into_iter().flat_map(|reference| {
		let on_delete = match &reference.on_delete {
			ReferenceDeleteStrategy::Custom(expr) => Some(expr),
			ReferenceDeleteStrategy::Reject
			| ReferenceDeleteStrategy::Ignore
			| ReferenceDeleteStrategy::Cascade
			| ReferenceDeleteStrategy::Unset => None,
		};
		let filter = reference.materialize.as_ref().and_then(|m| m.filter.as_ref());
		on_delete.into_iter().chain(filter)
	})
}

/// The names of the types a field definition names, each once: in its kind, in the types
/// written in its `DEFAULT`, `VALUE`, `ASSERT`, `COMPUTED` and `REFERENCE` clauses, and in its
/// permissions.
pub(crate) fn field_named_types(definition: &crate::catalog::FieldDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	if let Some(kind) = &definition.field_kind {
		names.kind(kind);
	}
	let default = match &definition.default {
		surrealdb_catalog::DefineDefault::None => None,
		surrealdb_catalog::DefineDefault::Always(expr)
		| surrealdb_catalog::DefineDefault::Set(expr) => Some(expr),
	};
	let clauses = [
		default,
		definition.value.as_ref(),
		definition.assert.as_ref(),
		definition.computed.as_ref(),
	];
	for expr in clauses.into_iter().flatten().chain(reference_exprs(definition.reference.as_ref()))
	{
		names.expr(expr);
	}
	for permission in [
		&definition.select_permission,
		&definition.create_permission,
		&definition.update_permission,
	] {
		names.permission(permission);
	}
	names.0
}

/// The names of the types a function definition names, each once: in the kinds of its
/// arguments and its return, in the types written in its body, and in its permission.
pub(crate) fn function_named_types(definition: &crate::catalog::FunctionDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	for kind in definition.args.iter().map(|(_, kind)| kind).chain(definition.returns.as_ref()) {
		names.kind(kind);
	}
	crate::expr::visit::each_named_type_in_block(&definition.block, &mut |name| names.name(name));
	names.permission(&definition.permissions);
	names.0
}

/// The names of the types an event definition names, each once: in its `WHEN` and `THEN`.
pub(crate) fn event_named_types(definition: &crate::catalog::EventDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	for expr in std::iter::once(&definition.when).chain(&definition.then) {
		names.expr(expr);
	}
	names.0
}

/// The names of the types a table definition names, each once: in its view and its permissions.
pub(crate) fn table_named_types(definition: &crate::catalog::TableDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	if let Some(view) = &definition.view {
		view.each_named_type(&mut |name| names.name(name));
	}
	names.permissions(&definition.permissions);
	names.0
}

/// The names of the types an index definition names, each once: in its `COUNT WHERE` condition.
pub(crate) fn index_named_types(definition: &crate::catalog::IndexDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	if let Some(cond) = &definition.count_cond {
		names.expr(&cond.0);
	}
	names.0
}

/// The names of the types a permission names, each once: a parameter's, a bucket's, a model's
/// or a module's.
pub(crate) fn permission_named_types(permission: &crate::catalog::Permission) -> Vec<String> {
	let mut names = NamedTypes::default();
	names.permission(permission);
	names.0
}

/// The names of the types an API definition names, each once: in its handlers, their
/// permissions, its fallback and its permission.
pub(crate) fn api_named_types(definition: &crate::catalog::ApiDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	for action in &definition.actions {
		names.expr(&action.action);
		names.permission(&action.config.permissions);
	}
	if let Some(fallback) = &definition.fallback {
		names.expr(fallback);
	}
	names.permission(&definition.config.permissions);
	names.0
}

/// The names of the types an access method names, each once: in its `AUTHENTICATE`, `CONTEXT`,
/// `SIGNUP` and `SIGNIN` clauses.
pub(crate) fn access_named_types(definition: &crate::catalog::AccessDefinition) -> Vec<String> {
	let mut names = NamedTypes::default();
	for expr in
		[&definition.authenticate, &definition.context, &definition.signup, &definition.signin]
			.into_iter()
			.flatten()
	{
		names.expr(expr);
	}
	names.0
}

/// The names of the types a config definition names, each once: in the permission of an API
/// config.
pub(crate) fn config_named_types(definition: &crate::catalog::ConfigDefinition) -> Vec<String> {
	match definition {
		crate::catalog::ConfigDefinition::Api(api) => permission_named_types(&api.permissions),
		crate::catalog::ConfigDefinition::Default(_)
		| crate::catalog::ConfigDefinition::GraphQL(_) => Vec::new(),
	}
}

/// Refuses the definition of a type in the database `ns`/`db` whose body names a type the
/// database does not define, or a type whose definition leads back to the one defined. The
/// database's other types are read as stored; `definition` stands in for a stored type of its
/// name.
pub(super) async fn check_type_definition(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	definition: &crate::catalog::TypeDefinition,
) -> anyhow::Result<()> {
	use crate::catalog::providers::TypeProvider;
	let name = definition.name.as_str();
	// A label's identity is below `ENUM_LABEL_IDENT_LIMIT` and the first is 1.
	if let Some(labels) = definition.union()
		&& labels.labels.len() >= crate::catalog::ENUM_LABEL_IDENT_LIMIT as usize
	{
		anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
			"The type '{name}' cannot be defined: a union holds at most {} labels",
			crate::catalog::ENUM_LABEL_IDENT_LIMIT - 1
		)));
	}
	crate::exec::vm::descriptor::schema(async {
		// Each type reached from the definition's body, with the type that named it.
		let mut pending: Vec<(String, String)> = definition
			.referenced_types()
			.into_iter()
			.map(|reached| (reached, name.to_owned()))
			.collect();
		let mut seen: Vec<String> = Vec::new();
		while let Some((reached, from)) = pending.pop() {
			if reached == name {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"The type '{name}' cannot be defined: its body leads back to itself through the type '{from}'"
				)));
			}
			if seen.contains(&reached) {
				continue;
			}
			let Some(found) = txn.get_db_type(ns, db, &reached, None).await? else {
				anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
					"The type '{name}' cannot be defined: the type '{reached}' it names does not exist"
				)));
			};
			pending
				.extend(found.referenced_types().into_iter().map(|next| (next, reached.clone())));
			seen.push(reached);
		}
		// The types the permission names are judged to exist; a permission is no part of the
		// body, so it may name the type itself and it leads nowhere.
		if let crate::catalog::Permission::Specific(predicate) = &definition.permissions {
			let mut named: Vec<String> = Vec::new();
			crate::expr::visit::each_named_type_in_expr(predicate, &mut |n| {
				if n != name && !named.iter().any(|seen| seen == n) {
					named.push(n.to_owned());
				}
			});
			for reached in named {
				if txn.get_db_type(ns, db, &reached, None).await?.is_none() {
					anyhow::bail!(crate::exec::Error::InvalidStatement(format!(
						"The type '{name}' cannot be defined: the type '{reached}' it names does not exist"
					)));
				}
			}
		}
		Ok(())
	})
	.await
}

/// Whether a type written in `block` names the type `name`: a cast, the type of a `LET`, a
/// closure's parameter or return type, or a type a statement in the block declares. Text that
/// spells the type (a string literal, a comment) does not name it.
fn block_names_type(block: &crate::expr::Block, name: &str) -> bool {
	let mut found = false;
	crate::expr::visit::each_named_type_in_block(block, &mut |named| found |= named == name);
	found
}

/// Whether the predicate of `permission` names the type `name`, as [`expr_names_type`] judges.
fn permission_names_type(permission: &crate::catalog::Permission, name: &str) -> bool {
	match permission {
		crate::catalog::Permission::None | crate::catalog::Permission::Full => false,
		crate::catalog::Permission::Specific(expr) => expr_names_type(expr, name),
	}
}

/// [`block_names_type`] for one expression.
fn expr_names_type(expr: &crate::expr::Expr, name: &str) -> bool {
	let mut found = false;
	crate::expr::visit::each_named_type_in_expr(expr, &mut |named| found |= named == name);
	found
}

/// How deep [`check_named_types`] follows types that name types. A definition that leads back to
/// itself is refused when it is defined, so the bound is reached only by a long chain.
const NAMED_TYPE_DEPTH: u32 = 64;

/// `value`, already coerced to `kind`, judged against every named type `kind` holds in the
/// database `ns`/`db`, and returned as those types coerce it.
///
/// A named type coerces the value to its body and then runs its checks in order, with `$value`
/// bound to the coerced value; a check whose result is not truthy refuses the value, naming the
/// type and the check. A union is satisfied by its first member that accepts the value; a
/// collection and an object literal are judged element by element.
pub(crate) async fn check_named_types(
	exec: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &crate::expr::Kind,
	value: Value,
) -> Result<Value, crate::val::value::CoerceError> {
	named_types(exec, ns, db, kind, value, Judge::coerce(None), 0).await
}

/// How [`named_types`] brings a value to the body of a named type.
#[derive(Clone, Copy)]
enum Convert {
	Coerce,
	Cast,
}

impl Convert {
	/// The refusal of `value` as a value of `kind`, naming the whole kind.
	fn mismatch(value: Value, kind: &crate::expr::Kind) -> crate::val::value::CoerceError {
		crate::val::value::CoerceError::InvalidKind {
			from: value,
			into: surrealdb_types::ToSql::to_sql(kind),
		}
	}

	fn admits(self, value: &Value, kind: &crate::expr::Kind) -> bool {
		match self {
			Convert::Coerce => value.can_coerce_to_kind(kind),
			Convert::Cast => value.can_cast_to_kind(kind),
		}
	}

	fn apply(
		self,
		value: Value,
		kind: &crate::expr::Kind,
	) -> Result<Value, crate::val::value::CoerceError> {
		match self {
			Convert::Coerce => value.coerce_to_kind(kind),
			Convert::Cast => value.clone().cast_to_kind(kind).map_err(|_| {
				crate::val::value::CoerceError::InvalidKind {
					from: value,
					into: surrealdb_types::ToSql::to_sql(kind),
				}
			}),
		}
	}
}

/// How [`named_types`] judges a value: how it brings the value to a type's body, and the
/// definition that stands in for the stored type of its name, which [`validate_type_held`] judges
/// stored values by before it is stored.
#[derive(Clone, Copy)]
struct Judge<'a> {
	convert: Convert,
	stand_in: Option<&'a crate::catalog::TypeDefinition>,
}

impl<'a> Judge<'a> {
	fn coerce(stand_in: Option<&'a crate::catalog::TypeDefinition>) -> Self {
		Judge {
			convert: Convert::Coerce,
			stand_in,
		}
	}
}

async fn named_types(
	exec: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &crate::expr::Kind,
	value: Value,
	how: Judge<'_>,
	depth: u32,
) -> Result<Value, crate::val::value::CoerceError> {
	use crate::catalog::providers::TypeProvider;
	use crate::expr::{Kind, KindLiteral};
	use crate::val::value::CoerceError;
	if !kind.has_named_type() {
		return Ok(value);
	}
	match kind {
		Kind::Named(name) => {
			let refuse = |value: &Value, into: String| CoerceError::InvalidKind {
				from: value.clone(),
				into,
			};
			if depth >= NAMED_TYPE_DEPTH {
				return Err(refuse(&value, format!("{name}, which names types too deeply")));
			}
			let stored;
			let definition = match how.stand_in {
				Some(stand_in) if stand_in.name.as_str() == name.as_str() => stand_in,
				Some(_) | None => {
					let txn = exec.txn();
					stored =
						crate::exec::vm::descriptor::schema(txn.get_db_type(ns, db, name, None))
							.await
							.map_err(|e| refuse(&value, format!("{name} ({e})")))?
							.ok_or_else(|| {
								refuse(&value, format!("{name}, a type which does not exist"))
							})?;
					&*stored
				}
			};
			let body = definition.kind();
			let value = how.convert.apply(value, &body).map_err(|e| {
				if let CoerceError::InvalidKind {
					from,
					..
				} = e
				{
					CoerceError::InvalidKind {
						from,
						into: format!("{name} ({})", surrealdb_types::ToSql::to_sql(&body)),
					}
				} else {
					e
				}
			})?;
			let value = Box::pin(named_types(exec, ns, db, &body, value, how, depth + 1)).await?;
			for assert in &definition.asserts {
				let frame = exec.with_params([("value", value.clone())]);
				let eval = crate::exec::EvalContext::from_exec_ctx(&frame);
				let check = |reason: String| CoerceError::InvalidKind {
					from: value.clone(),
					into: match &assert.name {
						Some(constraint) => format!("{name} (constraint {constraint} {reason})"),
						None => format!(
							"{name} (ASSERT {} {reason})",
							surrealdb_types::ToSql::to_sql(&assert.check)
						),
					},
				};
				match crate::exec::entry::evaluate(&assert.check, &eval).await {
					Ok(result) if result.is_truthy() => {}
					Ok(_) => return Err(check("failed".to_owned())),
					Err(e) => return Err(check(format!("failed with: {e}"))),
				}
			}
			Ok(value)
		}
		Kind::Either(kinds) => {
			// Members are tried in the order coercion selects one
			// ([`Value::union_member`]): those that take the value unchanged before those that
			// convert it, each group in declared order. A member whose named types refuse the
			// value hands it to the next.
			let members = kinds
				.iter()
				.filter(|member| !value.converts_to(member))
				.chain(kinds.iter().filter(|member| value.converts_to(member)));
			// A union of one member beside `NONE` and `NULL` is that member made optional, so its
			// refusal is the member's own, which names the check that failed.
			let present: Vec<&Kind> =
				kinds.iter().filter(|member| !matches!(member, Kind::None | Kind::Null)).collect();
			let mut refused = None;
			for member in members {
				if !member.has_named_type() {
					if how.convert.admits(&value, member) {
						return how.convert.apply(value, member);
					}
					continue;
				}
				let attempt = match how.convert.apply(value.clone(), member) {
					Ok(coerced) => {
						Box::pin(named_types(exec, ns, db, member, coerced, how, depth)).await
					}
					Err(e) => Err(e),
				};
				match attempt {
					Ok(value) => return Ok(value),
					Err(error) => refused = Some(error),
				}
			}
			// No member accepts the value: the refusal names the whole union, as a union of
			// built-in kinds names it.
			match refused {
				Some(error) if present.len() == 1 => Err(error),
				Some(_) | None => Err(Convert::mismatch(value, kind)),
			}
		}
		Kind::Array(inner, _) => {
			if let Value::Array(items) = value {
				let mut out = Vec::with_capacity(items.len());
				for item in items {
					out.push(Box::pin(named_types(exec, ns, db, inner, item, how, depth)).await?);
				}
				Ok(Value::Array(out.into()))
			} else {
				Ok(value)
			}
		}
		Kind::Set(inner, len) => {
			let items: Vec<Value> = if let Value::Array(items) = value {
				items.into_iter().collect()
			} else if let Value::Set(items) = value {
				items.into_iter().collect()
			} else {
				return Ok(value);
			};
			let mut out = crate::val::Set::new();
			for item in items {
				out.insert(Box::pin(named_types(exec, ns, db, inner, item, how, depth)).await?);
			}
			// The named types can bring distinct elements to one value, which the set holds
			// once, so a bounded set is measured again after they are judged.
			if let Some(len) = len
				&& out.len() as u64 != *len
			{
				return Err(CoerceError::InvalidLength {
					into: surrealdb_types::ToSql::to_sql(kind),
					len: out.len(),
				});
			}
			Ok(Value::Set(out))
		}
		Kind::Literal(KindLiteral::Array(kinds)) => {
			if let Value::Array(items) = value {
				let mut out = Vec::with_capacity(items.len());
				let mut positions = kinds.iter();
				for item in items {
					let item = match positions.next() {
						Some(position) => {
							Box::pin(named_types(exec, ns, db, position, item, how, depth)).await?
						}
						None => item,
					};
					out.push(item);
				}
				Ok(Value::Array(out.into()))
			} else {
				Ok(value)
			}
		}
		Kind::Literal(KindLiteral::Object(fields)) => {
			if let Value::Object(mut object) = value {
				for (key, field) in fields.iter() {
					if !field.has_named_type() {
						continue;
					}
					// An absent field is judged as NONE: it may be absent only where its kind,
					// with the named types it holds, admits NONE.
					let held = object.remove(key.as_str()).unwrap_or(Value::None);
					let held = if held.is_none() {
						how.convert.apply(Value::None, field)?
					} else {
						held
					};
					let held = Box::pin(named_types(exec, ns, db, field, held, how, depth)).await?;
					if !held.is_none() {
						object.insert(key.as_str().to_owned(), held);
					}
				}
				Ok(Value::Object(object))
			} else {
				Ok(value)
			}
		}
		Kind::Any
		| Kind::None
		| Kind::Null
		| Kind::Bool
		| Kind::Bytes
		| Kind::Datetime
		| Kind::Date
		| Kind::Time
		| Kind::Timestamp
		| Kind::Interval
		| Kind::Decimal
		| Kind::BigDecimal
		| Kind::Duration
		| Kind::Float
		| Kind::Int
		| Kind::I8
		| Kind::U8
		| Kind::I16
		| Kind::U16
		| Kind::I32
		| Kind::U32
		| Kind::U64
		| Kind::F16
		| Kind::F32
		| Kind::Number
		| Kind::Object
		| Kind::String
		| Kind::Uuid
		| Kind::Regex
		| Kind::Table(_)
		| Kind::Record(_)
		| Kind::Geometry(_)
		| Kind::Function(..)
		| Kind::Range
		| Kind::Literal(_)
		| Kind::File(_)
		| Kind::Enum(_) => Ok(value),
	}
}

/// The value a field of `kind` takes when a write leaves it `NONE` and the field declares no
/// `DEFAULT` of its own: the `DEFAULT` of the type `kind` names, or of the first type along a
/// chain of types whose bodies each name the next. `None` when `kind` is not a named type or no
/// type along the chain declares one.
pub(crate) async fn type_default(
	exec: &crate::exec::ExecutionContext,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &crate::expr::Kind,
) -> anyhow::Result<Option<Value>> {
	use crate::catalog::providers::TypeProvider;
	let txn = exec.txn();
	let mut current = kind.clone();
	for _ in 0..NAMED_TYPE_DEPTH {
		let crate::expr::Kind::Named(name) = &current else {
			return Ok(None);
		};
		let Some(definition) =
			crate::exec::vm::descriptor::schema(txn.get_db_type(ns, db, name, None)).await?
		else {
			return Ok(None);
		};
		if let Some(default) = &definition.default {
			let eval = crate::exec::EvalContext::from_exec_ctx(exec);
			return Ok(Some(crate::exec::entry::evaluate(default, &eval).await?));
		}
		current = definition.kind();
	}
	Ok(None)
}

/// `value`, already coerced or cast to `kind`, judged against every named type the kind holds
/// where `ctx` reaches the catalog, and returned as those types coerce it. A kind that names no
/// type, or a context with no database selected beside a kind that does, leaves the value as its
/// coercion made it.
pub(crate) async fn check_types_held(
	ctx: &crate::exec::ExecutionContext,
	kind: &crate::expr::Kind,
	value: Value,
) -> Result<Value, crate::val::value::CoerceError> {
	if !kind.has_named_type() {
		return Ok(value);
	}
	let Ok(database) = ctx.database() else {
		return Ok(value);
	};
	let (ns, db) = (database.ns_ctx.ns.namespace_id, database.db.database_id);
	check_named_types(ctx, ns, db, kind, value).await
}

/// `value`, already cast to `kind`, cast to the body of every named type the kind holds where
/// `ctx` reaches the catalog and judged by the checks of each, as [`check_types_held`] judges a
/// coerced value. A context with no database selected refuses a kind that names a type, which it
/// cannot read.
pub(crate) async fn cast_types_held(
	ctx: &crate::exec::ExecutionContext,
	kind: &crate::expr::Kind,
	value: Value,
) -> anyhow::Result<Value> {
	if !kind.has_named_type() {
		return Ok(value);
	}
	let database = ctx.database()?;
	let (ns, db) = (database.ns_ctx.ns.namespace_id, database.db.database_id);
	Ok(named_types(
		ctx,
		ns,
		db,
		kind,
		value,
		Judge {
			convert: Convert::Cast,
			stand_in: None,
		},
		0,
	)
	.await?)
}

#[cfg(test)]
mod tests {
	use super::{block_names_type, composed_renames};
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
	fn a_function_body_names_a_type_only_through_a_kind() {
		let names = |text: &str| block_names_type(&crate::syn::block(text).unwrap(), "foo");
		assert!(!names("{ RETURN 'foo' }"));
		assert!(!names("{ RETURN \"<foo> $x\" }"));
		assert!(!names("{ RETURN <bar> $x }"));
		assert!(names("{ RETURN <foo> $x }"));
		assert!(names("{ RETURN <option<array<foo>>> $x }"));
		assert!(names("{ LET $x: foo = 'a'; RETURN $x }"));
		assert!(names("{ RETURN |$v: foo| $v }"));
		assert!(names("{ RETURN |$v| -> foo { $v } }"));
		assert!(names("{ IF true { RETURN [1, <foo> $x] } }"));
	}
}
