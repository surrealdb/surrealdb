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
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	kind: &DefineKind,
	definition: &crate::catalog::FieldDefinition,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::define_field_statement_validate_key_components(
		txn, ns, db, kind, definition,
	))
	.await
}

/// What refuses `REMOVE FIELD id` on the table `table` of the database `ns`,
/// `db`: a definition of the table that reads a component of its record id,
/// as `txn` holds the catalog and the rows of the table's views.
pub(super) async fn refuse_removing_id(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	table: &TableName,
) -> anyhow::Result<()> {
	crate::exec::vm::descriptor::schema(crate::ddl::key_scan::refuse_removing_field(
		txn,
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
