use anyhow::Result;
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{self, Error, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::val::Value;

/// Remove the field `name` of table `table_name`, as `REMOVE FIELD` does once
/// its field and table are named; `if_exists` leaves a missing field alone.
pub(crate) async fn remove_field_statement_apply(
	if_exists: bool,
	name: crate::expr::Idiom,
	table_name: TableName,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Get the transaction
	let txn = ctx.tx();
	// A lightweight relation's only fields are the auto in/out pair, which
	// the record-less scan and endpoint checks rest on.
	if let Some(tb) = txn.get_tb(ns, db, &table_name, None).await? {
		anyhow::ensure!(
			crate::kvs::lightweight::lightweight_relation(&tb.table_type).is_none(),
			crate::exec::Error::Thrown(
				"a LIGHTWEIGHT relation's fields cannot be removed: its in/out contract is \
				 managed by the table definition"
					.to_owned(),
			)
		);
	}
	// A component of a named record id is a derived field with no definition to
	// remove, and the `id` itself stays while a definition reads a component.
	let exec = crate::ddl::frame(ctx, opt, None).await;
	crate::ddl::key_scan::refuse_removing_field(&exec, &txn, ns, db, &table_name, &name).await?;
	// Get the field name
	let name = catalog::FieldDefinition::storage_name(&name);
	// Get the definition
	let fd = match txn.get_tb_field(ns, db, &table_name, &name, None).await? {
		Some(x) => x,
		None => {
			if if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::FdNotFound {
					name,
				}
				.into());
			}
		}
	};
	// A field a `MATERIALIZE INTO` clause fills is not removed.
	if !opt.import {
		crate::ddl::materialize::refuse_changing_destination(&txn, ns, db, &table_name, &fd.name)
			.await?;
	}
	// Removing a `COMPUTED` field changes the value it derives to none.
	if !opt.import && fd.computed.is_some() {
		let removed = catalog::FieldDefinition {
			name: fd.name.clone(),
			table: table_name.clone(),
			..Default::default()
		};
		crate::ddl::define_field_statement_validate_computed_views(
			&txn,
			ns,
			db,
			Some(fd.as_ref()),
			&removed,
		)
		.await?;
	}
	// On a table that enforces migrations the field's stored values go with
	// it; an import restores definitions as they were.
	if !opt.import {
		let exec = crate::ddl::frame(ctx, opt, None).await;
		crate::exec::migrate::remove_field(&exec, ns, db, &table_name, &fd)
			.await
			.map_err(crate::exec::migrate::into_error)?;
	}
	// Delete the definition, and every field nested in it: nothing is left
	// nested in a field that is no longer defined.
	let nested: Vec<catalog::FieldDefinition> = txn
		.all_tb_fields(ns, db, &table_name, None)
		.await?
		.iter()
		.filter(|nested| nested.name.len() > fd.name.len() && nested.name.starts_with(&fd.name))
		.cloned()
		.collect();
	txn.del_tb_field(ns, db, &table_name, &name).await?;
	// If a removed field declared a REFERENCE, purge the reference keys it
	// wrote. Those keys live under the referenced (target) record's range,
	// keyed by the referencing (table, field) rather than under the removed
	// field, so deleting the field definition above does not remove them.
	// Passing `new = None` cleans every table the field could target.
	//
	// Unlike ALTER FIELD and DEFINE FIELD ... OVERWRITE, this is NOT gated
	// on `opt.import`: a removed field must never leave reference keys
	// behind, and there is no verbatim-restore case to preserve here (a
	// removed field has no keys to re-establish). The import guard on
	// ALTER/DEFINE exists to avoid purging keys an in-progress import is
	// restoring for a field that still exists; a consistent import that
	// removes a field carries no keys for it, so purging unconditionally is
	// at worst a no-op and never deletes live data.
	crate::exec::reference_purge::purge_dropped_reference_keys(
		&txn,
		ns,
		db,
		&table_name,
		&fd,
		None,
	)
	.await?;
	// The nested ones go too, the definitions its kind implied among them,
	// and their reference keys with them.
	crate::ddl::define::field::del_tb_field_definitions(&txn, (ns, db), &table_name, &nested, true)
		.await?;
	// Refresh the table cache for fields
	let Some(tb) = txn.get_tb(ns, db, &table_name, None).await? else {
		return Err(Error::TbNotFound {
			name: table_name,
		}
		.into());
	};
	// Refresh the table cache. Removing an inline field changes the inline
	// field set, so the generation is bumped and every written payload
	// falls back until a write restamps it.
	txn.replace_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			graph_inline_gen: if fd.inline {
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
