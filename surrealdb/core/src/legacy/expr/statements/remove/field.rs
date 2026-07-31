use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_strand::TableName;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::remove::field::RemoveFieldStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::FieldKey;
use crate::legacy::{expr_to_ident, expr_to_idiom};
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_field_statement_compute(
	this: &RemoveFieldStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Field, Base::Db)?;
	// Compute the table name
	let table_name =
		TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.table_name, "table name").await?);
	// Compute the name
	let name = expr_to_idiom(stk, ctx, opt, doc, &this.name, "field name").await?;
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Get the transaction
	let txn = ctx.tx();
	// Get the field name
	let name = name.to_raw_string();
	// Get the definition
	let fd = match txn.get_tb_field(ns, db, &table_name, &name, None).await? {
		Some(x) => x,
		None => {
			if this.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::FdNotFound {
					name,
				}
				.into());
			}
		}
	};
	// Delete the definition
	let key = FieldKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table_name),
		fd: std::borrow::Cow::Borrowed(&name),
	};
	txn.del_key(&key).await?;
	// If the removed field declared a REFERENCE, purge the reference keys it
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
	crate::legacy::expr::statements::define::field::purge_dropped_reference_keys(
		&txn,
		ns,
		db,
		&table_name,
		&fd,
		None,
	)
	.await?;
	// Refresh the table cache for fields
	let Some(tb) = txn.get_tb(ns, db, &table_name, None).await? else {
		return Err(Error::TbNotFound {
			name: table_name,
		}
		.into());
	};
	// Refresh the table cache
	txn.put_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_fields_ts: Uuid::now_v7(),
			..(*tb).clone()
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
