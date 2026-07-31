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
use crate::expr::statements::remove::event::RemoveEventStatement;
use crate::iam::{Action, ResourceKind};
use crate::key::schema::EventKey;
use crate::legacy::expr_to_ident;
use crate::val::Value;

/// Process this type returning a computed simple Value
pub(crate) async fn remove_event_statement_compute(
	this: &RemoveEventStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Event, Base::Db)?;
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	// Compute the table name
	let table_name =
		TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.table_name, "table name").await?);
	// Compute the name
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "event name").await?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

	// Get the transaction
	let txn = ctx.tx();
	// Get the definition
	let ev = match txn.get_tb_event(ns, db, &table_name, &name, None).await {
		Ok(x) => x,
		Err(e) => {
			if this.if_exists && matches!(e.downcast_ref(), Some(Error::EvNotFound { .. })) {
				return Ok(Value::None);
			} else {
				return Err(e);
			}
		}
	};
	// Delete the definition
	let key = EventKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&ev.target_table),
		ev: std::borrow::Cow::Borrowed(&ev.name),
	};
	txn.del_key(&key).await?;

	let Some(tb) = txn.get_tb(ns, db, &table_name, None).await? else {
		return Err(Error::TbNotFound {
			name: table_name,
		}
		.into());
	};

	// Refresh the table cache for events
	txn.put_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_events_ts: Uuid::now_v7(),
			..(*tb).clone()
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
