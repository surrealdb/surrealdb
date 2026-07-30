use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_strand::TableName;
use tracing::instrument;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::index::AlterIndexStatement;
use crate::iam::{Action, ResourceKind};
use crate::legacy::expr_to_ident;
use crate::val::Value;

#[instrument(level = "trace", name = "AlterIndexStatement::compute", skip_all)]
pub(crate) async fn alter_index_statement_compute(
	this: &AlterIndexStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "index name").await?;
	let table = TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.table, "table name").await?);
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Get the index definition
	let mut ix = match txn.get_tb_index(ns, db, &table, &name, None).await? {
		Some(tb) => tb.deref().clone(),
		None => {
			if this.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::IxNotFound {
					name,
				}
				.into());
			}
		}
	};

	match this.comment {
		AlterKind::Set(ref k) => ix.comment = Some(k.clone()),
		AlterKind::Drop => ix.comment = None,
		AlterKind::None => {}
	}

	if this.prepare_remove && !ix.prepare_remove {
		ix.prepare_remove = true;
	}

	// Set the index definition
	txn.put_tb_index(ns, db, &table, &ix).await?;

	// Refresh the table cache for indexes
	let tb = txn.expect_tb(ns, db, &table).await?;
	txn.put_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_indexes_ts: Uuid::now_v7(),
			..(*tb).clone()
		},
	)
	.await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
