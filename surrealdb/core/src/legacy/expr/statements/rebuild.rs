use std::sync::Arc;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, INDEX_FORMAT_VERSION};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::rebuild::{RebuildIndexStatement, RebuildStatement};
use crate::iam::{Action, ResourceKind};
use crate::legacy::{refresh_table_index_cache, run_indexing};
use crate::val::Value;

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "RebuildStatement::compute", skip_all)]
pub(crate) async fn rebuild_statement_compute(
	this: &RebuildStatement,
	_stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	_doc: Option<&CursorDoc>,
) -> Result<Value> {
	match this {
		RebuildStatement::Index(s) => {
			crate::legacy::rebuild_index_statement_compute(s, ctx, opt).await
		}
	}
}

/// Process this type returning a computed simple Value
pub(crate) async fn rebuild_index_statement_compute(
	this: &RebuildIndexStatement,
	ctx: &FrozenContext,
	opt: &Options,
) -> Result<Value> {
	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Index, Base::Db)?;
	// Get the index definition
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let res = ctx.tx().get_tb_index(ns, db, &this.table, this.name.as_str(), None).await?;
	let ix = match res {
		Some(x) => x,
		None => {
			if this.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::IxNotFound {
					name: this.name.to_string(),
				}
				.into());
			}
		}
	};
	let tb = ctx.tx().expect_tb(ns, db, &this.table).await?;

	// Stamp the definition with the current on-disk format version. The rebuild
	// repopulates the index in the current layout, so once it completes queries
	// must stop rejecting it as out-of-date (see
	// `StoredIndexDefinition::ensure_current_format`). Index kinds that don't use the
	// shared doc-ID space are already at the required version, so this is a no-op
	// for them.
	let ix = if ix.format_version != INDEX_FORMAT_VERSION {
		let mut updated = (*ix).clone();
		updated.format_version = INDEX_FORMAT_VERSION;
		let txn = ctx.tx();
		txn.put_tb_index(ns, db, &this.table, &updated).await?;
		let (ns_name, db_name) = opt.ns_db()?;
		refresh_table_index_cache(ctx, &txn, ns_name, db_name, &tb).await?;
		Arc::new(updated)
	} else {
		ix
	};

	// Rebuild the index
	run_indexing(ctx, opt, tb.table_id, ix, !this.concurrently).await?;
	// Ok all good
	Ok(Value::None)
}
