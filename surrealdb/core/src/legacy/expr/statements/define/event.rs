use std::borrow::Cow;

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{Error, EventDefinition, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exe::FlowResultExt;
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::expr::statements::define::event::DefineEventStatement;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::legacy::expr_to_ident;
use crate::val::{TableName, Value};

/// Process this type returning a computed simple Value
#[instrument(level = "trace", name = "DefineEventStatement::compute", skip_all)]
pub(crate) async fn define_event_statement_compute(
	this: &DefineEventStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "event name").await?;
	let target_table = TableName::new(
		expr_to_ident(stk, ctx, opt, doc, &this.target_table, "target table").await?,
	);

	// Allowed to run?
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Event, Base::Db)?;
	// Get the NS and DB
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.get_ns_db_ids(opt).await?;
	// Fetch the transaction
	let txn = ctx.tx();
	// Check if the definition exists
	if txn.get_tb_event(ns, db, &target_table, &name, None).await.is_ok() {
		match this.kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::EvAlreadyExists {
						name: name.clone(),
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	// Ensure the table exists
	let tb = txn.get_or_add_tb(Some(ctx), ns_name, db_name, &target_table, None).await?;

	let comment = stk
		.run(|stk| crate::legacy::expr_compute(&this.comment, stk, ctx, opt, doc))
		.await
		.catch_return()?
		.cast_to()?;

	// Process the statement
	let key = crate::key::table::ev::Ev {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		tb: Cow::Borrowed(&target_table),
		ev: Cow::Borrowed(&name),
	};
	txn.set_key(
		&key,
		&EventDefinition::new(
			name.clone().into(),
			target_table.clone(),
			this.when.clone(),
			this.then.clone(),
			this.event_kind.clone().into(),
			AuthLimit::new_from_auth(opt.auth.as_ref()).into(),
			comment,
		)
		.to_stored(),
	)
	.await?;

	// Refresh the table cache
	let tb = TableDefinition {
		cache_events_ts: Uuid::now_v7(),
		..(*tb).clone()
	};
	txn.put_tb(ns_name, db_name, &tb).await?;
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
