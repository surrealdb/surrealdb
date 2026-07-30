use std::borrow::Cow;
use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use tracing::instrument;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{EventKind, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::expr::statements::alter::event::AlterEventStatement;
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::key::database::all::DatabaseRoot;
use crate::legacy::expr_to_ident;
use crate::val::{TableName, Value};

#[instrument(level = "trace", name = "AlterEventStatement::compute", skip_all)]
pub(crate) async fn alter_event_statement_compute(
	this: &AlterEventStatement,
	stk: &mut Stk,
	ctx: &FrozenContext,
	opt: &Options,
	doc: Option<&CursorDoc>,
) -> Result<Value> {
	ctx.is_allowed(opt, Action::Edit, ResourceKind::Event, Base::Db)?;
	let name = expr_to_ident(stk, ctx, opt, doc, &this.name, "event name").await?;
	let what = TableName::new(expr_to_ident(stk, ctx, opt, doc, &this.what, "table name").await?);
	let (ns_name, db_name) = opt.ns_db()?;
	let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
	let txn = ctx.tx();

	let mut ev = match txn.get_tb_event(ns, db, &what, &name, None).await {
		Ok(v) => v.deref().clone(),
		Err(e) => {
			if this.if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match this.when {
		AlterKind::Set(ref v) => ev.when = v.clone(),
		AlterKind::Drop => {}
		AlterKind::None => {}
	}

	match this.then {
		AlterKind::Set(ref v) => ev.then = v.clone(),
		AlterKind::Drop => {}
		AlterKind::None => {}
	}

	match this.comment {
		AlterKind::Set(ref v) => ev.comment = Some(v.clone()),
		AlterKind::Drop => ev.comment = None,
		AlterKind::None => {}
	}

	match this.kind {
		AlterKind::Set(ref v) => ev.kind = v.clone().into(),
		AlterKind::Drop => ev.kind = EventKind::Sync,
		AlterKind::None => {}
	}

	// Recompute auth_limit from the current principal to prevent privilege escalation
	ev.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

	let key = crate::key::table::ev::Ev {
		prefix: DatabaseRoot {
			ns,
			db,
		},
		tb: Cow::Borrowed(&what),
		ev: Cow::Borrowed(&name),
	};
	txn.set_key(&key, &ev.to_stored()).await?;

	// Refresh the table cache
	if let Some(tb) = txn.get_tb(ns, db, &what, None).await? {
		let tb = TableDefinition {
			cache_events_ts: Uuid::now_v7(),
			..(*tb).clone()
		};
		txn.put_tb(ns_name, db_name, &tb).await?;
	}
	// Clear the cache
	txn.clear_cache();
	// Ok all good
	Ok(Value::None)
}
