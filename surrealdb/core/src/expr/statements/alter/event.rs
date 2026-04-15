use std::ops::Deref;

use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};
use uuid::Uuid;

use super::AlterKind;
use crate::catalog::providers::TableProvider;
use crate::catalog::{EventKind, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::expr::{Base, Expr};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct AlterEventStatement {
	pub name: String,
	pub what: TableName,
	pub if_exists: bool,
	pub when: AlterKind<Expr>,
	pub then: AlterKind<Vec<Expr>>,
	pub comment: AlterKind<String>,
	pub kind: AlterKind<EventKind>,
}

impl AlterEventStatement {
	#[instrument(level = "trace", name = "AlterEventStatement::compute", skip_all)]
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let txn = ctx.tx();

		let ev_name = &self.name;
		let mut ev = match txn.get_tb_event(ns, db, &self.what, ev_name, None).await {
			Ok(v) => v.deref().clone(),
			Err(e) => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(e);
			}
		};

		match self.when {
			AlterKind::Set(ref v) => ev.when = v.clone(),
			AlterKind::Drop => {}
			AlterKind::None => {}
		}

		match self.then {
			AlterKind::Set(ref v) => ev.then.clone_from(v),
			AlterKind::Drop => {}
			AlterKind::None => {}
		}

		match self.comment {
			AlterKind::Set(ref v) => ev.comment = Some(v.clone()),
			AlterKind::Drop => ev.comment = None,
			AlterKind::None => {}
		}

		match self.kind {
			AlterKind::Set(ref v) => ev.kind = v.clone(),
			AlterKind::Drop => ev.kind = EventKind::Sync,
			AlterKind::None => {}
		}

		// Recompute auth_limit from the current principal to prevent privilege escalation
		ev.auth_limit = AuthLimit::new_from_auth(opt.auth.as_ref()).into();

		let key = crate::key::table::ev::new(ns, db, &self.what, ev_name);
		txn.set(&key, &ev).await?;

		// Refresh the table cache
		if let Some(tb) = txn.get_tb(ns, db, &self.what, None).await? {
			let tb = TableDefinition {
				cache_events_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			};
			txn.put_tb(ns_name, db_name, &tb).await?;
		}

		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.what);
		}
		txn.clear_cache();
		Ok(Value::None)
	}
}

impl ToSql for AlterEventStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterEventStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
