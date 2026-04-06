use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{EventKind, TableDefinition};
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Expr};
use crate::iam::{Action, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Clone, Debug)]
pub struct AlterEventPlan {
	pub name: String,
	pub what: TableName,
	pub if_exists: bool,
	pub when: AlterKind<Expr>,
	pub then: AlterKind<Vec<Expr>>,
	pub comment: AlterKind<String>,
	pub kind: AlterKind<EventKind>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterEventPlan {
	#[allow(clippy::too_many_arguments)]
	pub(crate) fn new(
		name: String,
		what: TableName,
		if_exists: bool,
		when: AlterKind<Expr>,
		then: AlterKind<Vec<Expr>>,
		comment: AlterKind<String>,
		kind: AlterKind<EventKind>,
	) -> Self {
		Self {
			name,
			what,
			if_exists,
			when,
			then,
			comment,
			kind,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterEventPlan {
	ddl_operator_common!("AlterEvent", ContextLevel::Database, strict);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let what = self.what.clone();
		let if_exists = self.if_exists;
		let when = self.when.clone();
		let then = self.then.clone();
		let comment = self.comment.clone();
		let kind = self.kind.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, name, what, if_exists, when, then, comment, kind).await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: String,
	what: TableName,
	if_exists: bool,
	when: AlterKind<Expr>,
	then: AlterKind<Vec<Expr>>,
	comment: AlterKind<String>,
	kind: AlterKind<EventKind>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut ev = match txn.get_tb_event(ns, db, &what, &name).await {
		Ok(v) => v.as_ref().clone(),
		Err(e) => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match when {
		AlterKind::Set(ref v) => ev.when = v.clone(),
		AlterKind::Drop | AlterKind::None => {}
	}

	match then {
		AlterKind::Set(ref v) => ev.then.clone_from(v),
		AlterKind::Drop | AlterKind::None => {}
	}

	match comment {
		AlterKind::Set(ref v) => ev.comment = Some(v.clone()),
		AlterKind::Drop => ev.comment = None,
		AlterKind::None => {}
	}

	match kind {
		AlterKind::Set(ref v) => ev.kind = v.clone(),
		AlterKind::Drop => ev.kind = EventKind::Sync,
		AlterKind::None => {}
	}

	let key = crate::key::table::ev::new(ns, db, &what, &name);
	txn.set(&key, &ev, None).await?;

	if let Some(tb) = txn.get_tb(ns, db, &what).await? {
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_events_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
	}

	txn.clear_cache();
	Ok(Value::None)
}
