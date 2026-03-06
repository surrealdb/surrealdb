use std::ops::Deref;
use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::alter::AlterKind;
use crate::iam::{Action, ResourceKind};
use crate::val::{TableName, Value};

#[derive(Debug)]
pub struct AlterIndexPlan {
	pub name: String,
	pub table: TableName,
	pub if_exists: bool,
	pub prepare_remove: bool,
	pub comment: AlterKind<String>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterIndexPlan {
	pub(crate) fn new(
		name: String,
		table: TableName,
		if_exists: bool,
		prepare_remove: bool,
		comment: AlterKind<String>,
	) -> Self {
		Self {
			name,
			table,
			if_exists,
			prepare_remove,
			comment,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for AlterIndexPlan {
	ddl_operator_common!("AlterIndex", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let table = self.table.clone();
		let if_exists = self.if_exists;
		let prepare_remove = self.prepare_remove;
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(
				async move { execute(&ctx, name, table, if_exists, prepare_remove, comment).await },
			)
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	name: String,
	table: TableName,
	if_exists: bool,
	prepare_remove: bool,
	comment: AlterKind<String>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut ix = match txn.get_tb_index(ns, db, &table, &name).await? {
		Some(ix) => ix.deref().clone(),
		None => {
			if if_exists {
				return Ok(Value::None);
			} else {
				return Err(crate::err::Error::IxNotFound {
					name,
				}
				.into());
			}
		}
	};

	match comment {
		AlterKind::Set(ref k) => ix.comment = Some(k.clone()),
		AlterKind::Drop => ix.comment = None,
		AlterKind::None => {}
	}

	if prepare_remove && !ix.prepare_remove {
		ix.prepare_remove = true;
	}

	txn.put_tb_index(ns, db, &table, &ix).await?;

	let tb = txn.expect_tb(ns, db, &table).await?;
	txn.put_tb(
		ns_name,
		db_name,
		&TableDefinition {
			cache_indexes_ts: Uuid::now_v7(),
			..tb.as_ref().clone()
		},
	)
	.await?;

	txn.clear_cache();
	Ok(Value::None)
}
