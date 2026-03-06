use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::SequenceDefinition;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::statements::define::DefineKind;
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;
use crate::key::sequence::Prefix;
use crate::val::{Duration, Value};

#[derive(Debug)]
pub struct DefineSequencePlan {
	pub kind: DefineKind,
	pub name: Arc<dyn PhysicalExpr>,
	pub batch: Arc<dyn PhysicalExpr>,
	pub start: Arc<dyn PhysicalExpr>,
	pub timeout: Arc<dyn PhysicalExpr>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineSequencePlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Arc<dyn PhysicalExpr>,
		batch: Arc<dyn PhysicalExpr>,
		start: Arc<dyn PhysicalExpr>,
		timeout: Arc<dyn PhysicalExpr>,
	) -> Self {
		Self {
			kind,
			name,
			batch,
			start,
			timeout,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineSequencePlan {
	ddl_operator_common!("DefineSequence", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let batch = self.batch.clone();
		let start = self.start.clone();
		let timeout = self.timeout.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move { execute(&ctx, kind, &*name, &*batch, &*start, &*timeout).await })
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name_expr: &dyn PhysicalExpr,
	batch_expr: &dyn PhysicalExpr,
	start_expr: &dyn PhysicalExpr,
	timeout_expr: &dyn PhysicalExpr,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns_name = &db_ctx.ns_ctx.ns.name;
	let db_name = &db_ctx.db.name;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	let timeout = helpers::eval_value(timeout_expr, ctx)
		.await?
		.cast_to::<Option<Duration>>()
		.map_err(anyhow::Error::from)?
		.map(|x| x.0);

	if txn
		.get_db_sequence(db_ctx.ns_ctx.ns.namespace_id, db_ctx.db.database_id, &name)
		.await
		.is_ok()
	{
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::SeqAlreadyExists {
						name: name.clone()
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let db_def = txn.get_or_add_db(Some(ctx.ctx()), ns_name, db_name).await?;

	let batch = helpers::eval_value(batch_expr, ctx)
		.await?
		.cast_to::<i64>()
		.map_err(anyhow::Error::from)?;

	let batch = u32::try_from(batch).map_err(|_| {
		Error::Query {
			message: format!(
				"`{batch}` is not valid batch size for a sequence definition. A batch size must be within 0..={}",
				u32::MAX
			),
		}
	})?;

	let start = helpers::eval_value(start_expr, ctx)
		.await?
		.cast_to::<i64>()
		.map_err(anyhow::Error::from)?;

	let ns_id = db_def.namespace_id;
	let db_id = db_def.database_id;

	let sq = SequenceDefinition {
		name: name.clone(),
		batch,
		start,
		timeout,
	};

	let key = Sq::new(ns_id, db_id, &name);
	txn.set(&key, &sq, None).await?;

	let ba_range = Prefix::new_ba_range(ns_id, db_id, &name)?;
	txn.delr(ba_range).await?;
	let st_range = Prefix::new_st_range(ns_id, db_id, &name)?;
	txn.delr(st_range).await?;

	txn.clear_cache();
	Ok(Value::None)
}
