use std::sync::Arc;

use anyhow::{Result, bail};
use async_trait::async_trait;

use crate::catalog::AnalyzerDefinition;
use crate::catalog::providers::DatabaseProvider;
use crate::err::Error;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::physical_expr::PhysicalExpr;
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::Base;
use crate::expr::filter::Filter;
use crate::expr::statements::define::DefineKind;
use crate::expr::tokenizer::Tokenizer;
use crate::iam::{Action, ResourceKind};
use crate::key::database::az;
use crate::val::Value;

#[derive(Debug)]
pub struct DefineAnalyzerPlan {
	pub kind: DefineKind,
	pub name: Arc<dyn PhysicalExpr>,
	pub function: Option<String>,
	pub tokenizers: Option<Vec<Tokenizer>>,
	pub filters: Option<Vec<Filter>>,
	pub comment: Arc<dyn PhysicalExpr>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl DefineAnalyzerPlan {
	pub(crate) fn new(
		kind: DefineKind,
		name: Arc<dyn PhysicalExpr>,
		function: Option<String>,
		tokenizers: Option<Vec<Tokenizer>>,
		filters: Option<Vec<Filter>>,
		comment: Arc<dyn PhysicalExpr>,
	) -> Self {
		Self {
			kind,
			name,
			function,
			tokenizers,
			filters,
			comment,
			metrics: Arc::new(OperatorMetrics::new()),
		}
	}
}

#[cfg_attr(target_family = "wasm", async_trait(?Send))]
#[cfg_attr(not(target_family = "wasm"), async_trait)]
impl ExecOperator for DefineAnalyzerPlan {
	ddl_operator_common!("DefineAnalyzer", ContextLevel::Database);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let kind = self.kind.clone();
		let name = self.name.clone();
		let function = self.function.clone();
		let tokenizers = self.tokenizers.clone();
		let filters = self.filters.clone();
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, kind, &*name, function, tokenizers, filters, &*comment).await
			})
		})
	}
}

async fn execute(
	ctx: &ExecutionContext,
	kind: DefineKind,
	name_expr: &dyn PhysicalExpr,
	function: Option<String>,
	tokenizers: Option<Vec<Tokenizer>>,
	filters: Option<Vec<Filter>>,
	comment_expr: &dyn PhysicalExpr,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();
	let name = helpers::eval_ident(name_expr, ctx).await?;

	if txn.get_db_analyzer(ns, db, &name).await.is_ok() {
		match kind {
			DefineKind::Default => {
				if !opt.import {
					bail!(Error::AzAlreadyExists {
						name: name.clone()
					});
				}
			}
			DefineKind::Overwrite => {}
			DefineKind::IfNotExists => return Ok(Value::None),
		}
	}

	let comment = helpers::eval_comment(comment_expr, ctx).await?;

	let definition = AnalyzerDefinition {
		name: name.clone(),
		function,
		tokenizers,
		filters,
		comment,
	};

	let key = az::new(ns, db, &name);
	ctx.ctx().get_index_stores().mappers().load(&definition).await?;
	txn.set(&key, &definition, None).await?;

	txn.clear_cache();
	Ok(Value::None)
}
