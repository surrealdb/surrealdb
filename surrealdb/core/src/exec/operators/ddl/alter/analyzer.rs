use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::catalog::providers::DatabaseProvider;
use crate::exec::context::{ContextLevel, ExecutionContext};
use crate::exec::operators::ddl::helpers::{self, ddl_operator_common};
use crate::exec::{ExecOperator, FlowResult, OperatorMetrics, ValueBatchStream};
use crate::expr::statements::alter::AlterKind;
use crate::expr::{Base, Filter, Tokenizer};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug)]
pub struct AlterAnalyzerPlan {
	pub name: String,
	pub if_exists: bool,
	pub function: AlterKind<String>,
	pub tokenizers: AlterKind<Vec<Tokenizer>>,
	pub filters: AlterKind<Vec<Filter>>,
	pub comment: AlterKind<String>,
	pub(crate) metrics: Arc<OperatorMetrics>,
}

impl AlterAnalyzerPlan {
	pub(crate) fn new(
		name: String,
		if_exists: bool,
		function: AlterKind<String>,
		tokenizers: AlterKind<Vec<Tokenizer>>,
		filters: AlterKind<Vec<Filter>>,
		comment: AlterKind<String>,
	) -> Self {
		Self {
			name,
			if_exists,
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
impl ExecOperator for AlterAnalyzerPlan {
	ddl_operator_common!("AlterAnalyzer", ContextLevel::Database, strict);

	fn execute(&self, ctx: &ExecutionContext) -> FlowResult<ValueBatchStream> {
		let name = self.name.clone();
		let if_exists = self.if_exists;
		let function = self.function.clone();
		let tokenizers = self.tokenizers.clone();
		let filters = self.filters.clone();
		let comment = self.comment.clone();
		helpers::ddl_stream(ctx, move |ctx| {
			Box::pin(async move {
				execute(&ctx, name, if_exists, function, tokenizers, filters, comment).await
			})
		})
	}
}

#[allow(clippy::too_many_arguments)]
async fn execute(
	ctx: &ExecutionContext,
	name: String,
	if_exists: bool,
	function: AlterKind<String>,
	tokenizers: AlterKind<Vec<Tokenizer>>,
	filters: AlterKind<Vec<Filter>>,
	comment: AlterKind<String>,
) -> Result<Value> {
	let opt = helpers::get_opt(ctx)?;
	opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;

	let db_ctx = ctx.database()?;
	let ns = db_ctx.ns_ctx.ns.namespace_id;
	let db = db_ctx.db.database_id;

	let txn = ctx.txn();

	let mut az = match txn.get_db_analyzer(ns, db, &name).await {
		Ok(v) => v.as_ref().clone(),
		Err(e) => {
			if if_exists {
				return Ok(Value::None);
			}
			return Err(e);
		}
	};

	match function {
		AlterKind::Set(ref v) => az.function = Some(v.clone()),
		AlterKind::Drop => az.function = None,
		AlterKind::None => {}
	}

	match tokenizers {
		AlterKind::Set(ref v) => az.tokenizers = Some(v.clone()),
		AlterKind::Drop => az.tokenizers = None,
		AlterKind::None => {}
	}

	match filters {
		AlterKind::Set(ref v) => az.filters = Some(v.clone()),
		AlterKind::Drop => az.filters = None,
		AlterKind::None => {}
	}

	match comment {
		AlterKind::Set(ref v) => az.comment = Some(v.clone()),
		AlterKind::Drop => az.comment = None,
		AlterKind::None => {}
	}

	let key = crate::key::database::az::new(ns, db, &name);
	txn.set(&key, &az, None).await?;
	txn.clear_cache();
	Ok(Value::None)
}
