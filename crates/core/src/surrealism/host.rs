use anyhow::Result;
use async_trait::async_trait;
use reblessive::TreeStack;
use surrealism_runtime::config::SurrealismConfig;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::kv::KVStore;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt, FunctionCall};
use crate::syn;
use crate::types::{PublicObject, PublicValue};
use crate::val::convert_value_to_public_value;

pub(crate) struct Host {
	pub(crate) stk: TreeStack,
	pub(crate) ctx: Context,
	pub(crate) opt: Options,
	pub(crate) doc: Option<CursorDoc>,
}

impl Host {
	pub(crate) fn new(ctx: &Context, opt: &Options, doc: Option<&CursorDoc>) -> Self {
		Self {
			stk: TreeStack::new(),
			ctx: ctx.clone(),
			opt: opt.clone(),
			doc: doc.cloned(),
		}
	}
}

#[async_trait]
impl InvocationContext for Host {
	async fn sql(
		&mut self,
		_config: &SurrealismConfig,
		query: String,
		_vars: PublicObject,
	) -> Result<PublicValue> {
		let expr: Expr = syn::expr(&query)?.into();
		let res = self
			.stk
			.enter(|stk| expr.compute(stk, &self.ctx, &self.opt, self.doc.as_ref()))
			.finish()
			.await
			.catch_return()?;

		convert_value_to_public_value(res)
	}

	async fn run(
		&mut self,
		_config: &SurrealismConfig,
		fnc: String,
		_version: Option<String>,
		_args: Vec<PublicValue>,
	) -> Result<PublicValue> {
		let expr = Expr::FunctionCall(Box::new(FunctionCall {
			receiver: syn::function(&fnc)?.into(),
			arguments: _args.into_iter().map(Expr::from_public_value).collect(),
		}));

		let res = self
			.stk
			.enter(|stk| expr.compute(stk, &self.ctx, &self.opt, self.doc.as_ref()))
			.finish()
			.await
			.catch_return()?;

		convert_value_to_public_value(res)
	}

	fn kv(&mut self) -> &dyn KVStore {
		todo!()
	}

	async fn ml_invoke_model(
		&mut self,
		_config: &SurrealismConfig,
		_model: String,
		_input: PublicValue,
		_weight: i64,
		_weight_dir: String,
	) -> Result<PublicValue> {
		todo!()
	}

	async fn ml_tokenize(
		&mut self,
		_config: &SurrealismConfig,
		_model: String,
		_input: PublicValue,
	) -> Result<Vec<f64>> {
		todo!()
	}

	fn stdout(&mut self, _output: &str) -> Result<()> {
		todo!()
	}

	fn stderr(&mut self, _output: &str) -> Result<()> {
		todo!()
	}
}
