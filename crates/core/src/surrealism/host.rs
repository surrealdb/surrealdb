use anyhow::Result;
use async_trait::async_trait;
use reblessive::tree::Stk;
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

pub(crate) struct Host<'a> {
	pub(crate) stk: &'a mut Stk,
	pub(crate) ctx: &'a Context,
	pub(crate) opt: &'a Options,
	pub(crate) doc: Option<&'a CursorDoc>,
}

#[async_trait(?Send)]
impl<'a> InvocationContext for Host<'a> {
	async fn sql(
		&mut self,
		_config: &SurrealismConfig,
		query: String,
		_vars: PublicObject,
	) -> Result<PublicValue> {
		let expr: Expr = syn::expr(&query)?.into();
		let res = expr.compute(self.stk, self.ctx, self.opt, self.doc).await.catch_return()?;

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

		let res = expr.compute(self.stk, self.ctx, self.opt, self.doc).await.catch_return()?;

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
