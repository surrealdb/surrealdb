use anyhow::{Result, bail};
use async_trait::async_trait;
use reblessive::TreeStack;
use surrealism_runtime::config::SurrealismConfig;
use surrealism_runtime::host::InvocationContext;
use surrealism_runtime::kv::{BTreeMapStore, KVStore};

use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt, FunctionCall};
use crate::syn;
use crate::types::{PublicObject, PublicValue};
use crate::val::convert_value_to_public_value;

pub(crate) struct Host {
	pub(crate) stk: TreeStack,
	pub(crate) ctx: FrozenContext,
	pub(crate) opt: Options,
	pub(crate) doc: Option<CursorDoc>,
	kv: BTreeMapStore,
}

impl Host {
	pub(crate) fn new(ctx: &FrozenContext, opt: &Options, doc: Option<&CursorDoc>) -> Self {
		Self {
			stk: TreeStack::new(),
			ctx: ctx.clone(),
			opt: opt.clone(),
			doc: doc.cloned(),
			kv: BTreeMapStore::new(),
		}
	}
}

#[async_trait]
impl InvocationContext for Host {
	async fn sql(
		&mut self,
		_config: &SurrealismConfig,
		query: String,
		vars: PublicObject,
	) -> Result<PublicValue> {
		let ctx = if vars.is_empty() {
			self.ctx.clone()
		} else {
			let mut ctx = Context::new(&self.ctx);
			ctx.attach_public_variables(vars.into())?;
			ctx.freeze()
		};

		let expr: Expr = syn::expr(&query)?.into();
		let res = self
			.stk
			.enter(|stk| expr.compute(stk, &ctx, &self.opt, self.doc.as_ref()))
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

	fn kv(&mut self) -> Result<&dyn KVStore> {
		Ok(&self.kv)
	}

	fn stdout(&mut self, output: &str) -> Result<()> {
		tracing::info!(target: "surrealism::module", "{output}");
		Ok(())
	}

	fn stderr(&mut self, output: &str) -> Result<()> {
		tracing::warn!(target: "surrealism::module", "{output}");
		Ok(())
	}
}

pub(crate) struct SignatureHost {}

impl SignatureHost {
	pub(crate) fn new() -> Self {
		Self {}
	}
}

#[async_trait]
impl InvocationContext for SignatureHost {
	async fn sql(
		&mut self,
		_config: &SurrealismConfig,
		_query: String,
		_vars: PublicObject,
	) -> Result<PublicValue> {
		bail!("SQL is not supported in signature host")
	}

	async fn run(
		&mut self,
		_config: &SurrealismConfig,
		_fnc: String,
		_version: Option<String>,
		_args: Vec<PublicValue>,
	) -> Result<PublicValue> {
		bail!("Run is not supported in signature host")
	}

	fn kv(&mut self) -> Result<&dyn KVStore> {
		bail!("Run is not supported in signature host")
	}

	fn stdout(&mut self, _output: &str) -> Result<()> {
		bail!("I/O is not supported in signature host")
	}

	fn stderr(&mut self, _output: &str) -> Result<()> {
		bail!("I/O is not supported in signature host")
	}
}
