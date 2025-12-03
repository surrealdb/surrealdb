use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{ApiConfigDefinition, MiddlewareDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResultExt};

/// The api configuration as it is received from ast.

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

/// The api middleware as it is received from ast.

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct Middleware {
	pub name: String,
	pub args: Vec<Expr>,
}

impl ApiConfig {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<ApiConfigDefinition> {
		let mut middleware = Vec::new();
		for m in self.middleware.iter() {
			let mut args = Vec::new();
			for arg in m.args.iter() {
				args.push(stk.run(|stk| arg.compute(stk, ctx, opt, doc)).await.catch_return()?)
			}
			middleware.push(MiddlewareDefinition {
				name: m.name.clone(),
				args,
			});
		}

		Ok(ApiConfigDefinition {
			middleware,
			permissions: self.permissions.clone(),
		})
	}
}
