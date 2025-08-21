use std::fmt;

use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::{ApiConfigDefinition, MiddlewareDefinition, Permission};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fmt::Fmt;
use crate::expr::{Expr, FlowResultExt};

/// The api configuration as it is received from ast.

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

/// The api middleware as it is received from ast.

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct Middleware {
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

	pub fn is_empty(&self) -> bool {
		self.middleware.is_empty() && self.permissions.is_none()
	}
}

impl fmt::Display for ApiConfig {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, " API")?;

		if !self.middleware.is_empty() {
			write!(f, " MIDDLEWARE ")?;
			write!(
				f,
				"{}",
				Fmt::pretty_comma_separated(self.middleware.iter().map(|m| format!(
					"{}({})",
					m.name,
					Fmt::pretty_comma_separated(m.args.iter())
				)))
			)?
		}

		write!(f, " PERMISSIONS {}", self.permissions)?;
		Ok(())
	}
}
