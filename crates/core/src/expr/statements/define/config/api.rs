use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

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

impl ToSql for ApiConfig {
	fn fmt_sql(&self, f: &mut String, sql_fmt: SqlFormat) {
		if !self.middleware.is_empty() {
			f.push_str(" MIDDLEWARE ");
			for (i, m) in self.middleware.iter().enumerate() {
				if i > 0 {
					sql_fmt.write_separator(f);
				}
				f.push_str(&m.name);
				f.push('(');
				for (j, arg) in m.args.iter().enumerate() {
					if j > 0 {
						sql_fmt.write_separator(f);
					}
					arg.fmt_sql(f, sql_fmt);
				}
				f.push(')');
			}
		}

		f.push_str(" PERMISSIONS ");
		self.permissions.fmt_sql(f, sql_fmt);
	}
}
