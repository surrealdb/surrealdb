use std::fmt::{self, Display};

use anyhow::Result;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::fmt::Fmt;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Expr, FlowResultExt, Permission, Value};
use crate::val::{Array, Object, Strand};

/// The api configuration as it is received from ast.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ApiConfig {
	pub middleware: Vec<Middleware>,
	pub permissions: Permission,
}

/// The api middleware as it is received from ast.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct Middleware {
	pub name: String,
	pub args: Vec<Expr>,
}

/// The api config as it is stored on disk.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct ApiConfigStore {
	pub middleware: Vec<MiddlewareStore>,
	pub permissions: Permission,
}

/// The api middleware as it is stored on disk.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct MiddlewareStore {
	pub name: String,
	pub args: Vec<Value>,
}

impl ApiConfig {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<ApiConfigStore> {
		let mut middleware = Vec::new();
		for m in self.middleware.iter() {
			let mut args = Vec::new();
			for arg in m.args.iter() {
				args.push(stk.run(|stk| arg.compute(stk, ctx, opt, doc)).await.catch_return()?)
			}
			middleware.push(MiddlewareStore {
				name: m.name.clone(),
				args,
			});
		}

		Ok(ApiConfigStore {
			middleware,
			permissions: self.permissions.clone(),
		})
	}

	pub fn is_empty(&self) -> bool {
		self.middleware.is_empty() && self.permissions.is_none()
	}
}

impl Display for ApiConfig {
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

impl Display for ApiConfigStore {
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

impl InfoStructure for ApiConfig {
	fn structure(self) -> Value {
		Value::from(map!(
			"permissions" => self.permissions.structure(),
			"middleware", if !self.middleware.is_empty() => {
				Value::Object(Object(
						self.middleware
						.into_iter()
						.map(|m| {
							let value = m.args
								.iter()
								.map(|x| Value::Strand(Strand::new(x.to_string()).unwrap()))
								.collect();

							(m.name.clone(), Value::Array(Array(value)))
						})
						.collect(),
				))
			}
		))
	}
}

impl InfoStructure for ApiConfigStore {
	fn structure(self) -> Value {
		Value::from(map!(
			"permissions" => self.permissions.structure(),
			"middleware", if !self.middleware.is_empty() => {
				Value::Object(Object(
						self.middleware
						.into_iter()
						.map(|m| {
							let value = m.args
								.iter()
								.map(|x| Value::Strand(Strand::new(x.to_string()).unwrap()))
								.collect();

							(m.name.clone(), Value::Array(Array(value)))
						})
						.collect(),
				))
			}
		))
	}
}
