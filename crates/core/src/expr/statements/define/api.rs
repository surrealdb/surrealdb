use std::fmt;

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::config::api::ApiConfig;
use super::{CursorDoc, DefineKind};
use crate::api::path::Path;
use crate::catalog::{ApiActionDefinition, ApiDefinition, ApiMethod};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::fmt::{Fmt, pretty_indent};
use crate::expr::{Base, Expr, FlowResultExt as _, Value};
use crate::iam::{Action, ResourceKind};
use crate::val::Strand;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct DefineApiStatement {
	pub kind: DefineKind,
	pub path: Expr,
	pub actions: Vec<ApiAction>,
	pub fallback: Option<Expr>,
	pub config: ApiConfig,
	pub comment: Option<Strand>,
}

impl DefineApiStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Api, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Check if the definition exists
		if txn.get_db_api(ns, db, &self.path.to_string()).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::ApAlreadyExists {
							value: self.path.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		let path = stk.run(|stk| self.path.compute(stk, ctx, opt, doc)).await.catch_return()?;
		// Process the statement
		let path: Path = path.coerce_to::<String>()?.parse()?;
		let name = path.to_string();

		let config = self.config.compute(stk, ctx, opt, doc).await?;

		let key = crate::key::database::ap::new(ns, db, &name);
		let mut actions = Vec::new();
		for action in self.actions.iter() {
			actions.push(ApiActionDefinition {
				methods: action.methods.clone(),
				action: action.action.clone(),
				config: action.config.compute(stk, ctx, opt, doc).await?,
			});
		}

		let ap = ApiDefinition {
			path,
			actions,
			fallback: self.fallback.clone(),
			config,
			comment: self.comment.as_ref().map(|c| c.clone().into_string()),
		};
		txn.set(&key, &ap, None).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl fmt::Display for DefineApiStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE API")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.path)?;
		let indent = pretty_indent();

		write!(f, "FOR any")?;
		{
			let indent = pretty_indent();

			write!(f, "{}", self.config)?;

			if let Some(fallback) = &self.fallback {
				write!(f, "THEN {fallback}")?;
			}

			drop(indent);
		}

		for action in &self.actions {
			write!(f, "{action}")?;
		}

		if let Some(ref comment) = self.comment {
			write!(f, " COMMENT {comment}")?;
		}

		drop(indent);
		Ok(())
	}
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ApiAction {
	pub methods: Vec<ApiMethod>,
	pub action: Expr,
	pub config: ApiConfig,
}

impl fmt::Display for ApiAction {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "FOR {}", Fmt::comma_separated(self.methods.iter()))?;
		let indent = pretty_indent();
		write!(f, "{}", &self.config)?;
		write!(f, "THEN {}", self.action)?;
		drop(indent);
		Ok(())
	}
}
