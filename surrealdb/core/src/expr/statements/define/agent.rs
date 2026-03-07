use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::ai::agent::types::{AgentConfig, AgentGuardrails, AgentMemory, AgentModel, AgentTool};
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{AgentDefinition, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Expr, FlowResultExt};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineAgentStatement {
	pub kind: DefineKind,
	pub name: String,
	pub model: AgentModel,
	pub prompt: String,
	pub config: Option<AgentConfig>,
	pub tools: Vec<AgentTool>,
	pub memory: Option<AgentMemory>,
	pub guardrails: Option<AgentGuardrails>,
	pub comment: Expr,
	pub permissions: Permission,
}

impl DefineAgentStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "DefineAgentStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Agent, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if txn.get_db_agent(ns, db, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::AgAlreadyExists {
							name: self.name.clone(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}

		// Process the statement
		let (ns_name, db_name) = opt.ns_db()?;
		txn.get_or_add_db(Some(ctx), ns_name, db_name).await?;

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?;

		txn.put_db_agent(
			ns,
			db,
			&AgentDefinition {
				name: self.name.clone(),
				model: self.model.clone(),
				prompt: self.prompt.clone(),
				config: self.config.clone(),
				tools: self.tools.clone(),
				memory: self.memory.clone(),
				guardrails: self.guardrails.clone(),
				permissions: self.permissions.clone(),
				comment,
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for DefineAgentStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::DefineAgentStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
