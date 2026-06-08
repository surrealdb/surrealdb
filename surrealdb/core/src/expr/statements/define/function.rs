use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_strand::Strand;
use surrealdb_types::{SqlFormat, ToSql};

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider};
use crate::catalog::{FunctionDefinition, Permission};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Block, Expr, FlowResultExt, Kind};
use crate::iam::{Action, AuthLimit, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineFunctionStatement {
	pub kind: DefineKind,
	pub name: Strand,
	pub args: Vec<(String, Kind)>,
	pub block: Block,
	pub comment: Expr,
	pub permissions: Permission,
	pub returns: Option<Kind>,
	pub graphql_alias: Option<String>,
	pub graphql_deprecated: Option<String>,
}

impl DefineFunctionStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "DefineFunctionStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Function, Base::Db)?;
		// Validate any GRAPHQL_ALIAS at definition time so typos surface here
		// rather than silently falling back at schema-generation time.
		super::validate_graphql_alias(&self.graphql_alias, "function")?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		if txn.get_db_function(ns, db, &self.name, None).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::FcAlreadyExists {
							name: self.name.to_string(),
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

		txn.put_db_function(
			ns,
			db,
			&FunctionDefinition {
				name: self.name.clone(),
				args: self.args.clone(),
				block: self.block.clone(),
				permissions: self.permissions.clone(),
				returns: self.returns.clone(),
				comment,
				auth_limit: AuthLimit::new_from_auth(&opt.auth).into(),
				graphql_alias: self.graphql_alias.clone(),
				graphql_deprecated: self.graphql_deprecated.clone(),
			},
		)
		.await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for DefineFunctionStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::define::DefineFunctionStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
