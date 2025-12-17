use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::DatabaseDefinition;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Expr, FlowResultExt, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub strict: bool,
	pub comment: Expr,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: Expr::Literal(Literal::None),
			changefeed: None,
			strict: false,
		}
	}
}

impl DefineDatabaseStatement {
	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "DefineDatabaseStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;

		// Get the NS
		let ns = opt.ns()?;

		// Fetch the transaction
		let txn = ctx.tx();
		let nsv = txn.get_or_add_ns(Some(ctx), ns).await?;

		// Process the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "database name").await?;

		// Check if the definition exists
		let database_id = if let Some(db) = txn.get_db_by_name(ns, &name).await? {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::DbAlreadyExists {
							name: name.clone(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}

			db.database_id
		} else {
			ctx.try_get_sequences()?.next_database_id(Some(ctx), nsv.namespace_id).await?
		};

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, doc))
			.await
			.catch_return()?
			.cast_to()?;

		// Set the database definition, keyed by namespace name and database name.
		let db_def = DatabaseDefinition {
			namespace_id: nsv.namespace_id,
			database_id,
			name: name.clone(),
			comment,
			changefeed: self.changefeed,
			strict: self.strict,
		};
		txn.put_db(&nsv.name, db_def).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear();
		}

		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
impl InfoStructure for DefineDatabaseStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"comment".to_string() => self.comment.structure(),
		})
	}
}
