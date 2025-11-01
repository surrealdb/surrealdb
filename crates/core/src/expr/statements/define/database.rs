use std::fmt::{self, Display};

use anyhow::{Result, bail};
use reblessive::tree::Stk;

use super::DefineKind;
use crate::catalog::DatabaseDefinition;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::changefeed::ChangeFeed;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Expr, Literal};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineDatabaseStatement {
	pub kind: DefineKind,
	pub id: Option<u32>,
	pub name: Expr,
	pub strict: bool,
	pub comment: Option<Expr>,
	pub changefeed: Option<ChangeFeed>,
}

impl Default for DefineDatabaseStatement {
	fn default() -> Self {
		Self {
			kind: DefineKind::Default,
			id: None,
			name: Expr::Literal(Literal::None),
			comment: None,
			changefeed: None,
			strict: false,
		}
	}
}

impl DefineDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
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

		// Set the database definition, keyed by namespace name and database name.
		let db_def = DatabaseDefinition {
			namespace_id: nsv.namespace_id,
			database_id,
			name: name.clone(),
			comment: map_opt!(x as &self.comment => compute_to!(stk, ctx, opt, doc, x => String)),
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

impl Display for DefineDatabaseStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE DATABASE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {}", self.name)?;
		if self.strict {
			write!(f, " STRICT")?;
		}
		if let Some(ref v) = self.comment {
			write!(f, " COMMENT {}", v)?
		}
		if let Some(ref v) = self.changefeed {
			write!(f, " {v}")?;
		}
		Ok(())
	}
}

impl InfoStructure for DefineDatabaseStatement {
	fn structure(self) -> Value {
		Value::from(map! {
			"name".to_string() => self.name.structure(),
			"comment".to_string(), if let Some(v) = self.comment => v.structure(),
		})
	}
}
