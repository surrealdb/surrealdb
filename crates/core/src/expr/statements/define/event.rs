use std::fmt::{self, Display};

use anyhow::{Result, bail};
use reblessive::tree::Stk;
use uuid::Uuid;

use super::DefineKind;
use crate::catalog::providers::{CatalogProvider, TableProvider};
use crate::catalog::{EventDefinition, TableDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, FlowResultExt, Literal};
use crate::fmt::{CoverStmts, Fmt};
use crate::iam::{Action, ResourceKind};
use crate::val::Value;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct DefineEventStatement {
	pub kind: DefineKind,
	pub name: Expr,
	pub target_table: Expr,
	pub when: Expr,
	pub then: Vec<Expr>,
	pub comment: Expr,
}

impl DefineEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		_doc: Option<&CursorDoc>,
	) -> Result<Value> {
		let name = expr_to_ident(stk, ctx, opt, _doc, &self.name, "event name").await?;
		let target_table =
			expr_to_ident(stk, ctx, opt, _doc, &self.target_table, "target table").await?;

		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.get_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Check if the definition exists
		if txn.get_tb_event(ns, db, &target_table, &name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::EvAlreadyExists {
							name: name.clone(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => return Ok(Value::None),
			}
		}

		// Ensure the table exists
		let tb = {
			let (ns, db) = opt.ns_db()?;
			txn.get_or_add_tb(Some(ctx), ns, db, &target_table).await?
		};

		let comment = stk
			.run(|stk| self.comment.compute(stk, ctx, opt, _doc))
			.await
			.catch_return()?
			.cast_to()?;

		// Process the statement
		let key = crate::key::table::ev::new(ns, db, &target_table, &name);
		txn.set(
			&key,
			&EventDefinition {
				name: name.clone(),
				target_table: target_table.clone(),
				when: self.when.clone(),
				then: self.then.clone(),
				comment,
			},
			None,
		)
		.await?;

		// Refresh the table cache
		let tb = TableDefinition {
			cache_events_ts: Uuid::now_v7(),
			..tb.as_ref().clone()
		};

		txn.put_tb(ns_name, db_name, &tb).await?;

		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &target_table);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineEventStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE EVENT",)?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(
			f,
			" {} ON {} WHEN {} THEN {}",
			CoverStmts(&self.name),
			CoverStmts(&self.target_table),
			CoverStmts(&self.when),
			Fmt::comma_separated(self.then.iter().map(CoverStmts))
		)?;
		if !matches!(self.comment, Expr::Literal(Literal::None)) {
			write!(f, " COMMENT {}", CoverStmts(&self.comment))?;
		}
		Ok(())
	}
}
