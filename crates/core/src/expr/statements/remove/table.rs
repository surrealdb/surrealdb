use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::providers::TableProvider;
use crate::catalog::{TableDefinition, ViewDefinition};
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};
use crate::types::{PublicAction, PublicNotification, PublicValue};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveTableStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveTableStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl RemoveTableStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Table, &Base::Db)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "table name").await?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Remove the index stores
		ctx.get_index_stores().table_removed(ctx.get_index_builder(), &txn, ns, db, &name).await?;
		// Get the defined table
		let Some(tb) = txn.get_tb(ns, db, &name).await? else {
			if self.if_exists {
				return Ok(Value::None);
			}

			return Err(Error::TbNotFound {
				name,
			}
			.into());
		};

		// Get the foreign tables
		let fts = txn.all_tb_views(ns, db, &name).await?;
		// Get the live queries
		let lvs = txn.all_tb_lives(ns, db, &name).await?;

		// Delete the definition
		if self.expunge {
			txn.clr_tb(ns_name, db_name, &name).await?
		} else {
			txn.del_tb(ns_name, db_name, &name).await?
		};

		// Remove the resource data
		let key = crate::key::table::all::new(ns, db, &name);
		if self.expunge {
			txn.clrp(&key).await?
		} else {
			txn.delp(&key).await?
		};
		// Process each attached foreign table
		for ft in fts.iter() {
			// Refresh the table cache
			let foreign_tb = txn.expect_tb(ns, db, &ft.name).await?;
			txn.put_tb(
				ns_name,
				db_name,
				&TableDefinition {
					cache_tables_ts: Uuid::now_v7(),
					..foreign_tb.as_ref().clone()
				},
			)
			.await?;
		}
		// Check if this is a foreign table
		if let Some(view) = &tb.view {
			let (ViewDefinition::Materialized {
				tables,
				..
			}
			| ViewDefinition::Aggregated {
				tables,
				..
			}
			| ViewDefinition::Select {
				tables,
				..
			}) = &view;

			// Process each foreign table
			for ft in tables.iter() {
				// Save the view config
				let key = crate::key::table::ft::new(ns, db, ft, &name);
				txn.del(&key).await?;
				// Refresh the table cache for foreign tables
				let foreign_tb = txn.expect_tb(ns, db, ft).await?;
				txn.put_tb(
					ns_name,
					db_name,
					&TableDefinition {
						cache_tables_ts: Uuid::now_v7(),
						..foreign_tb.as_ref().clone()
					},
				)
				.await?;
			}
		}
		if let Some(sender) = opt.broker.as_ref() {
			for lv in lvs.iter() {
				sender
					.send(PublicNotification::new(
						lv.id.into(),
						PublicAction::Killed,
						PublicValue::None,
						PublicValue::None,
					))
					.await;
			}
		}
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &name);
			cache.clear();
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveTableStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE TABLE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
