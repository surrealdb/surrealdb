use anyhow::{Result, bail};
use reblessive::tree::Stk;
use surrealdb_strand::TableName;
use uuid::Uuid;

use super::retire_table_indexes;
use crate::catalog::providers::TableProvider;
use crate::catalog::{Error as CatalogError, TableDefinition};
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::exec::Error as ExecError;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::statements::subscriptions::kill_table_subscriptions;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

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
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Table, Base::Db)?;
		// Compute the name
		let name =
			TableName::new(expr_to_ident(stk, ctx, opt, doc, &self.name, "table name").await?);
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the defined table
		let Some(tb) = txn.get_tb(ns, db, &name, None).await? else {
			if self.if_exists {
				return Ok(Value::None);
			}

			return Err(CatalogError::TbNotFound {
				name,
			}
			.into());
		};
		// Get the foreign tables
		let fts = txn.all_tb_views(ns, db, &name, None).await?;

		if !fts.is_empty() {
			let mut message =
				format!("Cannot delete table `{name}` on which a view is defined, table(s) `");
			for (idx, f) in fts.iter().enumerate() {
				if idx != 0 {
					message.push_str("`, `")
				}
				message.push_str(f.name.as_str());
			}

			message.push_str("` are defined as a view on this table.");

			bail!(ExecError::Query {
				message
			});
		}

		// Retire index state before deleting the table definition. Durable
		// cleanup is transactional; local builder aborts are deferred until commit.
		retire_table_indexes(ctx, &txn, ns, db, &tb).await?;
		// Every subscription on the table is about to lose its keys, so each one
		// is owed a KILLED. Compiling is total, so this reaches subscriptions
		// whose text no longer parses too: the clients most in need of being
		// told, since nothing else will ever wake them.
		kill_table_subscriptions(ctx, &txn, ns, db, &name).await?;

		// Delete the definition
		if self.expunge {
			txn.clr_tb(ns_name, db_name, &name).await?
		} else {
			txn.del_tb(ns_name, db_name, &name).await?
		};

		// Remove the resource data
		let key = crate::key::table::all::TableRoot {
			prefix: crate::key::database::all::DatabaseRoot {
				ns,
				db,
			},
			tb: std::borrow::Cow::Borrowed(&name),
		};
		if self.expunge {
			txn.clr_prefix_key(&key).await?
		} else {
			txn.del_prefix_key(&key).await?
		};
		// Check if this is a foreign table
		if let Some(tables) = tb.view.as_ref().map(|v| v.source_tables()) {
			// Process each foreign table
			for ft in tables.iter() {
				// Save the view config
				let key = crate::key::table::ft::Ft {
					prefix: crate::key::database::all::DatabaseRoot {
						ns,
						db,
					},
					tb: std::borrow::Cow::Borrowed(ft),
					ft: std::borrow::Cow::Borrowed(&name),
				};
				txn.del_key(&key).await?;
				// Refresh the table cache for foreign tables
				let foreign_tb = txn.expect_tb(ns, db, ft).await?;
				txn.put_tb(
					ns_name,
					db_name,
					&TableDefinition {
						cache_tables_ts: Uuid::now_v7(),
						..(*foreign_tb).clone()
					},
				)
				.await?;
			}
		}
		// The table (and its committed `cache_lives_ts`) is being removed, so
		// there is nothing to invalidate: open subscriptions were queued a KILLED
		// notification above, and a re-created table gets a fresh
		// `cache_lives_ts`, so the live-query cache cannot serve stale entries.
		// Clear the transaction cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
