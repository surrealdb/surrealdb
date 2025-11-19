use anyhow::Result;
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::{expr_to_ident, expr_to_idiom};
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveFieldStatement {
	pub name: Expr,
	pub table_name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveFieldStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table_name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Compute the table name
		let table_name = expr_to_ident(stk, ctx, opt, doc, &self.table_name, "table name").await?;
		// Compute the name
		let name = expr_to_idiom(stk, ctx, opt, doc, &self.name, "field name").await?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the field name
		let name = name.to_raw_string();
		// Get the definition
		let _fd = match txn.get_tb_field(ns, db, &table_name, &name).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::FdNotFound {
						name,
					}
					.into());
				}
			}
		};
		// Delete the definition
		let key = crate::key::table::fd::new(ns, db, &table_name, &name);
		txn.del(&key).await?;
		// Refresh the table cache for fields
		let Some(tb) = txn.get_tb(ns, db, &table_name).await? else {
			return Err(Error::TbNotFound {
				name: self.table_name.to_string(),
			}
			.into());
		};

		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_fields_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &table_name);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}
