use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use reblessive::tree::Stk;
use uuid::Uuid;

use crate::catalog::TableDefinition;
use crate::catalog::providers::TableProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::expression::VisitExpression;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveEventStatement {
	pub name: Expr,
	pub table_name: Expr,
	pub if_exists: bool,
}

impl VisitExpression for RemoveEventStatement {
	fn visit<F>(&self, visitor: &mut F)
	where
		F: FnMut(&Expr),
	{
		self.name.visit(visitor);
		self.table_name.visit(visitor);
	}
}
impl Default for RemoveEventStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			table_name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		// Compute the table name
		let table_name = expr_to_ident(stk, ctx, opt, doc, &self.table_name, "table name").await?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "event name").await?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;

		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let ev = match txn.get_tb_event(ns, db, &table_name, &name).await {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::EvNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		let key = crate::key::table::ev::new(ns, db, &ev.target_table, &ev.name);
		txn.del(&key).await?;

		let Some(tb) = txn.get_tb(ns, db, &table_name).await? else {
			return Err(Error::TbNotFound {
				name: table_name,
			}
			.into());
		};

		// Refresh the table cache for events
		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_events_ts: Uuid::now_v7(),
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

impl Display for RemoveEventStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.table_name)?;
		Ok(())
	}
}
