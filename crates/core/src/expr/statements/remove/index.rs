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
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveIndexStatement {
	pub name: Expr,
	pub what: Expr,
	pub if_exists: bool,
}

impl Default for RemoveIndexStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			what: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "index name").await?;
		// Compute the what
		let what = expr_to_ident(stk, ctx, opt, doc, &self.what, "what").await?;
		// Get the NS and DB
		let (ns_name, db_name) = opt.ns_db()?;
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Get the transaction
		let txn = ctx.tx();
		// Clear the index store cache
		let err = ctx
			.get_index_stores()
			.index_removed(ctx.get_index_builder(), &txn, ns, db, &what, &name)
			.await;
		if let Err(e) = err {
			if self.if_exists && matches!(e.downcast_ref(), Some(Error::IxNotFound { .. })) {
				return Ok(Value::None);
			}
			return Err(e);
		}

		// Delete the index data.
		txn.del_tb_index(ns, db, &what, &name).await?;

		// Refresh the table cache for indexes
		let Some(tb) = txn.get_tb(ns, db, &what).await? else {
			return Err(Error::TbNotFound {
				name: what,
			}
			.into());
		};

		txn.put_tb(
			ns_name,
			db_name,
			&TableDefinition {
				cache_indexes_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &what);
		}
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveIndexStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE INDEX")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
