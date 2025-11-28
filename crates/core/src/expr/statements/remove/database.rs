use anyhow::Result;
use reblessive::tree::Stk;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, Literal, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct RemoveDatabaseStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub expunge: bool,
}

impl Default for RemoveDatabaseStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			expunge: false,
		}
	}
}

impl RemoveDatabaseStatement {
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
		// Get the transaction
		let txn = ctx.tx();

		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "database name").await?;
		let ns = opt.ns()?;
		let db = match txn.get_db_by_name(ns, &name).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::DbNotFound {
						name,
					}
					.into());
				}
			}
		};

		// Remove the index stores
		ctx.get_index_stores()
			.database_removed(ctx.get_index_builder(), &txn, db.namespace_id, db.database_id)
			.await?;
		// Remove the sequences
		if let Some(seq) = ctx.get_sequences() {
			seq.database_removed(&txn, db.namespace_id, db.database_id).await?;
		}

		// Delete the definition
		txn.del_db(ns, &db.name, self.expunge).await?;

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
