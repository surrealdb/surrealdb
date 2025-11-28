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
pub(crate) struct RemoveAnalyzerStatement {
	pub name: Expr,
	pub if_exists: bool,
}

impl Default for RemoveAnalyzerStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
		}
	}
}

impl RemoveAnalyzerStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		// Compute the name
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "analyzer name").await?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let az = txn.get_db_analyzer(ns, db, &name).await;
		let az = match az {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::AzNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		let key = crate::key::database::az::new(ns, db, &az.name);
		txn.del(&key).await?;
		// Clear the cache
		txn.clear_cache();
		// Cleanup in-memory mappers if not used anymore
		let azs = txn.all_db_analyzers(ns, db).await?;
		ctx.get_index_stores().mappers().cleanup(&azs);
		// TODO Check that the analyzer is not used in any schema
		// Ok all good
		Ok(Value::None)
	}
}
