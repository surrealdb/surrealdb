use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};
use tracing::instrument;

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::parameterize::expr_to_ident;
use crate::expr::{Base, Expr, FlowResultExt, Literal, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;
use crate::val::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct AlterSequenceStatement {
	pub name: Expr,
	pub if_exists: bool,
	pub timeout: Option<Expr>,
}

impl Default for AlterSequenceStatement {
	fn default() -> Self {
		Self {
			name: Expr::Literal(Literal::None),
			if_exists: false,
			timeout: None,
		}
	}
}

impl AlterSequenceStatement {
	#[instrument(level = "trace", name = "AlterSequenceStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		ctx.is_allowed(opt, Action::Edit, ResourceKind::Sequence, Base::Db)?;
		let name = expr_to_ident(stk, ctx, opt, doc, &self.name, "sequence name").await?;
		// Get the NS and DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the sequence definition
		let mut sq = match txn.get_db_sequence(ns, db, &name, None).await {
			Ok(tb) => tb.deref().clone(),
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};

		if let Some(timeout) = &self.timeout {
			// Process the statement
			if let Some(timeout) = stk
				.run(|stk| timeout.compute(stk, ctx, opt, doc))
				.await
				.catch_return()?
				.cast_to::<Option<Duration>>()?
			{
				sq.timeout = Some(timeout.0);
			} else {
				sq.timeout = None;
			}
		}
		// Set the sequence definition
		let key = Sq::new(ns, db, &name);
		txn.set(&key, &sq).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for AlterSequenceStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::alter::AlterSequenceStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
