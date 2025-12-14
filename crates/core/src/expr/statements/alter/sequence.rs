use std::ops::Deref;

use anyhow::Result;
use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{Base, Expr, FlowResultExt, Value};
use crate::iam::{Action, ResourceKind};
use crate::key::database::sq::Sq;
use crate::val::Duration;

#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub(crate) struct AlterSequenceStatement {
	pub name: String,
	pub if_exists: bool,
	pub timeout: Option<Expr>,
}

impl AlterSequenceStatement {
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &FrozenContext,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Fetch the transaction
		let txn = ctx.tx();
		// Get the sequence definition
		let mut sq = match txn.get_db_sequence(ns, db, &self.name).await {
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
		let key = Sq::new(ns, db, &self.name);
		txn.set(&key, &sq, None).await?;
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
