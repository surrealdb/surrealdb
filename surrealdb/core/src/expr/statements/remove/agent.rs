use anyhow::Result;
use surrealdb_types::{SqlFormat, ToSql};

use crate::catalog::providers::DatabaseProvider;
use crate::ctx::FrozenContext;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct RemoveAgentStatement {
	pub name: String,
	pub if_exists: bool,
}

impl RemoveAgentStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &FrozenContext, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Agent, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		match txn.get_db_agent(ns, db, &self.name).await {
			Ok(_) => {}
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::AgNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		txn.del_db_agent(ns, db, &self.name).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl ToSql for RemoveAgentStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::remove::RemoveAgentStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}
