use std::fmt::{self, Display, Formatter};

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveAnalyzerStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl RemoveAnalyzerStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let az = txn.get_db_analyzer(ns, db, &self.name).await;
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

impl Display for RemoveAnalyzerStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ANALYZER")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
