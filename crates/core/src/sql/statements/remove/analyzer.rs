use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveAnalyzerStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveAnalyzerStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			let (ns, db) = (opt.ns()?, opt.db()?);
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Analyzer, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Get the definition
			let az = txn.get_db_analyzer(ns, db, &self.name).await?;
			// Delete the definition
			let key = crate::key::database::az::new(ns, db, &az.name);
			txn.del(key).await?;
			// Clear the cache
			txn.clear();
			// Cleanup in-memory mappers if not used anymore
			let azs = txn.all_db_analyzers(ns, db).await?;
			ctx.get_index_stores().mappers().cleanup(&azs);
			// TODO Check that the analyzer is not used in any schema
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::AzNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
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
