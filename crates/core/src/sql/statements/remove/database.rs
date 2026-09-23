use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveDatabaseStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
	/// Accepted and implied.
	///
	/// A deferred reclaim destroys the versions of everything it deletes,
	/// because the prefix it drains holds keys nothing ever writes over and a
	/// tombstone would leave them on disk for good on a backend that retains
	/// versions. The clause therefore asks for what already happens, and the
	/// only thing it no longer buys is that the destruction is complete by the
	/// time the statement returns: it is complete once the background reclaim
	/// task drains the job, which by default starts on the next tick after the
	/// removal is first observed.
	///
	/// `REMOVE TABLE` still reads its own copy of this: a table's data is
	/// destroyed inside the removing transaction rather than deferred.
	#[revision(start = 3)]
	pub expunge: bool,
}

impl RemoveDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
			// Get the transaction
			let txn = ctx.tx();
			// Remove the index stores
			ctx.get_index_stores()
				.database_removed(ctx.get_index_builder(), &txn, opt.ns()?, &self.name)
				.await?;
			// Get the definition
			let db = txn.get_db(opt.ns()?, &self.name).await?;
			// Delete the catalog definition and enqueue the data for the background
			// reclaim task, which destroys it under the removal's own terms. The size
			// of this transaction does not depend on how much the database held.
			txn.del_db_deferred(opt.ns()?, &db.name).await?;
			// Clear the cache
			if let Some(cache) = ctx.get_cache() {
				cache.clear();
			}
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::DbNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveDatabaseStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE DATABASE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
