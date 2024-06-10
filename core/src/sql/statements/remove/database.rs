use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveDatabaseStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
			// Claim transaction
			let mut run = ctx.tx_lock().await;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let db = run.get_db(opt.ns()?, &self.name).await?;
			// Delete the definition
			let key = crate::key::namespace::db::new(opt.ns()?, &db.name);
			run.del(key).await?;
			// Delete the resource data
			let key = crate::key::database::all::new(opt.ns()?, &db.name);
			run.delp(key, u32::MAX).await?;
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
