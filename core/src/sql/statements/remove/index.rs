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
pub struct RemoveIndexStatement {
	pub name: Ident,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveIndexStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Index, &Base::Db)?;
			// Claim transaction
			let mut run = ctx.tx_lock().await;
			// Clear the index store cache
			ctx.get_index_stores().index_removed(opt, &mut run, &self.what, &self.name).await?;
			// Clear the cache
			run.clear_cache();
			// Delete the definition
			let key = crate::key::table::ix::new(opt.ns()?, opt.db()?, &self.what, &self.name);
			run.del(key).await?;
			// Remove the index data
			let key = crate::key::index::all::new(opt.ns()?, opt.db()?, &self.what, &self.name);
			run.delp(key, u32::MAX).await?;
			// Clear the cache
			let key = crate::key::table::ix::prefix(opt.ns()?, opt.db()?, &self.what);
			run.clr(key).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::IxNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
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
