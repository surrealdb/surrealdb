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
pub struct RemoveEventStatement {
	pub name: Ident,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
			// Claim transaction
			let mut run = ctx.tx_lock().await;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let ev = run.get_tb_event(opt.ns()?, opt.db()?, &self.what, &self.name).await?;
			// Delete the definition
			let key = crate::key::table::ev::new(opt.ns()?, opt.db()?, &ev.what, &ev.name);
			run.del(key).await?;
			// Clear the cache
			let key = crate::key::table::ev::prefix(opt.ns()?, opt.db()?, &ev.what);
			run.clr(key).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::EvNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveEventStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
