use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

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
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Get the definition
			let ev = txn.get_tb_event(opt.ns()?, opt.db()?, &self.what, &self.name).await?;
			// Delete the definition
			let key = crate::key::table::ev::new(opt.ns()?, opt.db()?, &ev.what, &ev.name);
			txn.del(key).await?;
			// Refresh the table cache
			let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.what);
			let tb = txn.get_tb(opt.ns()?, opt.db()?, &self.what).await?;
			txn.set(
				key,
				DefineTableStatement {
					cache_events_ts: Uuid::now_v7(),
					..tb.as_ref().clone()
				},
				None,
			)
			.await?;
			// Clear the cache
			txn.clear();
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
