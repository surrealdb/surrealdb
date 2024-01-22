use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 2)]
pub struct RemoveEventStatement {
	pub name: Ident,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveEventStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Event, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		match run.get_tb_event(opt.ns(), opt.db(), &self.what, &self.name).await {
			Ok(ev) => {
				// Delete the definition
				let key = crate::key::table::ev::new(opt.ns(), opt.db(), &ev.what, &ev.name);
				run.del(key).await?;
				// Clear the cache
				let key = crate::key::table::ev::prefix(opt.ns(), opt.db(), &ev.what);
				run.clr(key).await?;
				// Ok all good
				Ok(Value::None)
			},
			Err(err) => {
				if matches!(err, Error::EvNotFound { .. }) && self.if_exists {
					Ok(Value::None)
				} else {
					Err(err)
				}
			}
		}
	}
}

impl Display for RemoveEventStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE EVENT {} ON {}", self.name, self.what)?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		Ok(())
	}
}
