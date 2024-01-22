use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Idiom, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 2)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		match run.get_tb_field(opt.ns(), opt.db(), &self.what, &self.name.to_string()).await {
			Ok(fd) => {
				// Delete the definition
				let fd_name = fd.name.to_string();
				let key = crate::key::table::fd::new(opt.ns(), opt.db(), &fd.what, &fd_name);
				run.del(key).await?;
				// Clear the cache
				let key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &fd.what);
				run.clr(key).await?;
				// Ok all good
				Ok(Value::None)
			}
			Err(err) => {
				if matches!(err, Error::FdNotFound { .. }) && self.if_exists {
					Ok(Value::None)
				} else {
					Err(err)
				}
			}
		}
	}
}

impl Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD {} ON {}", self.name, self.what)?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		Ok(())
	}
}
