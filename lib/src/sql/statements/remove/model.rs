use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[revisioned(revision = 2)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct RemoveModelStatement {
	pub name: Ident,
	pub version: String,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		match run.get_db_model(opt.ns(), opt.db(), &self.name, &self.version).await {
			Ok(ml) => {
				// Delete the definition
				let key = crate::key::database::ml::new(opt.ns(), opt.db(), &ml.name, &ml.version);
				run.del(key).await?;
				// Remove the model file
				// TODO
				// Ok all good
				Ok(Value::None)
			}
			Err(err) => {
				if matches!(err, Error::TbNotFound { .. }) && self.if_exists {
					Ok(Value::None)
				} else {
					Err(err)
				}
			}
		}
	}
}

impl Display for RemoveModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODEL ml::{}<{}>", self.name.0, self.version)?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		Ok(())
	}
}
