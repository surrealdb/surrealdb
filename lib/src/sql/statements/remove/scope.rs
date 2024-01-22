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
pub struct RemoveScopeStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveScopeStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		_ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
	) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Scope, &Base::Db)?;
		// Claim transaction
		let mut run = txn.lock().await;
		// Clear the cache
		run.clear_cache();
		match run.get_sc(opt.ns(), opt.db(), &self.name).await {
			Ok(sc) => {
				let sc_name = sc.name.to_string();
				// Delete the definition
				let key = crate::key::database::sc::new(opt.ns(), opt.db(), &sc_name);
				run.del(key).await?;
				// Remove the resource data
				let key = crate::key::scope::all::new(opt.ns(), opt.db(), &sc_name);
				run.delp(key, u32::MAX).await?;
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

impl Display for RemoveScopeStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE SCOPE {}", self.name)?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		Ok(())
	}
}
