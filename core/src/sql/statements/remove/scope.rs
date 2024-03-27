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
#[non_exhaustive]
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
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Scope, &Base::Db)?;
			// Claim transaction
			let mut run = txn.lock().await;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let sc = run.get_sc(opt.ns(), opt.db(), &self.name).await?;
			// Delete the definition
			let key = crate::key::database::sc::new(opt.ns(), opt.db(), &sc.name);
			run.del(key).await?;
			// Remove the resource data
			let key = crate::key::scope::all::new(opt.ns(), opt.db(), &sc.name);
			run.delp(key, u32::MAX).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::ScNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveScopeStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE SCOPE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
