use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFunctionStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveFunctionStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
			// Claim transaction
			let mut run = ctx.tx_lock().await;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let fc = run.get_db_function(opt.ns(), opt.db(), &self.name).await?;
			// Delete the definition
			let key = crate::key::database::fc::new(opt.ns(), opt.db(), &fc.name);
			run.del(key).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::FcNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveFunctionStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE FUNCTION")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " fn::{}", self.name.0)?;
		Ok(())
	}
}
