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
pub struct RemoveModelStatement {
	pub name: Ident,
	pub version: String,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveModelStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;
			// Claim transaction
			let mut run = ctx.tx_lock().await;
			// Clear the cache
			run.clear_cache();
			// Delete the definition
			let key =
				crate::key::database::ml::new(opt.ns()?, opt.db()?, &self.name, &self.version);
			run.del(key).await?;
			// Remove the model file
			// TODO
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::MlNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveModelStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE MODEL")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ml::{}<{}>", self.name.0, self.version)?;
		Ok(())
	}
}
