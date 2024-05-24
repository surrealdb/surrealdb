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
pub struct RemoveParamStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveParamStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Parameter, &Base::Db)?;
			// Claim transaction
			let mut run = ctx.transaction()?.lock().await;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let pa = run.get_db_param(opt.ns(), opt.db(), &self.name).await?;
			// Delete the definition
			let key = crate::key::database::pa::new(opt.ns(), opt.db(), &pa.name);
			run.del(key).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::PaNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveParamStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE PARAM")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " ${}", self.name)?;
		Ok(())
	}
}
