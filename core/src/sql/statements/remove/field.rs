use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Idiom, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
			// Claim transaction
			let mut run = ctx.transaction()?.lock().await;
			// Clear the cache
			run.clear_cache();
			// Get the definition
			let fd_name = self.name.to_string();
			let fd = run.get_tb_field(opt.ns(), opt.db(), &self.what, &fd_name).await?;
			// Delete the definition
			let fd_name = fd.name.to_string();
			let key = crate::key::table::fd::new(opt.ns(), opt.db(), &self.what, &fd_name);
			run.del(key).await?;
			// Clear the cache
			let key = crate::key::table::fd::prefix(opt.ns(), opt.db(), &self.what);
			run.clr(key).await?;
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::FdNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
