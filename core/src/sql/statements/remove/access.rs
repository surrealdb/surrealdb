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
pub struct RemoveAccessStatement {
	pub name: Ident,
	pub base: Base,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveAccessStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context<'_>, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;

			match &self.base {
				Base::Ns => {
					// Claim transaction
					let mut run = ctx.transaction()?.lock().await;
					// Clear the cache
					run.clear_cache();
					// Get the definition
					let ac = run.get_ns_access(opt.ns(), &self.name).await?;
					// Delete the definition
					let key = crate::key::namespace::ac::new(opt.ns(), &ac.name);
					run.del(key).await?;
					// Ok all good
					Ok(Value::None)
				}
				Base::Db => {
					// Claim transaction
					let mut run = ctx.transaction()?.lock().await;
					// Clear the cache
					run.clear_cache();
					// Get the definition
					let ac = run.get_db_access(opt.ns(), opt.db(), &self.name).await?;
					// Delete the definition
					let key = crate::key::database::ac::new(opt.ns(), opt.db(), &ac.name);
					run.del(key).await?;
					// Ok all good
					Ok(Value::None)
				}
				_ => Err(Error::InvalidLevel(self.base.to_string())),
			}
		}
		.await;
		match future {
			Err(e) if self.if_exists => match e {
				Error::NaNotFound {
					..
				} => Ok(Value::None),
				Error::DaNotFound {
					..
				} => Ok(Value::None),
				e => Err(e),
			},
			v => v,
		}
	}
}

impl Display for RemoveAccessStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE ACCESS")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		Ok(())
	}
}
