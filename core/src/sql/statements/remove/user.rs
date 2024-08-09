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
pub struct RemoveUserStatement {
	pub name: Ident,
	pub base: Base,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
			// Check the statement type
			match self.base {
				Base::Root => {
					// Get the transaction
					let txn = ctx.tx();
					// Get the definition
					let us = txn.get_root_user(&self.name).await?;
					// Process the statement
					let key = crate::key::root::us::new(&us.name);
					txn.del(key).await?;
					// Clear the cache
					txn.clear();
					// Ok all good
					Ok(Value::None)
				}
				Base::Ns => {
					// Get the transaction
					let txn = ctx.tx();
					// Get the definition
					let us = txn.get_ns_user(opt.ns()?, &self.name).await?;
					// Delete the definition
					let key = crate::key::namespace::us::new(opt.ns()?, &us.name);
					txn.del(key).await?;
					// Clear the cache
					txn.clear();
					// Ok all good
					Ok(Value::None)
				}
				Base::Db => {
					// Get the transaction
					let txn = ctx.tx();
					// Get the definition
					let us = txn.get_db_user(opt.ns()?, opt.db()?, &self.name).await?;
					// Delete the definition
					let key = crate::key::database::us::new(opt.ns()?, opt.db()?, &us.name);
					txn.del(key).await?;
					// Clear the cache
					txn.clear();
					// Ok all good
					Ok(Value::None)
				}
				_ => Err(Error::InvalidLevel(self.base.to_string())),
			}
		}
		.await;
		match future {
			Err(e) if self.if_exists => match e {
				Error::UserRootNotFound {
					..
				} => Ok(Value::None),
				Error::UserNsNotFound {
					..
				} => Ok(Value::None),
				Error::UserDbNotFound {
					..
				} => Ok(Value::None),
				e => Err(e),
			},
			v => v,
		}
	}
}

impl Display for RemoveUserStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE USER")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.base)?;
		Ok(())
	}
}
