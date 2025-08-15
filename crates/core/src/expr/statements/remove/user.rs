use std::fmt::{self, Display, Formatter};

use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct RemoveUserStatement {
	pub name: Ident,
	pub base: Base,
	pub if_exists: bool,
}

impl RemoveUserStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Check the statement type
		match self.base {
			Base::Root => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let us = match txn.get_root_user(&self.name).await {
					Ok(x) => x,
					Err(e) => {
						if self.if_exists
							&& matches!(e.downcast_ref(), Some(Error::UserRootNotFound { .. }))
						{
							return Ok(Value::None);
						} else {
							return Err(e);
						}
					}
				};
				// Process the statement
				let key = crate::key::root::us::new(&us.name);
				txn.del(&key).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Ns => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let us = match txn.get_ns_user(opt.ns()?, &self.name).await {
					Ok(x) => x,
					Err(e) => {
						if self.if_exists
							&& matches!(e.downcast_ref(), Some(Error::UserNsNotFound { .. }))
						{
							return Ok(Value::None);
						} else {
							return Err(e);
						}
					}
				};
				// Delete the definition
				let key = crate::key::namespace::us::new(opt.ns()?, &us.name);
				txn.del(&key).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			Base::Db => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let (ns, db) = opt.ns_db()?;
				let us = match txn.get_db_user(ns, db, &self.name).await {
					Ok(x) => x,
					Err(e) => {
						if self.if_exists
							&& matches!(e.downcast_ref(), Some(Error::UserDbNotFound { .. }))
						{
							return Ok(Value::None);
						} else {
							return Err(e);
						}
					}
				};
				// Delete the definition
				let key = crate::key::database::us::new(ns, db, &us.name);
				txn.del(&key).await?;
				// Clear the cache
				txn.clear_cache();
				// Ok all good
				Ok(Value::None)
			}
			_ => Err(anyhow::Error::new(Error::InvalidLevel(self.base.to_string()))),
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
