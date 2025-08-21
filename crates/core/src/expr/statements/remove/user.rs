use std::fmt::{self, Display, Formatter};

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
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
				let us = match txn.get_root_user(&self.name).await? {
					Some(x) => x,
					None => {
						if self.if_exists {
							return Ok(Value::None);
						}

						return Err(Error::UserRootNotFound {
							name: self.name.to_string(),
						}
						.into());
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
				let ns = ctx.get_ns_id(opt).await?;
				let us = match txn.get_ns_user(ns, &self.name).await? {
					Some(x) => x,
					None => {
						if self.if_exists {
							return Ok(Value::None);
						}

						return Err(Error::UserNsNotFound {
							ns: opt.ns()?.to_string(),
							name: self.name.to_string(),
						}
						.into());
					}
				};
				// Delete the definition
				let key = crate::key::namespace::us::new(ns, &us.name);
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
				let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
				let us = match txn.get_db_user(ns, db, &self.name).await? {
					Some(x) => x,
					None => {
						if self.if_exists {
							return Ok(Value::None);
						}

						return Err(Error::UserDbNotFound {
							ns: opt.ns()?.to_string(),
							db: opt.db()?.to_string(),
							name: self.name.to_string(),
						}
						.into());
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
