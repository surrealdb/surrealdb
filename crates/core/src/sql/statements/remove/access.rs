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
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
			// Check the statement type
			match &self.base {
				Base::Root => {
					// Get the transaction
					let txn = ctx.tx();
					// Get the definition
					let ac = txn.get_root_access(&self.name).await?;
					// Delete the definition
					let key = crate::key::root::ac::new(&ac.name);
					txn.del(key).await?;
					// Delete any associated data including access grants.
					let key = crate::key::root::access::all::new(&ac.name);
					txn.delp(key).await?;
					// Clear the cache
					txn.clear();
					// Ok all good
					Ok(Value::None)
				}
				Base::Ns => {
					// Get the transaction
					let txn = ctx.tx();
					// Get the definition
					let ac = txn.get_ns_access(opt.ns()?, &self.name).await?;
					// Delete the definition
					let key = crate::key::namespace::ac::new(opt.ns()?, &ac.name);
					txn.del(key).await?;
					// Delete any associated data including access grants.
					let key = crate::key::namespace::access::all::new(opt.ns()?, &ac.name);
					txn.delp(key).await?;
					// Clear the cache
					txn.clear();
					// Ok all good
					Ok(Value::None)
				}
				Base::Db => {
					// Get the transaction
					let txn = ctx.tx();
					// Get the definition
					let ac = txn.get_db_access(opt.ns()?, opt.db()?, &self.name).await?;
					// Delete the definition
					let key = crate::key::database::ac::new(opt.ns()?, opt.db()?, &ac.name);
					txn.del(key).await?;
					// Delete any associated data including access grants.
					let key =
						crate::key::database::access::all::new(opt.ns()?, opt.db()?, &ac.name);
					txn.delp(key).await?;
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
				Error::AccessRootNotFound {
					..
				} => Ok(Value::None),
				Error::AccessNsNotFound {
					..
				} => Ok(Value::None),
				Error::AccessDbNotFound {
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
