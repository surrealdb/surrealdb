use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Actor, &self.base)?;
		// Check the statement type
		match &self.base {
			Base::Root => {
				// Get the transaction
				let txn = ctx.tx();
				// Get the definition
				let ac = match txn.get_root_access(&self.name).await {
					Ok(x) => x,
					Err(e) => {
						if self.if_exists
							&& matches!(e.downcast_ref(), Some(Error::AccessRootNotFound { .. }))
						{
							return Ok(Value::None);
						} else {
							return Err(e);
						}
					}
				};
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
				let ac = match txn.get_ns_access(opt.ns()?, &self.name).await {
					Ok(x) => x,
					Err(e) => {
						if self.if_exists
							&& matches!(e.downcast_ref(), Some(Error::AccessNsNotFound { .. }))
						{
							return Ok(Value::None);
						} else {
							return Err(e);
						}
					}
				};
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
				let (ns, db) = opt.ns_db()?;
				let ac = match txn.get_db_access(ns, db, &self.name).await {
					Ok(x) => x,
					Err(e) => {
						if self.if_exists
							&& matches!(e.downcast_ref(), Some(Error::AccessDbNotFound { .. }))
						{
							return Ok(Value::None);
						} else {
							return Err(e);
						}
					}
				};
				// Delete the definition
				let key = crate::key::database::ac::new(ns, db, &ac.name);
				txn.del(key).await?;
				// Delete any associated data including access grants.
				let key = crate::key::database::access::all::new(ns, db, &ac.name);
				txn.delp(key).await?;
				// Clear the cache
				txn.clear();
				// Ok all good
				Ok(Value::None)
			}
			_ => Err(anyhow::Error::new(Error::InvalidLevel(self.base.to_string()))),
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
