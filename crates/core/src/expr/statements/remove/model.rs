use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
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
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Model, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the defined model
		let (ns, db) = opt.ns_db()?;
		let ml = match txn.get_db_model(ns, db, &self.name, &self.version).await {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::MlNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Delete the definition
		let key = crate::key::database::ml::new(ns, db, &ml.name, &ml.version);
		txn.del(key).await?;
		// Clear the cache
		txn.clear();
		// TODO Remove the model file from storage
		// Ok all good
		Ok(Value::None)
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
