use std::fmt::{self, Display};

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveModelStatement {
	pub name: Ident,
	pub version: String,

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
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let ml = match txn.get_db_model(ns, db, &self.name, &self.version).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				}
				return Err(Error::MlNotFound {
					name: format!("{}<{}>", self.name, self.version),
				}
				.into());
			}
		};
		// Delete the definition
		let key = crate::key::database::ml::new(ns, db, &ml.name, &ml.version);
		txn.del(&key).await?;
		// Clear the cache
		txn.clear_cache();
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
		write!(f, " ml::{}<{}>", &*self.name, self.version)?;
		Ok(())
	}
}
