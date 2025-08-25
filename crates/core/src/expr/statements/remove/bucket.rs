use std::fmt::{self, Display, Formatter};

use anyhow::Result;

use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub struct RemoveBucketStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl RemoveBucketStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let (ns, db) = ctx.expect_ns_db_ids(opt).await?;
		let Some(bu) = txn.get_db_bucket(ns, db, &self.name).await? else {
			if self.if_exists {
				return Ok(Value::None);
			} else {
				return Err(Error::BuNotFound {
					name: self.name.to_raw_string(),
				}
				.into());
			}
		};

		// Delete the definition
		let key = crate::key::database::bu::new(ns, db, &bu.name);
		txn.del(&key).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveBucketStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE BUCKET")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
