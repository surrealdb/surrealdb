use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveBucketStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl RemoveBucketStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Bucket, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Get the definition
			let (ns, db) = opt.ns_db()?;
			let bu = txn.get_db_bucket(ns, db, &self.name).await?;
			// Delete the definition
			let key = crate::key::database::bu::new(ns, db, &bu.name);
			txn.del(key).await?;
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::BuNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

crate::expr::impl_display_from_sql!(RemoveBucketStatement);

impl crate::expr::DisplaySql for RemoveBucketStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE BUCKET")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
