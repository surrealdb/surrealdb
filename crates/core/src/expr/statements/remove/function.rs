use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self};

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFunctionStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveFunctionStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Function, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Get the definition
			let (ns, db) = opt.ns_db()?;
			let fc = txn.get_db_function(ns, db, &self.name).await?;
			// Delete the definition
			let key = crate::key::database::fc::new(ns, db, &fc.name);
			txn.del(key).await?;
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::FcNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

crate::expr::impl_display_from_sql!(RemoveFunctionStatement);

impl crate::expr::DisplaySql for RemoveFunctionStatement {
	fn fmt_sql(&self, f: &mut fmt::Formatter) -> fmt::Result {
		// Bypass ident display since we don't want backticks arround the ident.
		write!(f, "REMOVE FUNCTION")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " fn::{}", self.name.0)?;
		Ok(())
	}
}
