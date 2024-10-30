use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::define::DefineTableStatement;
use crate::sql::{Base, Ident, Idiom, Value};
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFieldStatement {
	pub name: Idiom,
	pub what: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Get the field name
			let na = self.name.to_string();
			// Get the definition
			let fd = txn.get_tb_field(opt.ns()?, opt.db()?, &self.what, &na).await?;
			// Delete the definition
			let key = crate::key::table::fd::new(opt.ns()?, opt.db()?, &fd.what, &na);
			txn.del(key).await?;
			// Refresh the cache id
			let key = crate::key::database::tb::new(opt.ns()?, opt.db()?, &self.what);
			let tb = txn.get_tb(opt.ns()?, opt.db()?, &self.what).await?;
			txn.set(
				key,
				DefineTableStatement {
					cache_fields_ts: Uuid::now_v7(),
					..tb.as_ref().clone()
				},
				None,
			)
			.await?;
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::FdNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

impl Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.what)?;
		Ok(())
	}
}
