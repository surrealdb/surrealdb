use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::expr::{Base, Ident, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Formatter};

#[revisioned(revision = 3)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveDatabaseStatement {
	pub name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
	#[revision(start = 3)]
	pub expunge: bool,
}

impl RemoveDatabaseStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Database, &Base::Ns)?;
			// Get the transaction
			let txn = ctx.tx();
			let ns = opt.ns()?;
			// Remove the index stores
			#[cfg(not(target_family = "wasm"))]
			ctx.get_index_stores()
				.database_removed(ctx.get_index_builder(), &txn, ns, &self.name)
				.await?;
			#[cfg(target_family = "wasm")]
			ctx.get_index_stores().database_removed(&txn, opt.ns()?, &self.name).await?;
			// Remove the sequences
			if let Some(seq) = ctx.get_sequences() {
				seq.database_removed(&txn, ns, &self.name).await?;
			}
			// Get the definition
			let db = txn.get_db(ns, &self.name).await?;
			// Delete the definition
			let key = crate::key::namespace::db::new(ns, &db.name);
			match self.expunge {
				true => txn.clr(key).await?,
				false => txn.del(key).await?,
			};
			// Delete the resource data
			let key = crate::key::database::all::new(ns, &db.name);
			match self.expunge {
				true => txn.clrp(key).await?,
				false => txn.delp(key).await?,
			};
			// Clear the cache
			if let Some(cache) = ctx.get_cache() {
				cache.clear();
			}
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::DbNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
	}
}

crate::expr::impl_display_from_sql!(RemoveDatabaseStatement);

impl crate::expr::DisplaySql for RemoveDatabaseStatement {
	fn fmt_sql(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE DATABASE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
