use crate::catalog::TableDefinition;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::statements::define::DefineTableStatement;
use crate::expr::{Base, Ident, Idiom, Value};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};
use uuid::Uuid;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveFieldStatement {
	pub name: Idiom,
	pub table_name: Ident,
	#[revision(start = 2)]
	pub if_exists: bool,
}

impl RemoveFieldStatement {
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Field, &Base::Db)?;
		// Get the NS and DB
		let (ns, db) = ctx.get_ns_db_ids(opt)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the field name
		let name = self.name.to_string();
		// Get the definition
		let fd = match txn.get_tb_field(ns, db, &self.table_name, &name).await? {
			Some(x) => x,
			None => {
				if self.if_exists {
					return Ok(Value::None);
				} else {
					return Err(Error::FdNotFound {
						name: name,
					}.into());
				}
			}
		};
		// Delete the definition
		let key = crate::key::table::fd::new(ns, db, &self.table_name, &name);
		txn.del(&key).await?;
		// Refresh the table cache for fields
		let key = crate::key::database::tb::new(ns, db, &self.table_name);
		let Some(tb) = txn.get_tb(ns, db, &self.table_name).await? else {
			return Err(Error::TbNotFound {
				name: self.table_name.to_string(),
			}.into());
		};

		txn.set(
			&key,
			&TableDefinition {
				cache_fields_ts: Uuid::now_v7(),
				..tb.as_ref().clone()
			},
			None,
		)
		.await?;
		// Clear the cache
		if let Some(cache) = ctx.get_cache() {
			cache.clear_tb(ns, db, &self.table_name);
		}
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveFieldStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE FIELD")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {} ON {}", self.name, self.table_name)?;
		Ok(())
	}
}
