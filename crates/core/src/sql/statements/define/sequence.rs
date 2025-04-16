use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::statements::info::InfoStructure;
use crate::sql::{Base, Ident, Value};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineSequenceStatement {
	pub name: Ident,
	pub batch: u32,
	pub start: i64,
	pub if_not_exists: bool,
	pub overwrite: bool,
}

impl DefineSequenceStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = opt.ns_db()?;
		// Check if the definition exists
		if txn.get_db_sequence(ns, db, &self.name).await.is_ok() {
			if self.if_not_exists {
				return Ok(Value::None);
			} else if !self.overwrite {
				return Err(Error::SeqAlreadyExists {
					name: self.name.to_string(),
				});
			}
		}
		// Process the statement
		let key = crate::key::database::sq::new(ns, db, &self.name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let sq = DefineSequenceStatement {
			// Don't persist the `IF NOT EXISTS` clause to schema
			if_not_exists: false,
			overwrite: false,
			..self.clone()
		};
		txn.set(key, revision::to_vec(&sq)?, None).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SEQUENCE")?;
		if self.if_not_exists {
			write!(f, " IF NOT EXISTS")?
		}
		if self.overwrite {
			write!(f, " OVERWRITE")?
		}
		write!(f, " {} BATCH {} START {}", self.name, self.batch, self.start)?;
		Ok(())
	}
}

impl InfoStructure for DefineSequenceStatement {
	fn structure(self) -> Value {
		Value::from(map! {
				"name".to_string() => self.name.structure(),
				"batch".to_string() => Value::from(self.batch).structure(),
				"start".to_string() => Value::from(self.start).structure(),
		})
	}
}
