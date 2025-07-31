use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::statements::info::InfoStructure;
use crate::expr::{Base, Ident, Timeout, Value};
use crate::iam::{Action, ResourceKind};
use crate::kvs::impl_kv_value_revisioned;
use anyhow::{Result, bail};

use crate::key::database::sq::Sq;
use crate::key::sequence::Prefix;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display};

use super::DefineKind;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct DefineSequenceStatement {
	pub kind: DefineKind,
	pub name: Ident,
	pub batch: u32,
	pub start: i64,
	pub timeout: Option<Timeout>,
}

impl_kv_value_revisioned!(DefineSequenceStatement);

impl DefineSequenceStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Fetch the transaction
		let txn = ctx.tx();
		let (ns, db) = opt.ns_db()?;
		// Check if the definition exists
		if txn.get_db_sequence(ns, db, &self.name).await.is_ok() {
			match self.kind {
				DefineKind::Default => {
					if !opt.import {
						bail!(Error::SeqAlreadyExists {
							name: self.name.to_string(),
						});
					}
				}
				DefineKind::Overwrite => {}
				DefineKind::IfNotExists => {
					return Ok(Value::None);
				}
			}
		}
		// Process the statement
		let key = Sq::new(ns, db, &self.name);
		txn.get_or_add_ns(ns, opt.strict).await?;
		txn.get_or_add_db(ns, db, opt.strict).await?;
		let sq = DefineSequenceStatement {
			// Don't persist the `IF NOT EXISTS` clause to schema
			kind: DefineKind::Default,
			..self.clone()
		};
		// Set the definition
		txn.set(&key, &sq, None).await?;
		// Clear any pre-existing sequence records
		let ba_range = Prefix::new_ba_range(ns, db, &sq.name)?;
		txn.delr(ba_range).await?;
		let st_range = Prefix::new_st_range(ns, db, &sq.name)?;
		txn.delr(st_range).await?;
		// Clear the cache
		txn.clear_cache();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for DefineSequenceStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "DEFINE SEQUENCE")?;
		match self.kind {
			DefineKind::Default => {}
			DefineKind::Overwrite => write!(f, " OVERWRITE")?,
			DefineKind::IfNotExists => write!(f, " IF NOT EXISTS")?,
		}
		write!(f, " {} BATCH {} START {}", self.name, self.batch, self.start)?;
		if let Some(ref v) = self.timeout {
			write!(f, " {v}")?
		}
		Ok(())
	}
}

impl InfoStructure for DefineSequenceStatement {
	fn structure(self) -> Value {
		Value::from(map! {
				"name".to_string() => self.name.structure(),
				"batch".to_string() => Value::from(self.batch).structure(),
				"start".to_string() => Value::from(self.start).structure(),
				"timeout".to_string() => self.timeout.as_ref().map(|t|t.0.into()).unwrap_or(Value::None),
		})
	}
}
