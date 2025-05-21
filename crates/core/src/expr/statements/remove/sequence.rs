use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::expr::{Base, Ident, Value};
use crate::iam::{Action, ResourceKind};
use anyhow::Result;

use crate::key::database::sq::Sq;
use crate::key::sequence::Prefix;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt::{self, Display, Formatter};

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct RemoveSequenceStatement {
	pub name: Ident,
	pub if_exists: bool,
}

impl RemoveSequenceStatement {
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value> {
		let (ns, db) = opt.ns_db()?;
		// Allowed to run?
		opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
		// Get the transaction
		let txn = ctx.tx();
		// Get the definition
		let sq = match txn.get_db_sequence(ns, db, &self.name).await {
			Ok(x) => x,
			Err(e) => {
				if self.if_exists && matches!(e.downcast_ref(), Some(Error::SeqNotFound { .. })) {
					return Ok(Value::None);
				} else {
					return Err(e);
				}
			}
		};
		// Remove the sequence
		if let Some(seq) = ctx.get_sequences() {
			seq.sequence_removed(ns, db, &self.name);
		}
		// Delete any sequence records
		let (beg, end) = Prefix::new_ba_range(ns, db, &sq.name)?;
		txn.delr(beg..end).await?;
		let (beg, end) = Prefix::new_st_range(ns, db, &sq.name)?;
		txn.delr(beg..end).await?;
		// Delete the definition
		let key = Sq::new(ns, db, &sq.name);
		txn.del(key).await?;
		// Clear the cache
		txn.clear();
		// Ok all good
		Ok(Value::None)
	}
}

impl Display for RemoveSequenceStatement {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		write!(f, "REMOVE SEQUENCE")?;
		if self.if_exists {
			write!(f, " IF EXISTS")?
		}
		write!(f, " {}", self.name)?;
		Ok(())
	}
}
