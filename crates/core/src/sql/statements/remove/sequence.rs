use crate::ctx::Context;
use crate::dbs::Options;
use crate::err::Error;
use crate::iam::{Action, ResourceKind};
use crate::sql::{Base, Ident, Value};

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
	pub(crate) async fn compute(&self, ctx: &Context, opt: &Options) -> Result<Value, Error> {
		let future = async {
			let (ns, db) = opt.ns_db()?;
			// Allowed to run?
			opt.is_allowed(Action::Edit, ResourceKind::Sequence, &Base::Db)?;
			// Get the transaction
			let txn = ctx.tx();
			// Get the definition
			let sq = txn.get_db_sequence(ns, db, &self.name).await?;
			// Remove the sequence
			if let Some(seq) = ctx.get_sequences() {
				seq.sequence_removed(ns, db, &self.name);
			}
			// Delete the definition
			let key = crate::key::database::sq::new(ns, db, &sq.name);
			txn.del(key).await?;
			// Clear the cache
			txn.clear();
			// Ok all good
			Ok(Value::None)
		}
		.await;
		match future {
			Err(Error::SeqNotFound {
				..
			}) if self.if_exists => Ok(Value::None),
			v => v,
		}
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
