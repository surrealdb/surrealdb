use crate::ctx::Context;
use crate::dbs::{Options, Transaction};
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::Value;
use derive::Store;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[revisioned(revision = 1)]
#[non_exhaustive]
pub struct ThrowStatement {
	pub error: Value,
}

impl ThrowStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		false
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		Err(Error::Thrown(self.error.compute(ctx, opt, txn, doc).await?.to_raw_string()))
	}
}

impl fmt::Display for ThrowStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "THROW {}", self.error)
	}
}
