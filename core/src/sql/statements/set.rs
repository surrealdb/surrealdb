use crate::cnf::PROTECTED_PARAM_NAMES;
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
#[revisioned(revision = 1)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct SetStatement {
	pub name: String,
	pub what: Value,
}

impl SetStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		ctx: &Context<'_>,
		opt: &Options,
		txn: &Transaction,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Check if the variable is a protected variable
		match PROTECTED_PARAM_NAMES.contains(&self.name.as_str()) {
			// The variable isn't protected and can be stored
			false => self.what.compute(ctx, opt, txn, doc).await,
			// The user tried to set a protected variable
			true => Err(Error::InvalidParam {
				// Move the parameter name, as we no longer need it
				name: self.name.to_owned(),
			}),
		}
	}
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${} = {}", self.name, self.what)
	}
}
