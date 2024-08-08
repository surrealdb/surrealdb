use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::sql::Value;
use crate::{cnf::PROTECTED_PARAM_NAMES, sql::Kind};
use derive::Store;
use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 2)]
#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Store, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct SetStatement {
	pub name: String,
	pub what: Value,
	#[revision(start = 2)]
	pub kind: Option<Kind>,
}

impl SetStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn writeable(&self) -> bool {
		self.what.writeable()
	}
	/// Process this type returning a computed simple Value
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context<'_>,
		opt: &Options,
		doc: Option<&CursorDoc<'_>>,
	) -> Result<Value, Error> {
		// Check if the variable is a protected variable
		match PROTECTED_PARAM_NAMES.contains(&self.name.as_str()) {
			// The variable isn't protected and can be stored
			false => {
				let result = self.what.compute(stk, ctx, opt, doc).await?;
				match self.kind {
					Some(ref kind) => result
						.coerce_to(kind)
						.map_err(|e| e.set_check_from_coerce(self.name.to_string())),
					None => Ok(result),
				}
			}
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
