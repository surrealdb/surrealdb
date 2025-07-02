use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{ControlFlow, Expr, FlowResult, Kind, Value};

use reblessive::tree::Stk;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::fmt;

#[revisioned(revision = 1)]
#[derive(Clone, Debug,  Eq, PartialEq,  Serialize, Deserialize, Hash)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct SetStatement {
	pub name: String,
	pub what: Expr,
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
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		// Check if the variable is a protected variable
		match PROTECTED_PARAM_NAMES.contains(&self.name.as_str()) {
			// The variable isn't protected and can be stored
			false => {
				let result = self.what.compute(stk, ctx, opt, doc).await?;
				match self.kind {
					Some(ref kind) => result
						.coerce_to_kind(kind)
						.map_err(|e| Error::SetCoerce {
							name: self.name.to_string(),
							error: Box::new(e),
						})
						.map_err(anyhow::Error::new)
						.map_err(ControlFlow::from),
					None => Ok(result),
				}
			}
			// The user tried to set a protected variable
			true => Err(ControlFlow::from(anyhow::Error::new(Error::InvalidParam {
				name: self.name.clone(),
			}))),
		}
	}
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${}", self.name)?;
		if let Some(ref kind) = self.kind {
			write!(f, ": {}", kind)?;
		}
		write!(f, " = {}", self.what)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::syn::parse;

	#[test]
	fn check_type() {
		let query = parse("LET $param = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param = 5;");

		let query = parse("LET $param: number = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param: number = 5;");
	}
}
