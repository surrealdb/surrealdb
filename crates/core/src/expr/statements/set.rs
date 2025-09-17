use std::fmt;

use reblessive::tree::Stk;

use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::{Context, MutableContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{ControlFlow, Expr, FlowResult, Kind, Value};
use crate::fmt::EscapeKwFreeIdent;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SetStatement {
	pub name: String,
	pub what: Expr,
	pub kind: Option<Kind>,
}

impl SetStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.what.read_only()
	}

	/// returns if the set is setting a protected param.
	pub(crate) fn is_protected_set(&self) -> bool {
		PROTECTED_PARAM_NAMES.contains(&self.name.as_str())
	}

	/// Compute the set statement, must be called with a valid a ctx that is
	/// Some.
	///
	/// Will keep the ctx Some unless an error happens in which case the calling
	/// function should return the error.
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &mut Option<Context>,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		assert!(ctx.is_some(), "SetStatement::compute must be called with a set option.");

		if self.is_protected_set() {
			return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidParam {
				name: self.name.clone(),
			})));
		}

		let result = stk.run(|stk| self.what.compute(stk, ctx.as_ref().unwrap(), opt, doc)).await?;
		let result = match &self.kind {
			Some(kind) => result
				.coerce_to_kind(kind)
				.map_err(|e| Error::SetCoerce {
					name: self.name.to_string(),
					error: Box::new(e),
				})
				.map_err(anyhow::Error::new)?,
			None => result,
		};

		let mut c = MutableContext::unfreeze(ctx.take().unwrap())?;
		c.add_value(self.name.clone(), result.into());
		*ctx = Some(c.freeze());
		Ok(Value::None)
	}
}

impl fmt::Display for SetStatement {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "LET ${}", EscapeKwFreeIdent(&self.name))?;
		if let Some(ref kind) = self.kind {
			write!(f, ": {}", kind)?;
		}
		write!(f, " = {}", self.what)?;
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use crate::syn;

	#[test]
	fn check_type() {
		let query = syn::expr("LET $param = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param = 5");

		let query = syn::expr("LET $param: number = 5").unwrap();
		assert_eq!(format!("{}", query), "LET $param: number = 5");
	}
}
