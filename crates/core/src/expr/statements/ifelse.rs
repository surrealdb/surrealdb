use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::ctx::Context;
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::expr::{Expr, FlowResult, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
pub(crate) struct IfelseStatement {
	/// The first if condition followed by a body, followed by any number of
	/// else if's
	pub exprs: Vec<(Expr, Expr)>,
	/// the final else body, if there is one
	pub close: Option<Expr>,
}

impl IfelseStatement {
	/// Check if we require a writeable transaction
	pub(crate) fn read_only(&self) -> bool {
		self.exprs.iter().all(|x| x.0.read_only() && x.1.read_only())
			&& self.close.as_ref().map(|x| x.read_only()).unwrap_or(true)
	}

	/// Process this type returning a computed simple Value
	#[instrument(level = "trace", name = "IfelseStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &Context,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		for (cond, then) in &self.exprs {
			let v = stk.run(|stk| cond.compute(stk, ctx, opt, doc)).await?;
			if v.is_truthy() {
				return stk.run(|stk| then.compute(stk, ctx, opt, doc)).await;
			}
		}
		match self.close {
			Some(ref v) => stk.run(|stk| v.compute(stk, ctx, opt, doc)).await,
			None => Ok(Value::None),
		}
	}
}

impl ToSql for IfelseStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let stmt: crate::sql::statements::ifelse::IfelseStatement = self.clone().into();
		stmt.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
	use super::*;
	use crate::syn;

	#[test]
	fn format_pretty() {
		let query = syn::expr("IF 1 { 1 } ELSE IF 2 { 2 }").unwrap();
		assert_eq!(query.to_sql(), "IF 1 { 1 } ELSE IF 2 { 2 }");
		// Single-statement blocks stay inline even in pretty mode
		assert_eq!(query.to_sql_pretty(), "IF 1 { 1 } ELSE IF 2 { 2 }");
	}
}
