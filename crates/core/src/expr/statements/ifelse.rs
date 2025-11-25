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
	/// Check if we require a writeable transaction
	#[allow(dead_code)]
	pub(crate) fn bracketed(&self) -> bool {
		self.exprs.iter().all(|(_, v)| matches!(v, Expr::Block(_)))
			&& (self.close.as_ref().is_none()
				|| self.close.as_ref().is_some_and(|v| matches!(v, Expr::Block(_))))
	}
	/// Process this type returning a computed simple Value
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
		let sql_stmt: crate::sql::statements::IfelseStatement = self.clone().into();
		sql_stmt.fmt_sql(f, fmt);
	}
}
