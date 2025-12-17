use reblessive::tree::Stk;
use surrealdb_types::{SqlFormat, ToSql};

use crate::cnf::PROTECTED_PARAM_NAMES;
use crate::ctx::{Context, FrozenContext};
use crate::dbs::Options;
use crate::doc::CursorDoc;
use crate::err::Error;
use crate::expr::{ControlFlow, Expr, FlowResult, Kind, Value};

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct SetStatement {
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
	#[instrument(level = "trace", name = "SetStatement::compute", skip_all)]
	pub(crate) async fn compute(
		&self,
		stk: &mut Stk,
		ctx: &mut Option<FrozenContext>,
		opt: &Options,
		doc: Option<&CursorDoc>,
	) -> FlowResult<Value> {
		assert!(ctx.is_some(), "SetStatement::compute must be called with a set option.");

		if self.is_protected_set() {
			return Err(ControlFlow::from(anyhow::Error::new(Error::InvalidParam {
				name: self.name.clone(),
			})));
		}

		let result = stk
			.run(|stk| {
				self.what.compute(
					stk,
					ctx.as_ref().expect("context should be initialized"),
					opt,
					doc,
				)
			})
			.await?;
		let result = match &self.kind {
			Some(kind) => result
				.coerce_to_kind(kind)
				.map_err(|e| Error::SetCoerce {
					name: self.name.clone(),
					error: Box::new(e),
				})
				.map_err(anyhow::Error::new)?,
			None => result,
		};

		let mut c = Context::unfreeze(ctx.take().expect("context should be initialized"))?;
		c.add_value(self.name.clone(), result.into());
		*ctx = Some(c.freeze());
		Ok(Value::None)
	}
}

impl ToSql for SetStatement {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		let sql_stmt: crate::sql::statements::SetStatement = self.clone().into();
		sql_stmt.fmt_sql(f, fmt);
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_types::ToSql;

	use crate::syn;

	#[test]
	fn check_type() {
		let query = syn::expr("LET $param = 5").unwrap();
		assert_eq!(query.to_sql(), "LET $param = 5");

		let query = syn::expr("LET $param: number = 5").unwrap();
		assert_eq!(query.to_sql(), "LET $param: number = 5");
	}
}
