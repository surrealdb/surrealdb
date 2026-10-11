//! A visitor pass that collects the parameter bindings an expression references
//! from a context, so a closure carries only the variables it actually uses.

use std::convert::Infallible;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;

use crate::ctx::FrozenContext;
use crate::expr::Param;
use crate::expr::variables::Variables;
use crate::expr::visit::Visitor;

/// A visitor pass which will capture the value of parameters in the visited expression from the
/// context.
pub(crate) struct ParameterCapturePass<'a, 'b> {
	pub context: &'a FrozenContext,
	pub captures: &'b mut Variables,
}

impl Visitor for ParameterCapturePass<'_, '_> {
	type Error = Infallible;

	fn visit_param(&mut self, param: &Param) -> Result<(), Self::Error> {
		if !PROTECTED_PARAM_NAMES.contains(&param.as_str())
			&& let Some(v) = self.context.value(param.as_str())
		{
			self.captures.0.entry(param.as_str().into()).or_insert_with(|| v.clone());
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::*;
	use crate::ctx::Context;
	use crate::expr::visit::Visit;
	use crate::val::Value;

	/// The names `src` captures from a context binding each of `bound`.
	fn captured(src: &str, bound: &[&str]) -> Vec<String> {
		let mut ctx = Context::new_test();
		for name in bound {
			ctx.add_value(name.to_string(), Arc::new(Value::from(1)));
		}
		let ctx = ctx.freeze();
		let expr = crate::syn::expr(src).expect("expression should parse");
		let mut captures = Variables::default();
		let _ = expr.visit(&mut ParameterCapturePass {
			context: &ctx,
			captures: &mut captures,
		});
		let mut names: Vec<String> = captures.0.keys().map(|k| k.to_string()).collect();
		names.sort();
		names
	}

	/// Every clause of a nested statement is walked, so a parameter read only
	/// in a `SELECT`'s `START` or `TIMEOUT`, or an `UPDATE`'s or `UPSERT`'s
	/// `TIMEOUT`, is captured.
	#[test]
	fn captures_every_statement_clause() {
		let bound = ["st", "to"];
		assert_eq!(captured("(SELECT * FROM t START $st TIMEOUT $to)", &bound), bound);
		assert_eq!(captured("(UPDATE t SET a = 1 TIMEOUT $to)", &bound), ["to"]);
		assert_eq!(captured("(UPSERT t SET a = 1 TIMEOUT $to)", &bound), ["to"]);
	}
}
