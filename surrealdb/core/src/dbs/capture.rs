//! Captures the parameter bindings a closure body references at evaluation
//! time, so `Closure::Expr` carries only the variables it actually uses.

use std::convert::Infallible;

use surrealdb_cnf::PROTECTED_PARAM_NAMES;

use crate::ctx::FrozenContext;
use crate::expr::Param;
use crate::expr::variables::Variables;
use crate::expr::visit::{Visit, Visitor};

/// A visitor pass which will capture the value of parameters in the visited expression from the
/// context.
pub(crate) struct ParameterCapturePass<'a, 'b> {
	pub context: &'a FrozenContext,
	pub captures: &'b mut Variables,
}

impl ParameterCapturePass<'_, '_> {
	pub fn capture<V: for<'a, 'b> Visit<ParameterCapturePass<'a, 'b>>>(
		context: &FrozenContext,
		v: &V,
	) -> Variables {
		let mut captures = Variables::new();

		let _ = v.visit(&mut ParameterCapturePass {
			context,
			captures: &mut captures,
		});

		captures
	}
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
