//! The registry as the oracle the logical layer's effect fold consults.
//!
//! `effects_of` (`crate::expr::logical`) derives what an expression does from
//! the declared effects of the builtins it calls. Those declarations live on
//! the registry, so this is where the fold and the registry meet; every
//! predicate the rules and the physical planner ask of an expression is a
//! field of the fold's answer, read through one of the two oracles here.

use std::collections::HashSet;

use crate::exec::function::FunctionRegistry;
use crate::expr::logical::{EffectOracle, EffectSet};

/// The registry's declarations. Every parameter read is taken to be in hand,
/// so `Effects::may_raise` is not the question to ask through this one; see
/// [`ParamsOnly`].
pub(super) struct Oracle<'a> {
	registry: &'a FunctionRegistry,
}

impl<'a> Oracle<'a> {
	pub(super) fn new(registry: &'a FunctionRegistry) -> Self {
		Self {
			registry,
		}
	}
}

impl EffectOracle for Oracle<'_> {
	fn builtin(&self, name: &str) -> Option<EffectSet> {
		self.registry.get(name).map(|function| function.effects())
	}

	fn method_always_dispatches(&self, name: &str) -> bool {
		self.registry.get_method(name).is_some_and(|d| d.dispatches_for_an_object())
	}

	fn evaluates_against_row(&self, name: &str) -> bool {
		self.registry.is_projection(name)
	}

	fn permission_checked_param(&self, _name: &str) -> bool {
		false
	}
}

/// The bind pass's classification of which parameter reads fetch a `DEFINE
/// PARAM` judged per read, and nothing of the registry: what `may_raise` is
/// asked with, by callers that hold no registry. `Effects::may_raise`
/// consults nothing else of the oracle; the other fields of an `Effects`
/// computed through this are not answers and are not read.
pub(super) struct ParamsOnly<'a>(pub(super) &'a HashSet<String>);

impl EffectOracle for ParamsOnly<'_> {
	fn builtin(&self, _name: &str) -> Option<EffectSet> {
		None
	}

	fn method_always_dispatches(&self, _name: &str) -> bool {
		false
	}

	fn evaluates_against_row(&self, _name: &str) -> bool {
		false
	}

	fn permission_checked_param(&self, name: &str) -> bool {
		self.0.contains(name)
	}
}
