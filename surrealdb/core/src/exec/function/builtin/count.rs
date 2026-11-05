//! Count function

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(Count, "count", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (?value: Any) -> Int, crate::fnc::count::count);

/// The `count` scalar builtin.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&Count];
