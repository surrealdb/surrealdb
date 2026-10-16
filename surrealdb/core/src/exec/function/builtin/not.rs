//! Not function

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(Not, "not", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (value: Any) -> Bool, crate::fnc::not::not);

/// The `not` scalar builtin.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&Not];
