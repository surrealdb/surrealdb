//! Count function

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(Count, "count", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (?value: Any) -> Int, crate::fnc::count::count);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, Count);
}
