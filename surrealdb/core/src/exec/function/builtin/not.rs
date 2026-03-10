//! Not function

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(Not, "not", (value: Any) -> Bool, crate::fnc::not::not);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, Not);
}
