use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(Storage, "storage", (value: Any) -> Int, crate::fnc::storage::storage);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, Storage);
}
