//! Print functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(PrintLog, "print::log", (message: String) -> None, crate::fnc::print::log);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, PrintLog,);
}
