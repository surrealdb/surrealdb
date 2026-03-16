//! Bytes functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(BytesLen, "bytes::len", (value: Any) -> Int, crate::fnc::bytes::len);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, BytesLen);
}
