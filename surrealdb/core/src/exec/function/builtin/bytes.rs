//! Bytes functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(BytesLen, "bytes::len", effects: EffectSet::PURE, (value: Any) -> Int, crate::fnc::bytes::len);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(registry, BytesLen);
}
