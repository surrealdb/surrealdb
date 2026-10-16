//! Bytes functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(BytesLen, "bytes::len", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Int, crate::fnc::bytes::len);

/// The `bytes::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&BytesLen];
