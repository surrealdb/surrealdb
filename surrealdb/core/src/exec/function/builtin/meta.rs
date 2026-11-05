//! Meta functions (aliases for record functions)

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(MetaId, "meta::id", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (record: Any) -> Any, crate::fnc::record::id);
define_pure_function!(MetaTb, "meta::tb", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (record: Any) -> String, crate::fnc::record::tb);

/// The `meta::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[&MetaId, &MetaTb];
