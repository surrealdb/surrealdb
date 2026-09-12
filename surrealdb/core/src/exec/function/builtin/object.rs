//! Object functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(ObjectEntries, "object::entries", effects: EffectSet::PURE, (object: Any) -> Any, crate::fnc::object::entries);
define_pure_function!(ObjectFromEntries, "object::from_entries", effects: EffectSet::PURE, (entries: Any) -> Any, crate::fnc::object::from_entries);
define_pure_function!(ObjectIsEmpty, "object::is_empty", effects: EffectSet::PURE, (object: Any) -> Bool, crate::fnc::object::is_empty);
define_pure_function!(ObjectKeys, "object::keys", effects: EffectSet::PURE, (object: Any) -> Any, crate::fnc::object::keys);
define_pure_function!(ObjectLen, "object::len", effects: EffectSet::PURE, (object: Any) -> Int, crate::fnc::object::len);
define_pure_function!(ObjectValues, "object::values", effects: EffectSet::PURE, (object: Any) -> Any, crate::fnc::object::values);

// Two argument functions
define_pure_function!(ObjectExtend, "object::extend", effects: EffectSet::PURE, (base: Any, extension: Any) -> Any, crate::fnc::object::extend);
define_pure_function!(ObjectRemove, "object::remove", effects: EffectSet::PURE, (object: Any, keys: Any) -> Any, crate::fnc::object::remove);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		ObjectEntries,
		ObjectExtend,
		ObjectFromEntries,
		ObjectIsEmpty,
		ObjectKeys,
		ObjectLen,
		ObjectRemove,
		ObjectValues,
	);
}
