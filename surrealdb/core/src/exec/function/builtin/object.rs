//! Object functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(ObjectEntries, "object::entries", (object: Any) -> Any, crate::fnc::object::entries);
define_pure_function!(ObjectFromEntries, "object::from_entries", (entries: Any) -> Any, crate::fnc::object::from_entries);
define_pure_function!(ObjectIsEmpty, "object::is_empty", (object: Any) -> Bool, crate::fnc::object::is_empty);
define_pure_function!(ObjectKeys, "object::keys", (object: Any) -> Any, crate::fnc::object::keys);
define_pure_function!(ObjectLen, "object::len", (object: Any) -> Int, crate::fnc::object::len);
define_pure_function!(ObjectValues, "object::values", (object: Any) -> Any, crate::fnc::object::values);

// Two argument functions
define_pure_function!(ObjectExtend, "object::extend", (base: Any, extension: Any) -> Any, crate::fnc::object::extend);
define_pure_function!(ObjectRemove, "object::remove", (object: Any, keys: Any) -> Any, crate::fnc::object::remove);

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
