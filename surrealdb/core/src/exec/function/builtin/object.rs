//! Object functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(ObjectEntries, "object::entries", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::object::entries_walk, (object: Any) -> Any, crate::fnc::object::entries);
define_pure_function!(ObjectFromEntries, "object::from_entries", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::object::from_entries_walk, (entries: Any) -> Any, crate::fnc::object::from_entries);
define_pure_function!(ObjectIsEmpty, "object::is_empty", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (object: Any) -> Bool, crate::fnc::object::is_empty);
define_pure_function!(ObjectKeys, "object::keys", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::object::keys_walk, (object: Any) -> Any, crate::fnc::object::keys);
define_pure_function!(ObjectLen, "object::len", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (object: Any) -> Int, crate::fnc::object::len);
define_pure_function!(ObjectValues, "object::values", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::object::values_walk, (object: Any) -> Any, crate::fnc::object::values);

// Two argument functions
define_pure_function!(ObjectExtend, "object::extend", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::object::extend_walk, (base: Any, extension: Any) -> Any, crate::fnc::object::extend);
define_pure_function!(ObjectRemove, "object::remove", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, metered: crate::fnc::object::remove_walk, (object: Any, keys: Any) -> Any, crate::fnc::object::remove);

/// The `object::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&ObjectEntries,
	&ObjectExtend,
	&ObjectFromEntries,
	&ObjectIsEmpty,
	&ObjectKeys,
	&ObjectLen,
	&ObjectRemove,
	&ObjectValues,
];
