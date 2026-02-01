//! Set functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

// Single set argument functions
define_pure_function!(SetFirst, "set::first", (set: Any) -> Any, crate::fnc::set::first);
define_pure_function!(SetFlatten, "set::flatten", (set: Any) -> Any, crate::fnc::set::flatten);
define_pure_function!(SetIsEmpty, "set::is_empty", (set: Any) -> Bool, crate::fnc::set::is_empty);
define_pure_function!(SetLast, "set::last", (set: Any) -> Any, crate::fnc::set::last);
define_pure_function!(SetLen, "set::len", (set: Any) -> Int, crate::fnc::set::len);
define_pure_function!(SetMax, "set::max", (set: Any) -> Any, crate::fnc::set::max);
define_pure_function!(SetMin, "set::min", (set: Any) -> Any, crate::fnc::set::min);

// Two argument set functions
define_pure_function!(SetAdd, "set::add", (set: Any, value: Any) -> Any, crate::fnc::set::add);
define_pure_function!(SetAt, "set::at", (set: Any, index: Int) -> Any, crate::fnc::set::at);
define_pure_function!(SetComplement, "set::complement", (a: Any, b: Any) -> Any, crate::fnc::set::complement);
define_pure_function!(SetContains, "set::contains", (set: Any, value: Any) -> Bool, crate::fnc::set::contains);
define_pure_function!(SetDifference, "set::difference", (a: Any, b: Any) -> Any, crate::fnc::set::difference);
define_pure_function!(SetIntersect, "set::intersect", (a: Any, b: Any) -> Any, crate::fnc::set::intersect);
define_pure_function!(SetJoin, "set::join", (set: Any, separator: String) -> String, crate::fnc::set::join);
define_pure_function!(SetRemove, "set::remove", (set: Any, value: Any) -> Any, crate::fnc::set::remove);
define_pure_function!(SetUnion, "set::union", (a: Any, b: Any) -> Any, crate::fnc::set::union);

// Three argument set functions
define_pure_function!(SetSlice, "set::slice", (set: Any, start: Int, ?length: Int) -> Any, crate::fnc::set::slice);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		SetAdd,
		SetAt,
		SetComplement,
		SetContains,
		SetDifference,
		SetFirst,
		SetFlatten,
		SetIntersect,
		SetIsEmpty,
		SetJoin,
		SetLast,
		SetLen,
		SetMax,
		SetMin,
		SetRemove,
		SetSlice,
		SetUnion,
	);
}
