//! The `sql::*` functions the SQL front ends lower into. They are registered for plans and have
//! no SurrealQL syntax.

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(SqlNetworkKey, "sql::network_key", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::sql::network_key);
define_pure_function!(SqlNetworkText, "sql::network_text", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any, ty: Any) -> Any, crate::fnc::sql::network_text);
define_pure_function!(SqlRangeLower, "sql::range_lower", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::sql::range_lower);
define_pure_function!(SqlRangeUpper, "sql::range_upper", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any) -> Any, crate::fnc::sql::range_upper);
define_pure_function!(SqlRangeOverlaps, "sql::range_overlaps", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Any, crate::fnc::sql::range_overlaps);
define_pure_function!(SqlRangeContains, "sql::range_contains", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (outer: Any, inner: Any) -> Any, crate::fnc::sql::range_contains);
define_pure_function!(SqlText, "sql::text", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any, ty: Any) -> Any, crate::fnc::sql::text);
define_pure_function!(SqlFit, "sql::fit", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (value: Any, ty: Any, dialect: Any) -> Any, crate::fnc::sql::fit);

/// The `sql::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&SqlFit,
	&SqlNetworkKey,
	&SqlNetworkText,
	&SqlRangeContains,
	&SqlRangeLower,
	&SqlRangeOverlaps,
	&SqlRangeUpper,
	&SqlText,
];
