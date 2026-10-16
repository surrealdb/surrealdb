//! Geo functions

use crate::define_pure_function;
use crate::exec::function::ScalarFunction;

define_pure_function!(GeoArea, "geo::area", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (geometry: Any) -> Float, crate::fnc::geo::area);
define_pure_function!(GeoBearing, "geo::bearing", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::geo::bearing);
define_pure_function!(GeoCentroid, "geo::centroid", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (geometry: Any) -> Any, crate::fnc::geo::centroid);
define_pure_function!(GeoDistance, "geo::distance", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (a: Any, b: Any) -> Float, crate::fnc::geo::distance);

// Geo hash functions
define_pure_function!(GeoHashDecode, "geo::hash::decode", effects: EffectSet::PURE, fallibility: Fallibility::INFALLIBLE, (hash: Any) -> Any, crate::fnc::geo::hash::decode);
define_pure_function!(GeoHashEncode, "geo::hash::encode", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (point: Any, ?precision: Int) -> String, crate::fnc::geo::hash::encode);

// Geo validation
define_pure_function!(GeoIsValid, "geo::is_valid", effects: EffectSet::PURE, fallibility: Fallibility::VALUE, (geometry: Any) -> Bool, crate::fnc::geo::is::valid);

/// The `geo::*` scalar builtins.
pub(super) const SCALARS: &[&dyn ScalarFunction] = &[
	&GeoArea,
	&GeoBearing,
	&GeoCentroid,
	&GeoDistance,
	&GeoHashDecode,
	&GeoHashEncode,
	&GeoIsValid,
];
