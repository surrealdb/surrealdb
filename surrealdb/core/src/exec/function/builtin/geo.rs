//! Geo functions

use crate::exec::function::FunctionRegistry;
use crate::{define_pure_function, register_functions};

define_pure_function!(GeoArea, "geo::area", effects: EffectSet::PURE, (geometry: Any) -> Float, crate::fnc::geo::area);
define_pure_function!(GeoBearing, "geo::bearing", effects: EffectSet::PURE, (a: Any, b: Any) -> Float, crate::fnc::geo::bearing);
define_pure_function!(GeoCentroid, "geo::centroid", effects: EffectSet::PURE, (geometry: Any) -> Any, crate::fnc::geo::centroid);
define_pure_function!(GeoDistance, "geo::distance", effects: EffectSet::PURE, (a: Any, b: Any) -> Float, crate::fnc::geo::distance);

// Geo hash functions
define_pure_function!(GeoHashDecode, "geo::hash::decode", effects: EffectSet::PURE, (hash: Any) -> Any, crate::fnc::geo::hash::decode);
define_pure_function!(GeoHashEncode, "geo::hash::encode", effects: EffectSet::PURE, (point: Any, ?precision: Int) -> String, crate::fnc::geo::hash::encode);

// Geo validation
define_pure_function!(GeoIsValid, "geo::is_valid", effects: EffectSet::PURE, (geometry: Any) -> Bool, crate::fnc::geo::is::valid);

pub fn register(registry: &mut FunctionRegistry) {
	register_functions!(
		registry,
		GeoArea,
		GeoBearing,
		GeoCentroid,
		GeoDistance,
		GeoHashDecode,
		GeoHashEncode,
		GeoIsValid,
	);
}
