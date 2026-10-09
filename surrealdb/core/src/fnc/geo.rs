//! The `geo::*` functions, from `surrealdb-runtime`, in a build without the
//! `geo` cargo feature.
//!
//! The stand-ins here keep the measuring functions' paths resolving and fail
//! each call with an error naming the feature.

pub use surrealdb_runtime::geo::*;

use crate::val::Geometry;

crate::fnc::feature::disabled_functions! {
	"geo";
	area((Geometry,)) => "geo::area";
	bearing((Geometry, Geometry)) => "geo::bearing";
	centroid((Geometry,)) => "geo::centroid";
	distance((Geometry, Geometry)) => "geo::distance";
}
