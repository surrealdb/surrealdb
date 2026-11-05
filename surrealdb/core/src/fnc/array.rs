//! The `array::*` functions, from `surrealdb-runtime`, in a build without the
//! `collate` cargo feature.
//!
//! The stand-ins here keep the transliterating sorts' paths resolving and fail
//! each call with an error naming the feature.

use surrealdb_runtime::args::Optional;
pub use surrealdb_runtime::array::*;

use crate::val::{Array, Value};

crate::fnc::feature::disabled_functions! {
	"collate";
	sort_lexical((Array, Optional<Value>)) => "array::sort_lexical";
	sort_natural_lexical((Array, Optional<Value>)) => "array::sort_natural_lexical";
}

crate::fnc::feature::disabled_walks! {
	"collate";
	sort_lexical_walk((Array, Optional<Value>)) => "array::sort_lexical";
	sort_natural_lexical_walk((Array, Optional<Value>)) => "array::sort_natural_lexical";
}
