//! The `string::*` functions, from `surrealdb-runtime`.
//!
//! `string::html::sanitize` needs the `html-sanitize` cargo feature and
//! `string::slug` the `collate` feature. Without one, the stand-in here keeps
//! the function's path resolving and fails each call with an error naming the
//! feature.

pub use surrealdb_runtime::string::*;

#[cfg(not(feature = "collate"))]
crate::fnc::feature::disabled_functions! {
	"collate";
	slug((String,)) => "string::slug";
}

#[cfg(not(feature = "html-sanitize"))]
pub mod html {
	pub use surrealdb_runtime::string::html::encode;

	crate::fnc::feature::disabled_functions! {
		"html-sanitize";
		sanitize((String,)) => "string::html::sanitize";
	}
}
