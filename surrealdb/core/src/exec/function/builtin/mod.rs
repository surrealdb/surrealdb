//! Built-in scalar and aggregate function definitions.
//!
//! This module registers all built-in functions with the registry.
//! Functions are organized by category (math, string, array, etc.).

pub mod aggregates;
#[cfg(feature = "api")]
mod api;
mod array;
mod bytes;
mod count;
mod crypto;
mod crypto_async;
#[cfg(not(all(
	feature = "api",
	feature = "encoding",
	feature = "files",
	feature = "search",
	feature = "url"
)))]
mod disabled;
mod duration;
#[cfg(feature = "encoding")]
mod encoding;
mod eval;
pub(crate) use eval::evaluate_streaming;
#[cfg(feature = "files")]
mod file;
mod geo;
mod http;
mod math;
mod meta;
mod not;
mod object;
mod parse;
mod rand;
mod record;
mod schema;
#[cfg(feature = "search")]
mod search;
mod sequence;
mod session;
mod set;
mod sleep;
mod string;
mod time;
mod r#type;
mod value;
mod vector;

use super::FunctionRegistry;

/// Register all built-in functions with the registry.
pub fn register_all(registry: &mut FunctionRegistry) {
	// Scalar functions
	#[cfg(feature = "api")]
	api::register(registry);
	#[cfg(not(feature = "api"))]
	disabled::register(
		registry,
		"api",
		&[
			"api::invoke",
			"api::req::body",
			"api::req::max_body",
			"api::res::body",
			"api::res::status",
			"api::res::header",
			"api::res::headers",
			"api::timeout",
		],
	);
	array::register(registry);
	bytes::register(registry);
	count::register(registry);
	crypto::register(registry);
	crypto_async::register(registry);
	duration::register(registry);
	#[cfg(feature = "encoding")]
	encoding::register(registry);
	#[cfg(not(feature = "encoding"))]
	disabled::register(
		registry,
		"encoding",
		&[
			"encoding::base64::decode",
			"encoding::base64::encode",
			"encoding::cbor::decode",
			"encoding::cbor::encode",
			"encoding::json::decode",
			"encoding::json::encode",
		],
	);
	eval::register(registry);
	#[cfg(feature = "files")]
	file::register(registry);
	#[cfg(not(feature = "files"))]
	disabled::register(
		registry,
		"files",
		&[
			"file::put",
			"file::put_if_not_exists",
			"file::get",
			"file::head",
			"file::delete",
			"file::copy",
			"file::copy_if_not_exists",
			"file::rename",
			"file::rename_if_not_exists",
			"file::exists",
			"file::list",
			"file::bucket",
			"file::key",
		],
	);
	geo::register(registry);
	http::register(registry);
	math::register(registry);
	meta::register(registry);
	not::register(registry);
	object::register(registry);
	parse::register(registry);
	rand::register(registry);
	record::register(registry);
	schema::register(registry);
	#[cfg(feature = "search")]
	search::register(registry);
	#[cfg(not(feature = "search"))]
	disabled::register(
		registry,
		"search",
		&[
			"search::analyze",
			"search::rrf",
			"search::linear",
			"search::highlight",
			"search::score",
			"search::offsets",
		],
	);
	sequence::register(registry);
	session::register(registry);
	set::register(registry);
	sleep::register(registry);
	string::register(registry);
	time::register(registry);
	r#type::register(registry);
	value::register(registry);
	vector::register(registry);

	// Aggregate functions
	aggregates::register(registry);
}
