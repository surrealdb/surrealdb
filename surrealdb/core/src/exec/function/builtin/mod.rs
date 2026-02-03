//! Built-in scalar and aggregate function definitions.
//!
//! This module registers all built-in functions with the registry.
//! Functions are organized by category (math, string, array, etc.).

pub mod aggregates;
mod array;
mod bytes;
mod count;
mod crypto;
mod crypto_async;
mod duration;
mod encoding;
mod geo;
mod http;
mod math;
mod meta;
mod not;
mod object;
mod parse;
mod rand;
mod record;
mod search;
mod session;
mod set;
mod sleep;
mod string;
mod time;
mod r#type;
mod vector;

use super::FunctionRegistry;

/// Register all built-in functions with the registry.
pub fn register_all(registry: &mut FunctionRegistry) {
	// Scalar functions
	array::register(registry);
	bytes::register(registry);
	count::register(registry);
	crypto::register(registry);
	crypto_async::register(registry);
	duration::register(registry);
	encoding::register(registry);
	geo::register(registry);
	http::register(registry);
	math::register(registry);
	meta::register(registry);
	not::register(registry);
	object::register(registry);
	parse::register(registry);
	rand::register(registry);
	record::register(registry);
	search::register(registry);
	session::register(registry);
	set::register(registry);
	sleep::register(registry);
	string::register(registry);
	time::register(registry);
	r#type::register(registry);
	vector::register(registry);

	// Aggregate functions
	aggregates::register(registry);
}
