//! Built-in scalar function definitions.
//!
//! This module registers all built-in pure scalar functions with the registry.
//! Functions are organized by category (math, string, array, etc.).

mod array;
mod bytes;
mod count;
mod crypto;
mod duration;
mod encoding;
mod geo;
mod math;
mod meta;
mod not;
mod object;
mod parse;
mod rand;
mod record;
mod set;
mod string;
mod time;
mod r#type;
mod vector;

use super::FunctionRegistry;

/// Register all built-in functions with the registry.
pub fn register_all(registry: &mut FunctionRegistry) {
	array::register(registry);
	bytes::register(registry);
	count::register(registry);
	crypto::register(registry);
	duration::register(registry);
	encoding::register(registry);
	geo::register(registry);
	math::register(registry);
	meta::register(registry);
	not::register(registry);
	object::register(registry);
	parse::register(registry);
	rand::register(registry);
	record::register(registry);
	set::register(registry);
	string::register(registry);
	time::register(registry);
	r#type::register(registry);
	vector::register(registry);
}
