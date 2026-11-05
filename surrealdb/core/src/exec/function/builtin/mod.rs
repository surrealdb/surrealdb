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
pub(crate) use eval::{evaluate_streaming, run_statements};
mod enumeration;
#[cfg(feature = "files")]
mod file;
mod geo;
mod http;
mod interval;
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
mod sql;
mod string;
mod time;
mod trace;
#[cfg(feature = "scripting")]
pub(crate) use time::timezone_at;
mod r#type;
mod value;
mod vector;

use super::{FunctionRegistry, ScalarFunction};

/// The scalar builtins of [`register_edge`], family by family.
static EDGE_SCALARS: &[&[&dyn ScalarFunction]] = &[
	array::SCALARS,
	bytes::SCALARS,
	count::SCALARS,
	crypto::SCALARS,
	duration::SCALARS,
	enumeration::SCALARS,
	#[cfg(feature = "encoding")]
	encoding::SCALARS,
	geo::SCALARS,
	interval::SCALARS,
	math::SCALARS,
	meta::SCALARS,
	not::SCALARS,
	object::SCALARS,
	parse::SCALARS,
	rand::SCALARS,
	record::SCALARS,
	schema::SCALARS,
	session::SCALARS,
	set::SCALARS,
	sql::SCALARS,
	string::SCALARS,
	time::SCALARS,
	trace::SCALARS,
	r#type::SCALARS,
	value::SCALARS,
	vector::SCALARS,
];

/// The scalar builtins of the families [`register_edge`] leaves out, which [`register_all`]
/// adds, with the stand-ins for the families a build lacks.
static REST_SCALARS: &[&[&dyn ScalarFunction]] = &[
	#[cfg(feature = "api")]
	api::SCALARS,
	#[cfg(not(feature = "api"))]
	disabled::API,
	crypto_async::SCALARS,
	#[cfg(not(feature = "encoding"))]
	disabled::ENCODING,
	eval::SCALARS,
	#[cfg(feature = "files")]
	file::SCALARS,
	#[cfg(not(feature = "files"))]
	disabled::FILES,
	http::SCALARS,
	#[cfg(feature = "search")]
	search::SCALARS,
	#[cfg(not(feature = "search"))]
	disabled::SEARCH,
	sequence::SCALARS,
	sleep::SCALARS,
];

/// Register the built-in functions an entry that serves only data statements resolves.
///
/// Left out are the families that reach beyond the datastore's key-value view or re-enter the
/// executor:
///
/// | family | reason |
/// |--|--|
/// | `eval::*` | re-enters the front end with arbitrary text |
/// | `http::*` | network |
/// | `sequence::*` | sequences |
/// | `sleep` | a sleep holds the statement |
/// | `crypto_async` | password hashing |
/// | `api`, `file`, `search` and the `disabled` stubs | each needs a host feature the entry does not carry |
///
/// A name outside the set is unknown to the binder.
pub fn register_edge(registry: &mut FunctionRegistry) {
	registry.register_scalars(EDGE_SCALARS);
	aggregates::register(registry);
	r#type::register(registry);
	vector::register(registry);
}

/// Register all built-in functions with the registry: the edge set ([`register_edge`]) and every
/// family it leaves out.
pub fn register_all(registry: &mut FunctionRegistry) {
	register_edge(registry);
	registry.register_scalars(REST_SCALARS);
	#[cfg(feature = "search")]
	search::register(registry);
}

/// The scalar tables of one registration: one slice of functions per family.
#[cfg(test)]
pub(super) type ScalarTables = &'static [&'static [&'static dyn ScalarFunction]];

/// The scalar tables of [`register_edge`] and of [`register_all`] beyond it, for the registry
/// tests.
#[cfg(test)]
pub(super) fn scalar_tables() -> (ScalarTables, ScalarTables) {
	(EDGE_SCALARS, REST_SCALARS)
}

/// The tables of the families [`register_edge`] leaves out and that every build carries, for the
/// registry tests.
#[cfg(test)]
pub(super) fn edge_excluded_tables() -> [&'static [&'static dyn ScalarFunction]; 5] {
	[eval::SCALARS, http::SCALARS, sequence::SCALARS, sleep::SCALARS, crypto_async::SCALARS]
}
