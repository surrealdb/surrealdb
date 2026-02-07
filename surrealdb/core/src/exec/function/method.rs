//! Method registry for value method dispatch.
//!
//! Methods are syntactic sugar for function calls invoked with dot syntax on values,
//! e.g. `"hello".len()` calls `string::len("hello")`. This module provides a declarative
//! registry that maps method names to their per-type [`ScalarFunction`] implementations.
//!
//! Methods fall into two categories:
//! - **Generic methods** work on all value types via a single fallback function (e.g.
//!   `.to_string()` → `type::string`, `.type_of()` → `type::of`)
//! - **Type-specific methods** dispatch to different functions based on value type (e.g. `.len()` →
//!   `string::len` on strings, `array::len` on arrays)
//!
//! The registry is built at startup from the [`FunctionRegistry`] and stored alongside it.
//! Methods are resolved at plan time to a [`MethodDescriptor`], which is stored in the
//! physical plan. At evaluation time, the descriptor resolves to the correct function
//! for the runtime value type with no string construction or HashMap lookup.

use std::collections::HashMap;
use std::sync::Arc;

use super::{FunctionRegistry, ScalarFunction};
use crate::val::Value;

// ============================================================================
// ValueKind - coarse type classification for method dispatch
// ============================================================================

/// Coarse value type classification for method dispatch.
///
/// This groups Value variants into the categories used by the old method dispatch.
/// For example, all Number variants (Int, Float, Decimal) map to `Number`,
/// and all Geometry variants map to `Geometry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ValueKind {
	String,
	Array,
	Set,
	Object,
	Bytes,
	Duration,
	Datetime,
	Number,
	Geometry,
	Record,
	File,
}

impl ValueKind {
	/// Extract the coarse kind from a Value, if it has type-specific methods.
	///
	/// Returns `None` for types that only support generic methods
	/// (None, Null, Bool, Uuid, Closure, Regex, Range, Table).
	pub fn of(value: &Value) -> Option<Self> {
		match value {
			Value::String(_) => Some(ValueKind::String),
			Value::Array(_) => Some(ValueKind::Array),
			Value::Set(_) => Some(ValueKind::Set),
			Value::Object(_) => Some(ValueKind::Object),
			Value::Bytes(_) => Some(ValueKind::Bytes),
			Value::Duration(_) => Some(ValueKind::Duration),
			Value::Datetime(_) => Some(ValueKind::Datetime),
			Value::Number(_) => Some(ValueKind::Number),
			Value::Geometry(_) => Some(ValueKind::Geometry),
			Value::RecordId(_) => Some(ValueKind::Record),
			Value::File(_) => Some(ValueKind::File),
			_ => None,
		}
	}
}

// ============================================================================
// MethodDescriptor
// ============================================================================

/// Describes how a method dispatches to functions based on value type.
///
/// At evaluation time, `resolve()` checks `type_impls` first (in order),
/// then falls back to `fallback`. This allows generic methods to work on all
/// types while still permitting type-specific overrides.
#[derive(Debug, Clone)]
pub struct MethodDescriptor {
	/// The method name (for error messages).
	pub name: &'static str,
	/// Per-type implementations. Checked in order; first matching kind wins.
	pub type_impls: Vec<(ValueKind, Arc<dyn ScalarFunction>)>,
	/// Fallback function used when no type-specific impl matches.
	/// This is the typical path for generic methods (to_string, type_of, etc.).
	pub fallback: Option<Arc<dyn ScalarFunction>>,
}

impl MethodDescriptor {
	/// Resolve the method to a concrete function for the given value.
	///
	/// Checks type-specific implementations first, then falls back to the
	/// generic fallback. Returns an error if no implementation is found.
	pub fn resolve(&self, value: &Value) -> anyhow::Result<&Arc<dyn ScalarFunction>> {
		let kind = ValueKind::of(value);

		// Check type-specific implementations first
		if let Some(kind) = kind {
			for (k, func) in &self.type_impls {
				if *k == kind {
					return Ok(func);
				}
			}
		}

		// Fall back to generic implementation
		if let Some(ref func) = self.fallback {
			return Ok(func);
		}

		Err(anyhow::anyhow!(
			"Method '{}' cannot be called on value of type '{}'",
			self.name,
			value.kind_of()
		))
	}
}

// ============================================================================
// MethodRegistry
// ============================================================================

/// Registry of all methods callable on values via dot syntax.
///
/// Built at startup from the [`FunctionRegistry`], this maps method names
/// to their [`MethodDescriptor`]s. The planner resolves method names to
/// descriptors at plan time for efficient evaluation.
#[derive(Debug)]
pub struct MethodRegistry {
	methods: HashMap<&'static str, Arc<MethodDescriptor>>,
}

impl Default for MethodRegistry {
	fn default() -> Self {
		Self {
			methods: HashMap::new(),
		}
	}
}

impl MethodRegistry {
	/// Look up a method by name.
	pub fn get(&self, name: &str) -> Option<&Arc<MethodDescriptor>> {
		self.methods.get(name)
	}

	/// Register a method that works on all types via a single fallback function.
	///
	/// If a descriptor already exists for this method (e.g. from a prior
	/// `register_typed` call), the fallback is set on the existing descriptor.
	fn register_generic(&mut self, method: &'static str, func: Arc<dyn ScalarFunction>) {
		let entry = self.methods.entry(method).or_insert_with(|| {
			Arc::new(MethodDescriptor {
				name: method,
				type_impls: Vec::new(),
				fallback: None,
			})
		});
		let desc = Arc::make_mut(entry);
		desc.fallback = Some(func);
	}

	/// Register a type-specific implementation for a method.
	///
	/// Creates the descriptor if it doesn't exist. Multiple calls with different
	/// `value_kind` values build up the type_impls list.
	fn register_typed(
		&mut self,
		method: &'static str,
		value_kind: ValueKind,
		func: Arc<dyn ScalarFunction>,
	) {
		let entry = self.methods.entry(method).or_insert_with(|| {
			Arc::new(MethodDescriptor {
				name: method,
				type_impls: Vec::new(),
				fallback: None,
			})
		});
		let desc = Arc::make_mut(entry);
		desc.type_impls.push((value_kind, func));
	}

	/// Register an alias: `alias` resolves to the same descriptor as `target`.
	///
	/// The target method must already be registered. The alias gets a clone
	/// of the target's descriptor (with the alias name for error messages).
	fn register_alias(&mut self, alias: &'static str, target: &str) {
		if let Some(target_desc) = self.methods.get(target).cloned() {
			// Clone the descriptor but with the alias name
			let mut desc = (*target_desc).clone();
			desc.name = alias;
			self.methods.insert(alias, Arc::new(desc));
		}
	}
}

// ============================================================================
// build_method_registry
// ============================================================================

/// Helper to look up a function from the registry, panicking if not found.
/// Since these are all builtins, a missing function indicates a registration bug.
fn get(funcs: &FunctionRegistry, name: &str) -> Arc<dyn ScalarFunction> {
	funcs
		.get(name)
		.unwrap_or_else(|| panic!("Expected builtin function '{}' to be registered", name))
		.clone()
}

/// Build the method registry from the function registry.
///
/// This creates descriptors for all methods callable via dot syntax on values,
/// organized into generic methods (available on all types), universal methods,
/// and type-specific methods.
pub fn build_method_registry(funcs: &FunctionRegistry) -> MethodRegistry {
	let mut m = MethodRegistry::default();

	// =====================================================================
	// Generic methods - type conversion (available on all value types)
	// These map to type::* functions and work regardless of value type.
	// =====================================================================
	m.register_generic("to_array", get(funcs, "type::array"));
	m.register_generic("to_bool", get(funcs, "type::bool"));
	m.register_generic("to_bytes", get(funcs, "type::bytes"));
	m.register_generic("to_datetime", get(funcs, "type::datetime"));
	m.register_generic("to_decimal", get(funcs, "type::decimal"));
	m.register_generic("to_duration", get(funcs, "type::duration"));
	m.register_generic("to_float", get(funcs, "type::float"));
	m.register_generic("to_geometry", get(funcs, "type::geometry"));
	m.register_generic("to_int", get(funcs, "type::int"));
	m.register_generic("to_number", get(funcs, "type::number"));
	m.register_generic("to_point", get(funcs, "type::point"));
	m.register_generic("to_range", get(funcs, "type::range"));
	m.register_generic("to_record", get(funcs, "type::record"));
	m.register_generic("to_set", get(funcs, "type::set"));
	m.register_generic("to_string", get(funcs, "type::string"));
	m.register_generic("to_string_lossy", get(funcs, "type::string_lossy"));
	m.register_generic("to_uuid", get(funcs, "type::uuid"));

	// =====================================================================
	// Generic methods - type checking (available on all value types)
	// These map to type::is_* functions.
	// =====================================================================
	m.register_generic("type_of", get(funcs, "type::of"));
	m.register_generic("is_array", get(funcs, "type::is_array"));
	m.register_generic("is_bool", get(funcs, "type::is_bool"));
	m.register_generic("is_bytes", get(funcs, "type::is_bytes"));
	m.register_generic("is_collection", get(funcs, "type::is_collection"));
	m.register_generic("is_datetime", get(funcs, "type::is_datetime"));
	m.register_generic("is_decimal", get(funcs, "type::is_decimal"));
	m.register_generic("is_duration", get(funcs, "type::is_duration"));
	m.register_generic("is_float", get(funcs, "type::is_float"));
	m.register_generic("is_geometry", get(funcs, "type::is_geometry"));
	m.register_generic("is_int", get(funcs, "type::is_int"));
	m.register_generic("is_line", get(funcs, "type::is_line"));
	m.register_generic("is_none", get(funcs, "type::is_none"));
	m.register_generic("is_null", get(funcs, "type::is_null"));
	m.register_generic("is_multiline", get(funcs, "type::is_multiline"));
	m.register_generic("is_multipoint", get(funcs, "type::is_multipoint"));
	m.register_generic("is_multipolygon", get(funcs, "type::is_multipolygon"));
	m.register_generic("is_number", get(funcs, "type::is_number"));
	m.register_generic("is_object", get(funcs, "type::is_object"));
	m.register_generic("is_point", get(funcs, "type::is_point"));
	m.register_generic("is_polygon", get(funcs, "type::is_polygon"));
	m.register_generic("is_range", get(funcs, "type::is_range"));
	m.register_generic("is_record", get(funcs, "type::is_record"));
	m.register_generic("is_set", get(funcs, "type::is_set"));
	m.register_generic("is_string", get(funcs, "type::is_string"));
	m.register_generic("is_uuid", get(funcs, "type::is_uuid"));

	// =====================================================================
	// Universal methods (available on all value types)
	// =====================================================================
	m.register_generic("chain", get(funcs, "value::chain"));
	m.register_generic("diff", get(funcs, "value::diff"));
	m.register_generic("patch", get(funcs, "value::patch"));
	m.register_generic("repeat", get(funcs, "array::repeat"));

	// =====================================================================
	// String methods
	// =====================================================================
	m.register_typed("capitalize", ValueKind::String, get(funcs, "string::capitalize"));
	m.register_typed("concat", ValueKind::String, get(funcs, "string::concat"));
	m.register_typed("contains", ValueKind::String, get(funcs, "string::contains"));
	m.register_typed("ends_with", ValueKind::String, get(funcs, "string::ends_with"));
	m.register_typed("join", ValueKind::String, get(funcs, "string::join"));
	m.register_typed("len", ValueKind::String, get(funcs, "string::len"));
	m.register_typed("lowercase", ValueKind::String, get(funcs, "string::lowercase"));
	m.register_typed("matches", ValueKind::String, get(funcs, "string::matches"));
	// String has its own repeat, overriding the generic array::repeat
	m.register_typed("repeat", ValueKind::String, get(funcs, "string::repeat"));
	m.register_typed("replace", ValueKind::String, get(funcs, "string::replace"));
	m.register_typed("reverse", ValueKind::String, get(funcs, "string::reverse"));
	m.register_typed("slice", ValueKind::String, get(funcs, "string::slice"));
	m.register_typed("slug", ValueKind::String, get(funcs, "string::slug"));
	m.register_typed("split", ValueKind::String, get(funcs, "string::split"));
	m.register_typed("starts_with", ValueKind::String, get(funcs, "string::starts_with"));
	m.register_typed("trim", ValueKind::String, get(funcs, "string::trim"));
	m.register_typed("uppercase", ValueKind::String, get(funcs, "string::uppercase"));
	m.register_typed("words", ValueKind::String, get(funcs, "string::words"));

	// String distance methods
	m.register_typed(
		"distance_damerau_levenshtein",
		ValueKind::String,
		get(funcs, "string::distance::damerau_levenshtein"),
	);
	m.register_typed(
		"distance_hamming",
		ValueKind::String,
		get(funcs, "string::distance::hamming"),
	);
	m.register_typed(
		"distance_levenshtein",
		ValueKind::String,
		get(funcs, "string::distance::levenshtein"),
	);
	m.register_typed(
		"distance_normalized_damerau_levenshtein",
		ValueKind::String,
		get(funcs, "string::distance::normalized_damerau_levenshtein"),
	);
	m.register_typed(
		"distance_normalized_levenshtein",
		ValueKind::String,
		get(funcs, "string::distance::normalized_levenshtein"),
	);

	// String HTML methods
	m.register_typed("html_encode", ValueKind::String, get(funcs, "string::html::encode"));
	m.register_typed("html_sanitize", ValueKind::String, get(funcs, "string::html::sanitize"));

	// String is_* methods (type-specific string format checks)
	// These override the generic type::is_* for strings where both exist.
	m.register_typed("is_alphanum", ValueKind::String, get(funcs, "string::is_alphanum"));
	m.register_typed("is_alpha", ValueKind::String, get(funcs, "string::is_alpha"));
	m.register_typed("is_ascii", ValueKind::String, get(funcs, "string::is_ascii"));
	m.register_typed("is_datetime", ValueKind::String, get(funcs, "string::is_datetime"));
	m.register_typed("is_domain", ValueKind::String, get(funcs, "string::is_domain"));
	m.register_typed("is_email", ValueKind::String, get(funcs, "string::is_email"));
	m.register_typed("is_hexadecimal", ValueKind::String, get(funcs, "string::is_hexadecimal"));
	m.register_typed("is_ip", ValueKind::String, get(funcs, "string::is_ip"));
	m.register_typed("is_ipv4", ValueKind::String, get(funcs, "string::is_ipv4"));
	m.register_typed("is_ipv6", ValueKind::String, get(funcs, "string::is_ipv6"));
	m.register_typed("is_latitude", ValueKind::String, get(funcs, "string::is_latitude"));
	m.register_typed("is_longitude", ValueKind::String, get(funcs, "string::is_longitude"));
	m.register_typed("is_numeric", ValueKind::String, get(funcs, "string::is_numeric"));
	m.register_typed("is_semver", ValueKind::String, get(funcs, "string::is_semver"));
	m.register_typed("is_url", ValueKind::String, get(funcs, "string::is_url"));
	m.register_typed("is_ulid", ValueKind::String, get(funcs, "string::is_ulid"));
	m.register_typed("is_uuid", ValueKind::String, get(funcs, "string::is_uuid"));
	m.register_typed("is_record", ValueKind::String, get(funcs, "string::is_record"));

	// String similarity methods
	m.register_typed(
		"similarity_fuzzy",
		ValueKind::String,
		get(funcs, "string::similarity::fuzzy"),
	);
	m.register_typed("similarity_jaro", ValueKind::String, get(funcs, "string::similarity::jaro"));
	m.register_typed(
		"similarity_jaro_winkler",
		ValueKind::String,
		get(funcs, "string::similarity::jaro_winkler"),
	);
	m.register_typed(
		"similarity_smithwaterman",
		ValueKind::String,
		get(funcs, "string::similarity::smithwaterman"),
	);
	m.register_typed(
		"similarity_sorensen_dice",
		ValueKind::String,
		get(funcs, "string::similarity::sorensen_dice"),
	);

	// String semver methods
	m.register_typed("semver_compare", ValueKind::String, get(funcs, "string::semver::compare"));
	m.register_typed("semver_major", ValueKind::String, get(funcs, "string::semver::major"));
	m.register_typed("semver_minor", ValueKind::String, get(funcs, "string::semver::minor"));
	m.register_typed("semver_patch", ValueKind::String, get(funcs, "string::semver::patch"));
	m.register_typed(
		"semver_inc_major",
		ValueKind::String,
		get(funcs, "string::semver::inc::major"),
	);
	m.register_typed(
		"semver_inc_minor",
		ValueKind::String,
		get(funcs, "string::semver::inc::minor"),
	);
	m.register_typed(
		"semver_inc_patch",
		ValueKind::String,
		get(funcs, "string::semver::inc::patch"),
	);
	m.register_typed(
		"semver_set_major",
		ValueKind::String,
		get(funcs, "string::semver::set::major"),
	);
	m.register_typed(
		"semver_set_minor",
		ValueKind::String,
		get(funcs, "string::semver::set::minor"),
	);
	m.register_typed(
		"semver_set_patch",
		ValueKind::String,
		get(funcs, "string::semver::set::patch"),
	);

	// =====================================================================
	// Array methods
	// =====================================================================
	m.register_typed("add", ValueKind::Array, get(funcs, "array::add"));
	m.register_typed("all", ValueKind::Array, get(funcs, "array::all"));
	m.register_typed("any", ValueKind::Array, get(funcs, "array::any"));
	m.register_typed("append", ValueKind::Array, get(funcs, "array::append"));
	m.register_typed("at", ValueKind::Array, get(funcs, "array::at"));
	m.register_typed("boolean_and", ValueKind::Array, get(funcs, "array::boolean_and"));
	m.register_typed("boolean_not", ValueKind::Array, get(funcs, "array::boolean_not"));
	m.register_typed("boolean_or", ValueKind::Array, get(funcs, "array::boolean_or"));
	m.register_typed("boolean_xor", ValueKind::Array, get(funcs, "array::boolean_xor"));
	m.register_typed("clump", ValueKind::Array, get(funcs, "array::clump"));
	m.register_typed("combine", ValueKind::Array, get(funcs, "array::combine"));
	m.register_typed("complement", ValueKind::Array, get(funcs, "array::complement"));
	m.register_typed("concat", ValueKind::Array, get(funcs, "array::concat"));
	m.register_typed("difference", ValueKind::Array, get(funcs, "array::difference"));
	m.register_typed("distinct", ValueKind::Array, get(funcs, "array::distinct"));
	m.register_typed("fill", ValueKind::Array, get(funcs, "array::fill"));
	m.register_typed("filter", ValueKind::Array, get(funcs, "array::filter"));
	m.register_typed("filter_index", ValueKind::Array, get(funcs, "array::filter_index"));
	m.register_typed("find", ValueKind::Array, get(funcs, "array::find"));
	m.register_typed("find_index", ValueKind::Array, get(funcs, "array::find_index"));
	m.register_typed("first", ValueKind::Array, get(funcs, "array::first"));
	m.register_typed("fold", ValueKind::Array, get(funcs, "array::fold"));
	m.register_typed("flatten", ValueKind::Array, get(funcs, "array::flatten"));
	m.register_typed("group", ValueKind::Array, get(funcs, "array::group"));
	m.register_typed("insert", ValueKind::Array, get(funcs, "array::insert"));
	m.register_typed("intersect", ValueKind::Array, get(funcs, "array::intersect"));
	m.register_typed("is_empty", ValueKind::Array, get(funcs, "array::is_empty"));
	m.register_typed("join", ValueKind::Array, get(funcs, "array::join"));
	m.register_typed("last", ValueKind::Array, get(funcs, "array::last"));
	m.register_typed("len", ValueKind::Array, get(funcs, "array::len"));
	m.register_typed("logical_and", ValueKind::Array, get(funcs, "array::logical_and"));
	m.register_typed("logical_or", ValueKind::Array, get(funcs, "array::logical_or"));
	m.register_typed("logical_xor", ValueKind::Array, get(funcs, "array::logical_xor"));
	m.register_typed("matches", ValueKind::Array, get(funcs, "array::matches"));
	m.register_typed("map", ValueKind::Array, get(funcs, "array::map"));
	m.register_typed("max", ValueKind::Array, get(funcs, "array::max"));
	m.register_typed("min", ValueKind::Array, get(funcs, "array::min"));
	m.register_typed("pop", ValueKind::Array, get(funcs, "array::pop"));
	m.register_typed("prepend", ValueKind::Array, get(funcs, "array::prepend"));
	m.register_typed("push", ValueKind::Array, get(funcs, "array::push"));
	m.register_typed("reduce", ValueKind::Array, get(funcs, "array::reduce"));
	m.register_typed("remove", ValueKind::Array, get(funcs, "array::remove"));
	m.register_typed("reverse", ValueKind::Array, get(funcs, "array::reverse"));
	m.register_typed("shuffle", ValueKind::Array, get(funcs, "array::shuffle"));
	m.register_typed("slice", ValueKind::Array, get(funcs, "array::slice"));
	m.register_typed("sort", ValueKind::Array, get(funcs, "array::sort"));
	m.register_typed("sort_natural", ValueKind::Array, get(funcs, "array::sort_natural"));
	m.register_typed("sort_lexical", ValueKind::Array, get(funcs, "array::sort_lexical"));
	m.register_typed(
		"sort_natural_lexical",
		ValueKind::Array,
		get(funcs, "array::sort_natural_lexical"),
	);
	m.register_typed("swap", ValueKind::Array, get(funcs, "array::swap"));
	m.register_typed("transpose", ValueKind::Array, get(funcs, "array::transpose"));
	m.register_typed("union", ValueKind::Array, get(funcs, "array::union"));
	m.register_typed("sort_asc", ValueKind::Array, get(funcs, "array::sort::asc"));
	m.register_typed("sort_desc", ValueKind::Array, get(funcs, "array::sort::desc"));
	m.register_typed("windows", ValueKind::Array, get(funcs, "array::windows"));

	// Array vector methods (callable as methods on arrays)
	m.register_typed("vector_add", ValueKind::Array, get(funcs, "vector::add"));
	m.register_typed("vector_angle", ValueKind::Array, get(funcs, "vector::angle"));
	m.register_typed("vector_cross", ValueKind::Array, get(funcs, "vector::cross"));
	m.register_typed("vector_dot", ValueKind::Array, get(funcs, "vector::dot"));
	m.register_typed("vector_divide", ValueKind::Array, get(funcs, "vector::divide"));
	m.register_typed("vector_magnitude", ValueKind::Array, get(funcs, "vector::magnitude"));
	m.register_typed("vector_multiply", ValueKind::Array, get(funcs, "vector::multiply"));
	m.register_typed("vector_normalize", ValueKind::Array, get(funcs, "vector::normalize"));
	m.register_typed("vector_project", ValueKind::Array, get(funcs, "vector::project"));
	m.register_typed("vector_scale", ValueKind::Array, get(funcs, "vector::scale"));
	m.register_typed("vector_subtract", ValueKind::Array, get(funcs, "vector::subtract"));
	m.register_typed(
		"vector_distance_chebyshev",
		ValueKind::Array,
		get(funcs, "vector::distance::chebyshev"),
	);
	m.register_typed(
		"vector_distance_euclidean",
		ValueKind::Array,
		get(funcs, "vector::distance::euclidean"),
	);
	m.register_typed(
		"vector_distance_hamming",
		ValueKind::Array,
		get(funcs, "vector::distance::hamming"),
	);
	m.register_typed(
		"vector_distance_mahalanobis",
		ValueKind::Array,
		get(funcs, "vector::distance::mahalanobis"),
	);
	m.register_typed(
		"vector_distance_manhattan",
		ValueKind::Array,
		get(funcs, "vector::distance::manhattan"),
	);
	m.register_typed(
		"vector_distance_minkowski",
		ValueKind::Array,
		get(funcs, "vector::distance::minkowski"),
	);
	m.register_typed(
		"vector_similarity_cosine",
		ValueKind::Array,
		get(funcs, "vector::similarity::cosine"),
	);
	m.register_typed(
		"vector_similarity_jaccard",
		ValueKind::Array,
		get(funcs, "vector::similarity::jaccard"),
	);
	m.register_typed(
		"vector_similarity_pearson",
		ValueKind::Array,
		get(funcs, "vector::similarity::pearson"),
	);
	m.register_typed(
		"vector_similarity_spearman",
		ValueKind::Array,
		get(funcs, "vector::similarity::spearman"),
	);

	// =====================================================================
	// Set methods
	// =====================================================================
	m.register_typed("add", ValueKind::Set, get(funcs, "set::add"));
	m.register_typed("at", ValueKind::Set, get(funcs, "set::at"));
	m.register_typed("complement", ValueKind::Set, get(funcs, "set::complement"));
	m.register_typed("contains", ValueKind::Set, get(funcs, "set::contains"));
	m.register_typed("difference", ValueKind::Set, get(funcs, "set::difference"));
	m.register_typed("filter", ValueKind::Set, get(funcs, "set::filter"));
	m.register_typed("find", ValueKind::Set, get(funcs, "set::find"));
	m.register_typed("first", ValueKind::Set, get(funcs, "set::first"));
	m.register_typed("flatten", ValueKind::Set, get(funcs, "set::flatten"));
	m.register_typed("fold", ValueKind::Set, get(funcs, "set::fold"));
	m.register_typed("intersect", ValueKind::Set, get(funcs, "set::intersect"));
	m.register_typed("is_empty", ValueKind::Set, get(funcs, "set::is_empty"));
	m.register_typed("join", ValueKind::Set, get(funcs, "set::join"));
	m.register_typed("last", ValueKind::Set, get(funcs, "set::last"));
	m.register_typed("len", ValueKind::Set, get(funcs, "set::len"));
	m.register_typed("map", ValueKind::Set, get(funcs, "set::map"));
	m.register_typed("max", ValueKind::Set, get(funcs, "set::max"));
	m.register_typed("min", ValueKind::Set, get(funcs, "set::min"));
	m.register_typed("reduce", ValueKind::Set, get(funcs, "set::reduce"));
	m.register_typed("remove", ValueKind::Set, get(funcs, "set::remove"));
	m.register_typed("slice", ValueKind::Set, get(funcs, "set::slice"));
	m.register_typed("union", ValueKind::Set, get(funcs, "set::union"));

	// =====================================================================
	// Object methods
	// =====================================================================
	m.register_typed("entries", ValueKind::Object, get(funcs, "object::entries"));
	m.register_typed("extend", ValueKind::Object, get(funcs, "object::extend"));
	m.register_typed("is_empty", ValueKind::Object, get(funcs, "object::is_empty"));
	m.register_typed("keys", ValueKind::Object, get(funcs, "object::keys"));
	m.register_typed("len", ValueKind::Object, get(funcs, "object::len"));
	m.register_typed("remove", ValueKind::Object, get(funcs, "object::remove"));
	m.register_typed("values", ValueKind::Object, get(funcs, "object::values"));

	// =====================================================================
	// Bytes methods
	// =====================================================================
	m.register_typed("len", ValueKind::Bytes, get(funcs, "bytes::len"));

	// =====================================================================
	// Duration methods
	// =====================================================================
	m.register_typed("days", ValueKind::Duration, get(funcs, "duration::days"));
	m.register_typed("hours", ValueKind::Duration, get(funcs, "duration::hours"));
	m.register_typed("micros", ValueKind::Duration, get(funcs, "duration::micros"));
	m.register_typed("millis", ValueKind::Duration, get(funcs, "duration::millis"));
	m.register_typed("mins", ValueKind::Duration, get(funcs, "duration::mins"));
	m.register_typed("nanos", ValueKind::Duration, get(funcs, "duration::nanos"));
	m.register_typed("secs", ValueKind::Duration, get(funcs, "duration::secs"));
	m.register_typed("weeks", ValueKind::Duration, get(funcs, "duration::weeks"));
	m.register_typed("years", ValueKind::Duration, get(funcs, "duration::years"));

	// =====================================================================
	// Number (math) methods
	// =====================================================================
	m.register_typed("abs", ValueKind::Number, get(funcs, "math::abs"));
	m.register_typed("acos", ValueKind::Number, get(funcs, "math::acos"));
	m.register_typed("acot", ValueKind::Number, get(funcs, "math::acot"));
	m.register_typed("asin", ValueKind::Number, get(funcs, "math::asin"));
	m.register_typed("atan", ValueKind::Number, get(funcs, "math::atan"));
	m.register_typed("ceil", ValueKind::Number, get(funcs, "math::ceil"));
	m.register_typed("cos", ValueKind::Number, get(funcs, "math::cos"));
	m.register_typed("cot", ValueKind::Number, get(funcs, "math::cot"));
	m.register_typed("deg2rad", ValueKind::Number, get(funcs, "math::deg2rad"));
	m.register_typed("floor", ValueKind::Number, get(funcs, "math::floor"));
	m.register_typed("ln", ValueKind::Number, get(funcs, "math::ln"));
	m.register_typed("log", ValueKind::Number, get(funcs, "math::log"));
	m.register_typed("log10", ValueKind::Number, get(funcs, "math::log10"));
	m.register_typed("log2", ValueKind::Number, get(funcs, "math::log2"));
	m.register_typed("rad2deg", ValueKind::Number, get(funcs, "math::rad2deg"));
	m.register_typed("round", ValueKind::Number, get(funcs, "math::round"));
	m.register_typed("sign", ValueKind::Number, get(funcs, "math::sign"));
	m.register_typed("sin", ValueKind::Number, get(funcs, "math::sin"));
	m.register_typed("tan", ValueKind::Number, get(funcs, "math::tan"));

	// =====================================================================
	// Geometry (geo) methods
	// =====================================================================
	m.register_typed("area", ValueKind::Geometry, get(funcs, "geo::area"));
	m.register_typed("bearing", ValueKind::Geometry, get(funcs, "geo::bearing"));
	m.register_typed("centroid", ValueKind::Geometry, get(funcs, "geo::centroid"));
	m.register_typed("distance", ValueKind::Geometry, get(funcs, "geo::distance"));
	m.register_typed("hash_decode", ValueKind::Geometry, get(funcs, "geo::hash::decode"));
	m.register_typed("hash_encode", ValueKind::Geometry, get(funcs, "geo::hash::encode"));
	m.register_typed("is_valid", ValueKind::Geometry, get(funcs, "geo::is_valid"));

	// =====================================================================
	// RecordId methods
	// =====================================================================
	m.register_typed("exists", ValueKind::Record, get(funcs, "record::exists"));
	m.register_typed("id", ValueKind::Record, get(funcs, "record::id"));
	m.register_typed("tb", ValueKind::Record, get(funcs, "record::tb"));
	m.register_typed("table", ValueKind::Record, get(funcs, "record::table"));
	m.register_typed("is_edge", ValueKind::Record, get(funcs, "record::is_edge"));

	// =====================================================================
	// Datetime (time) methods
	// =====================================================================
	m.register_typed("ceil", ValueKind::Datetime, get(funcs, "time::ceil"));
	m.register_typed("day", ValueKind::Datetime, get(funcs, "time::day"));
	m.register_typed("floor", ValueKind::Datetime, get(funcs, "time::floor"));
	m.register_typed("format", ValueKind::Datetime, get(funcs, "time::format"));
	m.register_typed("group", ValueKind::Datetime, get(funcs, "time::group"));
	m.register_typed("hour", ValueKind::Datetime, get(funcs, "time::hour"));
	m.register_typed("is_leap_year", ValueKind::Datetime, get(funcs, "time::is_leap_year"));
	m.register_typed("micros", ValueKind::Datetime, get(funcs, "time::micros"));
	m.register_typed("millis", ValueKind::Datetime, get(funcs, "time::millis"));
	m.register_typed("minute", ValueKind::Datetime, get(funcs, "time::minute"));
	m.register_typed("month", ValueKind::Datetime, get(funcs, "time::month"));
	m.register_typed("nano", ValueKind::Datetime, get(funcs, "time::nano"));
	m.register_typed("round", ValueKind::Datetime, get(funcs, "time::round"));
	m.register_typed("second", ValueKind::Datetime, get(funcs, "time::second"));
	m.register_typed("unix", ValueKind::Datetime, get(funcs, "time::unix"));
	m.register_typed("wday", ValueKind::Datetime, get(funcs, "time::wday"));
	m.register_typed("week", ValueKind::Datetime, get(funcs, "time::week"));
	m.register_typed("yday", ValueKind::Datetime, get(funcs, "time::yday"));
	m.register_typed("year", ValueKind::Datetime, get(funcs, "time::year"));

	// =====================================================================
	// File methods
	// =====================================================================
	m.register_typed("bucket", ValueKind::File, get(funcs, "file::bucket"));
	m.register_typed("key", ValueKind::File, get(funcs, "file::key"));
	m.register_typed("put", ValueKind::File, get(funcs, "file::put"));
	m.register_typed("put_if_not_exists", ValueKind::File, get(funcs, "file::put_if_not_exists"));
	m.register_typed("get", ValueKind::File, get(funcs, "file::get"));
	m.register_typed("head", ValueKind::File, get(funcs, "file::head"));
	m.register_typed("delete", ValueKind::File, get(funcs, "file::delete"));
	m.register_typed("copy", ValueKind::File, get(funcs, "file::copy"));
	m.register_typed("copy_if_not_exists", ValueKind::File, get(funcs, "file::copy_if_not_exists"));
	m.register_typed("rename", ValueKind::File, get(funcs, "file::rename"));
	m.register_typed(
		"rename_if_not_exists",
		ValueKind::File,
		get(funcs, "file::rename_if_not_exists"),
	);
	m.register_typed("exists", ValueKind::File, get(funcs, "file::exists"));
	m.register_typed("list", ValueKind::File, get(funcs, "file::list"));

	// =====================================================================
	// Aliases
	// These must come after the methods they alias are registered.
	// =====================================================================
	m.register_alias("every", "all"); // array.every() → array::all
	m.register_alias("includes", "any"); // array.includes() → array::any
	m.register_alias("some", "any"); // array.some() → array::any
	m.register_alias("index_of", "find_index"); // array.index_of() → array::find_index

	m
}
