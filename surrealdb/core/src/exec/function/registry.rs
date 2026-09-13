//! Function registry for storing and looking up scalar, aggregate, and projection functions.

use std::collections::HashMap;
use std::sync::Arc;

use super::method::{self, MethodDescriptor, MethodRegistry};
use super::{AggregateFunction, IndexFunction, ProjectionFunction, ScalarFunction, builtin};

/// Registry of functions available during query execution.
///
/// The registry maps function names to their implementations for:
/// - Scalar functions (operate on individual values, return single value)
/// - Aggregate functions (operate over groups of values)
/// - Projection functions (produce field bindings for output objects)
/// - Index functions (bound to WHERE clause predicates via index infrastructure)
///
/// It is typically created once at startup with all built-in functions
/// and then shared across execution contexts.
#[derive(Debug)]
pub struct FunctionRegistry {
	/// Scalar functions (e.g., math::abs, string::len)
	functions: HashMap<&'static str, Arc<dyn ScalarFunction>>,
	/// Aggregate functions (e.g., count, math::sum, math::mean)
	aggregates: HashMap<&'static str, Arc<dyn AggregateFunction>>,
	/// Projection functions (e.g., type::field, type::fields)
	projections: HashMap<&'static str, Arc<dyn ProjectionFunction>>,
	/// Index functions (e.g., search::highlight, search::score, search::offsets)
	index_functions: HashMap<&'static str, Arc<dyn IndexFunction>>,
	/// Method registry for value dot-syntax method dispatch
	methods: MethodRegistry,
}

impl FunctionRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			functions: HashMap::new(),
			aggregates: HashMap::new(),
			projections: HashMap::new(),
			index_functions: HashMap::new(),
			methods: MethodRegistry::default(),
		}
	}

	// =========================================================================
	// Scalar function methods
	// =========================================================================

	/// Register a scalar function in the registry.
	pub fn register(&mut self, func: impl ScalarFunction + 'static) {
		let name = func.name();
		self.functions.insert(name, Arc::new(func));
	}

	/// Look up a scalar function by name.
	pub fn get(&self, name: &str) -> Option<&Arc<dyn ScalarFunction>> {
		self.functions.get(name)
	}

	/// Check if a scalar function exists.
	#[cfg(test)]
	pub fn contains(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	/// Iterate all registered scalar functions.
	#[cfg(test)]
	pub(crate) fn scalar_functions(&self) -> impl Iterator<Item = &Arc<dyn ScalarFunction>> {
		self.functions.values()
	}

	/// Iterate all registered projection functions.
	#[cfg(all(test, feature = "kv-mem"))]
	pub(crate) fn projection_functions(
		&self,
	) -> impl Iterator<Item = &Arc<dyn ProjectionFunction>> {
		self.projections.values()
	}

	/// Iterate all registered index functions.
	#[cfg(all(test, feature = "kv-mem"))]
	pub(crate) fn index_functions(&self) -> impl Iterator<Item = &Arc<dyn IndexFunction>> {
		self.index_functions.values()
	}

	/// Get the number of registered scalar functions.
	#[cfg(test)]
	pub fn len(&self) -> usize {
		self.functions.len()
	}

	// =========================================================================
	// Aggregate function methods
	// =========================================================================

	/// Register an aggregate function in the registry.
	pub fn register_aggregate(&mut self, func: impl AggregateFunction + 'static) {
		let name = func.name();
		self.aggregates.insert(name, Arc::new(func));
	}

	/// Look up an aggregate function by name.
	pub fn get_aggregate(&self, name: &str) -> Option<&Arc<dyn AggregateFunction>> {
		self.aggregates.get(name)
	}

	/// Check if a function is registered as an aggregate.
	pub fn is_aggregate(&self, name: &str) -> bool {
		self.aggregates.contains_key(name)
	}

	/// Get the number of registered aggregate functions.
	#[cfg(test)]
	pub fn aggregate_len(&self) -> usize {
		self.aggregates.len()
	}

	/// The aggregate that reduces a projected expression carrying no aggregate
	/// of its own, by collecting each row's value with duplicates kept.
	///
	/// Deliberately not `array::group`, which owes its caller a deduplicated
	/// result: one accumulator cannot honour both contracts.
	pub fn get_implicit_collect_aggregate(&self) -> Arc<dyn AggregateFunction> {
		Arc::new(builtin::aggregates::ArrayCollect)
	}

	/// Get the appropriate aggregate function for "count", handling the special case:
	/// - count() with no arguments counts all rows (uses Count)
	/// - count(expr) with arguments counts truthy values (uses CountField)
	pub fn get_count_aggregate(&self, has_arguments: bool) -> Arc<dyn AggregateFunction> {
		if has_arguments {
			// Use CountField for count with arguments
			Arc::new(builtin::aggregates::CountField)
		} else {
			// Use Count for count with no arguments
			Arc::clone(self.aggregates.get("count").expect("count should be registered"))
		}
	}

	// =========================================================================
	// Projection function methods
	// =========================================================================

	/// Register a projection function in the registry.
	pub fn register_projection(&mut self, func: impl ProjectionFunction + 'static) {
		let name = func.name();
		self.projections.insert(name, Arc::new(func));
	}

	/// Look up a projection function by name.
	pub fn get_projection(&self, name: &str) -> Option<&Arc<dyn ProjectionFunction>> {
		self.projections.get(name)
	}

	/// Check if a function is registered as a projection function.
	pub fn is_projection(&self, name: &str) -> bool {
		self.projections.contains_key(name)
	}

	/// Get the number of registered projection functions.
	#[cfg(test)]
	pub fn projection_len(&self) -> usize {
		self.projections.len()
	}

	// =========================================================================
	// Index function methods
	// =========================================================================

	/// Register an index function in the registry.
	pub fn register_index_function(&mut self, func: impl IndexFunction + 'static) {
		let name = func.name();
		self.index_functions.insert(name, Arc::new(func));
	}

	/// Look up an index function by name.
	pub fn get_index_function(&self, name: &str) -> Option<&Arc<dyn IndexFunction>> {
		self.index_functions.get(name)
	}

	/// Check if a function is registered as an index function.
	pub fn is_index_function(&self, name: &str) -> bool {
		self.index_functions.contains_key(name)
	}

	// =========================================================================
	// Method registry methods
	// =========================================================================

	/// Look up a method descriptor by method name.
	pub fn get_method(&self, name: &str) -> Option<&Arc<MethodDescriptor>> {
		self.methods.get(name)
	}

	/// The full method registry.
	#[cfg(test)]
	pub fn methods(&self) -> &MethodRegistry {
		&self.methods
	}

	// =========================================================================
	// Combined methods
	// =========================================================================

	/// Create a registry with all built-in functions.
	pub fn with_builtins() -> Self {
		let mut registry = Self::new();
		builtin::register_all(&mut registry);
		// Build method registry from the registered scalar functions
		registry.methods = method::build_method_registry(&registry);
		registry
	}
}

impl Default for FunctionRegistry {
	fn default() -> Self {
		Self::with_builtins()
	}
}

/// The experimental target a built-in is gated behind, if any.
///
/// Mirrors the `exp(...)` markers on the corresponding arms of `fnc::dispatch`,
/// which is where the legacy executor enforces the same gate. Registry entries
/// that reach a bucket acquire it through their `EvalContext` and check for
/// themselves; the functions listed here are pure, so they carry no context and
/// the gate has to be applied by whoever invokes them — the built-in physical
/// expression, and the plan-time folder.
pub(crate) fn experimental_target(
	name: &str,
) -> Option<crate::dbs::capabilities::ExperimentalTarget> {
	use crate::dbs::capabilities::ExperimentalTarget;
	match name {
		"type::file" | "file::bucket" | "file::key" => Some(ExperimentalTarget::Files),
		_ => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::expr::logical::EffectSet as E;

	#[test]
	fn test_builtin_registry() {
		let registry = FunctionRegistry::with_builtins();

		// Check some known scalar functions exist
		assert!(registry.contains("math::abs"));
		assert!(registry.contains("string::len"));
		assert!(registry.contains("array::len"));

		// Check unknown function doesn't exist
		assert!(!registry.contains("unknown::function"));

		// Should have many scalar functions
		assert!(registry.len() > 100);
	}

	#[test]
	fn test_function_lookup() {
		let registry = FunctionRegistry::with_builtins();

		let abs = registry.get("math::abs").expect("math::abs should exist");
		assert_eq!(abs.name(), "math::abs");
		assert!(abs.is_pure());
		assert!(!abs.is_async());
	}

	#[test]
	fn test_aggregate_functions() {
		let registry = FunctionRegistry::with_builtins();

		// Check aggregate functions are registered
		assert!(registry.is_aggregate("count"));
		assert!(registry.is_aggregate("math::sum"));
		assert!(registry.is_aggregate("math::mean"));
		assert!(registry.is_aggregate("math::min"));
		assert!(registry.is_aggregate("math::max"));
		assert!(registry.is_aggregate("math::stddev"));
		assert!(registry.is_aggregate("math::variance"));
		assert!(registry.is_aggregate("time::min"));
		assert!(registry.is_aggregate("time::max"));
		assert!(registry.is_aggregate("array::group"));
		assert!(registry.is_aggregate("array::distinct"));
		assert!(registry.is_aggregate("vector::sum"));

		// vector::sum is both an aggregate and a scalar, so a direct call folds
		// the array it is given while a GROUP BY query folds across rows.
		assert!(registry.contains("vector::sum"));

		// Scalar functions should not be aggregates
		assert!(!registry.is_aggregate("math::abs"));
		assert!(!registry.is_aggregate("string::len"));

		// Should have the expected number of aggregate functions
		assert!(registry.aggregate_len() >= 10);
	}

	#[test]
	fn test_aggregate_lookup() {
		let registry = FunctionRegistry::with_builtins();

		let mean = registry.get_aggregate("math::mean").expect("math::mean should exist");
		assert_eq!(mean.name(), "math::mean");

		// Can create an accumulator
		let _accumulator = mean.create_accumulator();
	}

	#[test]
	fn test_projection_functions() {
		let registry = FunctionRegistry::with_builtins();

		// Check projection functions are registered
		assert!(registry.is_projection("type::field"));
		assert!(registry.is_projection("type::fields"));

		// Scalar functions should not be projection functions
		assert!(!registry.is_projection("math::abs"));
		assert!(!registry.is_projection("string::len"));
		assert!(!registry.is_projection("type::string"));

		// Aggregate functions should not be projection functions
		assert!(!registry.is_projection("count"));
		assert!(!registry.is_projection("math::sum"));

		// Should have exactly 2 projection functions
		assert_eq!(registry.projection_len(), 2);
	}

	#[test]
	fn test_projection_lookup() {
		let registry = FunctionRegistry::with_builtins();

		let field = registry.get_projection("type::field").expect("type::field should exist");
		assert_eq!(field.name(), "type::field");

		let fields = registry.get_projection("type::fields").expect("type::fields should exist");
		assert_eq!(fields.name(), "type::fields");
	}

	/// The names every builtin that answers `false` to
	/// [`ScalarFunction::is_deterministic`] is registered under.
	///
	/// Kept sorted, and asserted against the registry below, so that a declared
	/// [`EffectSet`] moving a function across the line — in either direction —
	/// is a deliberate edit here rather than a silent change of what the
	/// planner may rewrite.
	const NON_DETERMINISTIC: &[&str] = &[
		"api::invoke",
		"api::req::body",
		"api::req::max_body",
		"api::res::body",
		"api::res::header",
		"api::res::headers",
		"api::res::status",
		"api::timeout",
		"array::shuffle",
		"crypto::argon2::generate",
		"crypto::bcrypt::generate",
		"crypto::pbkdf2::generate",
		"crypto::scrypt::generate",
		"eval::gql",
		"eval::surql",
		"file::copy",
		"file::copy_if_not_exists",
		"file::delete",
		"file::exists",
		"file::get",
		"file::head",
		"file::list",
		"file::put",
		"file::put_if_not_exists",
		"file::rename",
		"file::rename_if_not_exists",
		"http::delete",
		"http::get",
		"http::head",
		"http::patch",
		"http::post",
		"http::put",
		"rand",
		"rand::bool",
		"rand::duration",
		"rand::enum",
		"rand::float",
		"rand::id",
		"rand::int",
		"rand::string",
		"rand::time",
		"rand::ulid",
		"rand::uuid",
		"rand::uuid::v4",
		"rand::uuid::v7",
		"sequence::nextval",
		"sleep",
		"time::day",
		"time::hour",
		"time::is_leap_year",
		"time::micros",
		"time::millis",
		"time::minute",
		"time::month",
		"time::nano",
		"time::now",
		"time::second",
		"time::timezone",
		"time::unix",
		"time::wday",
		"time::week",
		"time::yday",
		"time::year",
	];

	/// Whole families reach past what a rewrite can reason about, and every
	/// member of one owes `is_deterministic() == false`: `rand::*` draws
	/// entropy, `http::*` talks to a remote endpoint, and `file::*` reads or
	/// writes a bucket that no transaction snapshots — all but `file::bucket`
	/// and `file::key`, which only read the two fields of the file value they
	/// are handed. Naming the families rather than their members is what makes
	/// this catch a function added to one of them later.
	fn belongs_to_a_volatile_family(name: &str) -> bool {
		match name {
			"file::bucket" | "file::key" => false,
			_ => {
				name == "rand"
					|| name.starts_with("rand::")
					|| name.starts_with("http::")
					|| name.starts_with("file::")
			}
		}
	}

	/// A builtin that draws entropy, reaches past the transaction's snapshot or
	/// runs a body the call site cannot see must report itself
	/// non-deterministic. Two decisions rest on the answer: the planner's
	/// literal folder would otherwise evaluate the call once and freeze one
	/// answer into the whole statement, and the logical rules would treat an
	/// expression containing it as pure and so substitutable for every
	/// occurrence of a name — duplicating the call, or dropping it.
	///
	/// The answer is derived from the [`EffectSet`] each definition declares,
	/// so this pins what those declarations add up to.
	#[test]
	fn volatile_builtins_are_non_deterministic() {
		let registry = FunctionRegistry::with_builtins();

		// A family is checked by prefix, so a member added later is covered
		// without touching this test.
		let offenders: Vec<&str> = registry
			.scalar_functions()
			.map(|f| f.name())
			.filter(|name| belongs_to_a_volatile_family(name))
			.filter(|name| {
				registry.get(name).expect("just enumerated from the registry").is_deterministic()
			})
			.collect();

		assert!(
			offenders.is_empty(),
			"these must declare a non-deterministic effect, or the planner will fold a call \
			 to one constant reused for every row, and a rule will duplicate or drop it: \
			 {offenders:?}"
		);

		// Everything else is named one by one, so that the set as a whole only
		// moves on purpose.
		let mut actual: Vec<&str> = registry
			.scalar_functions()
			.filter(|f| !f.is_deterministic())
			.map(|f| f.name())
			.collect();
		actual.sort_unstable();

		assert!(NON_DETERMINISTIC.is_sorted(), "keep `NON_DETERMINISTIC` sorted");
		let expected: Vec<&str> =
			NON_DETERMINISTIC.iter().copied().filter(|name| registry.get(name).is_some()).collect();

		assert_eq!(
			actual, expected,
			"the non-deterministic set moved; a builtin that draws entropy, reaches past the \
			 transaction snapshot or runs a body the call site cannot see belongs in \
			 `NON_DETERMINISTIC` and declares the effect at its definition"
		);

		// Guard the guard: a registry that answered nothing would pass both.
		assert!(actual.len() > 40, "expected the whole volatile class, found {actual:?}");
	}

	/// Every builtin that leaves something behind must report it, so a grouped
	/// projection holding one is evaluated per row rather than once per group.
	/// The answer is derived from the declared [`EffectSet`]; this pins which
	/// declarations add up to it, since a mutator declared short reduces
	/// silently, and the only symptom is a statement with the wrong number of
	/// effects.
	#[test]
	fn mutating_builtins_report_mutating_state() {
		let registry = FunctionRegistry::with_builtins();

		let expected = [
			"sequence::nextval",
			"http::put",
			"http::post",
			"http::patch",
			"http::delete",
			"file::put",
			"file::put_if_not_exists",
			"file::delete",
			"file::copy",
			"file::copy_if_not_exists",
			"file::rename",
			"file::rename_if_not_exists",
			// A body the call site cannot see may write, and a write outlives the
			// statement as surely as a bucket put does.
			"eval::surql",
			"eval::gql",
			"api::invoke",
		];

		let missing: Vec<&str> = expected
			.iter()
			.filter(|name| {
				!registry.get(name).expect("expected a registered builtin").mutates_state()
			})
			.copied()
			.collect();
		assert!(
			missing.is_empty(),
			"these must declare an effect that outlives the statement, or a grouped \
			 projection will run them once per group instead of once per row: {missing:?}"
		);

		// The readers alongside them must not be swept up: reducing those is
		// what the flag is meant to keep allowed.
		for name in ["http::get", "http::head", "file::get", "file::exists", "file::list"] {
			assert!(
				!registry.get(name).expect("expected a registered builtin").mutates_state(),
				"{name} only reads, so it must stay reducible"
			);
		}
	}

	/// Every declared [`EffectSet`] that is not `PURE`, by name.
	///
	/// Kept sorted, and asserted against the registry below, so that a
	/// declaration changing — a flag gained, a flag lost, a function moving off
	/// `PURE` or onto it — is a deliberate edit here. A builtin not in the
	/// table is declared `PURE`, so this is the one place to read what every
	/// builtin is declared to do.
	const EFFECTS: &[(&str, E)] = &[
		("api::invoke", E::OPAQUE_BODY.union(E::WRITES_DATA)),
		("api::req::body", E::OPAQUE_BODY),
		("api::req::max_body", E::OPAQUE_BODY),
		("api::res::body", E::OPAQUE_BODY),
		("api::res::header", E::OPAQUE_BODY),
		("api::res::headers", E::OPAQUE_BODY),
		("api::res::status", E::OPAQUE_BODY),
		("api::timeout", E::OPAQUE_BODY),
		("array::shuffle", E::ENTROPY),
		("crypto::argon2::generate", E::ENTROPY),
		("crypto::bcrypt::generate", E::ENTROPY),
		("crypto::pbkdf2::generate", E::ENTROPY),
		("crypto::scrypt::generate", E::ENTROPY),
		("eval::gql", E::OPAQUE_BODY.union(E::WRITES_DATA)),
		("eval::surql", E::OPAQUE_BODY.union(E::WRITES_DATA)),
		("file::copy", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("file::copy_if_not_exists", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("file::delete", E::WRITES_EXTERNAL),
		("file::exists", E::READS_EXTERNAL),
		("file::get", E::READS_EXTERNAL),
		("file::head", E::READS_EXTERNAL),
		("file::list", E::READS_EXTERNAL),
		("file::put", E::WRITES_EXTERNAL),
		("file::put_if_not_exists", E::WRITES_EXTERNAL),
		("file::rename", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("file::rename_if_not_exists", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("http::delete", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("http::get", E::READS_EXTERNAL),
		("http::head", E::READS_EXTERNAL),
		("http::patch", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("http::post", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("http::put", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("rand", E::ENTROPY),
		("rand::bool", E::ENTROPY),
		("rand::duration", E::ENTROPY),
		("rand::enum", E::ENTROPY),
		("rand::float", E::ENTROPY),
		("rand::id", E::ENTROPY),
		("rand::int", E::ENTROPY),
		("rand::string", E::ENTROPY),
		("rand::time", E::ENTROPY),
		("rand::ulid", E::ENTROPY),
		("rand::uuid", E::ENTROPY),
		("rand::uuid::v4", E::ENTROPY),
		("rand::uuid::v7", E::ENTROPY),
		("record::exists", E::READS_DATA),
		("record::is_edge", E::READS_DATA),
		("schema::table::exists", E::READS_CATALOG),
		("search::analyze", E::READS_CATALOG),
		("sequence::nextval", E::ADVANCES_STATE),
		("session::ac", E::READS_SESSION),
		("session::db", E::READS_SESSION),
		("session::id", E::READS_SESSION),
		("session::ip", E::READS_SESSION),
		("session::ns", E::READS_SESSION),
		("session::origin", E::READS_SESSION),
		("session::rd", E::READS_SESSION),
		("session::token", E::READS_SESSION),
		("sleep", E::BLOCKING),
		("time::day", E::CLOCK),
		("time::hour", E::CLOCK),
		("time::is_leap_year", E::CLOCK),
		("time::micros", E::CLOCK),
		("time::millis", E::CLOCK),
		("time::minute", E::CLOCK),
		("time::month", E::CLOCK),
		("time::nano", E::CLOCK),
		("time::now", E::CLOCK),
		("time::second", E::CLOCK),
		("time::timezone", E::CLOCK),
		("time::unix", E::CLOCK),
		("time::wday", E::CLOCK),
		("time::week", E::CLOCK),
		("time::yday", E::CLOCK),
		("time::year", E::CLOCK),
	];

	/// The declarations add up to exactly the pinned table, and draw the same
	/// line as the writer list the expression layer keeps for transaction
	/// classification.
	#[test]
	fn every_effect_declaration_is_pinned() {
		let registry = FunctionRegistry::with_builtins();

		assert!(EFFECTS.is_sorted_by_key(|(name, _)| *name), "keep `EFFECTS` sorted");
		let mut actual: Vec<(&str, E)> = registry
			.scalar_functions()
			.filter(|f| !f.effects().is_pure())
			.map(|f| (f.name(), f.effects()))
			.collect();
		actual.sort_unstable_by_key(|(name, _)| *name);
		let expected: Vec<(&str, E)> =
			EFFECTS.iter().copied().filter(|(name, _)| registry.get(name).is_some()).collect();
		assert_eq!(
			actual, expected,
			"a declared effect set moved; `EFFECTS` names every builtin that is not `PURE`, \
			 with what it declares"
		);
		assert!(actual.len() > 60, "expected the whole declared class, found {actual:?}");

		// `is_writer_builtin` decides the transaction a statement holding the
		// call takes; a declaration that disagreed with it would let a plan
		// classify a call one way and the transaction another.
		for f in registry.scalar_functions() {
			assert_eq!(
				crate::expr::method::is_writer_builtin(f.name()),
				f.effects().writes_data(),
				"{}: `is_writer_builtin` and `EffectSet::WRITES_DATA` disagree",
				f.name()
			);
		}
	}

	/// Every builtin that runs a closure it is handed, with the argument
	/// positions the closure may arrive in ([`ScalarFunction::closure_args`]).
	///
	/// Kept sorted, and asserted against the registry below. A closure a
	/// builtin runs evaluates against the row, so a builtin that runs one
	/// without declaring the position lets a body reach a field-permission
	/// layer unguarded; one that declares a position it never runs a body in
	/// withholds a transparent call from the rules. Either is a deliberate
	/// edit here.
	const CLOSURE_ARGS: &[(&str, &[usize])] = &[
		("array::all", &[1]),
		("array::any", &[1]),
		("array::every", &[1]),
		("array::filter", &[1]),
		("array::filter_index", &[1]),
		("array::find", &[1]),
		("array::find_index", &[1]),
		("array::fold", &[2]),
		("array::includes", &[1]),
		("array::index_of", &[1]),
		("array::map", &[1]),
		("array::reduce", &[1]),
		("array::some", &[1]),
		("set::all", &[1]),
		("set::any", &[1]),
		("set::filter", &[1]),
		("set::find", &[1]),
		("set::fold", &[2]),
		("set::map", &[1]),
		("set::reduce", &[1]),
		("value::chain", &[1]),
		("value::expect", &[1]),
	];

	#[test]
	fn every_closure_position_is_pinned() {
		let registry = FunctionRegistry::with_builtins();

		assert!(CLOSURE_ARGS.is_sorted_by_key(|(name, _)| *name), "keep `CLOSURE_ARGS` sorted");
		let mut actual: Vec<(&str, &[usize])> = registry
			.scalar_functions()
			.filter(|f| !f.closure_args().is_empty())
			.map(|f| (f.name(), f.closure_args()))
			.collect();
		actual.sort_unstable_by_key(|(name, _)| *name);
		assert_eq!(
			actual, CLOSURE_ARGS,
			"a closure position moved; `CLOSURE_ARGS` names every builtin that runs a closure \
			 it is handed, with the positions it accepts one in"
		);

		// Every declared position is one the signature accepts an argument in.
		for f in registry.scalar_functions() {
			let (_, upper) = f.signature().arity();
			for &slot in f.closure_args() {
				assert!(
					upper.is_none_or(|upper| slot < upper),
					"{}: closure position {slot} is past the declared arguments",
					f.name()
				);
			}
		}
	}
}
