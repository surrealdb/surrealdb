//! Function registry for storing and looking up scalar, aggregate, and projection functions.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use super::method::{self, MethodDescriptor, MethodRegistry};
use super::{AggregateFunction, IndexFunction, ProjectionFunction, ScalarFunction, builtin};
use crate::exec::ir::iface::Fallibility;
use crate::expr::logical::EffectSet;

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
	functions: HashMap<&'static str, &'static dyn ScalarFunction>,
	/// The scalar names [`Self::register`] has bound since their family last registered them:
	/// each resolves to the function registered, not to the builtin of that name.
	registered: HashSet<&'static str>,
	/// Aggregate functions (e.g., count, math::sum, math::mean)
	aggregates: HashMap<&'static str, Arc<dyn AggregateFunction>>,
	/// Projection functions (e.g., type::field, type::fields)
	projections: HashMap<&'static str, Arc<dyn ProjectionFunction>>,
	/// Index functions (e.g., search::highlight, search::score, search::offsets)
	index_functions: HashMap<&'static str, Arc<dyn IndexFunction>>,
	/// Method registry for value dot-syntax method dispatch
	methods: MethodRegistry,
	/// The identity of what the registry holds ([`Self::id`]).
	id: u64,
}

/// A registry identity no registry has had, in this process.
fn fresh_id() -> u64 {
	static NEXT: AtomicU64 = AtomicU64::new(1);
	NEXT.fetch_add(1, Ordering::Relaxed)
}

impl FunctionRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			functions: HashMap::new(),
			registered: HashSet::new(),
			aggregates: HashMap::new(),
			projections: HashMap::new(),
			index_functions: HashMap::new(),
			methods: MethodRegistry::default(),
			id: fresh_id(),
		}
	}

	/// The identity of what this registry holds: never `0`, never shared with
	/// another registry of the process, alive or dropped, and renewed by every
	/// registration. A function resolved from a registry of one id is the
	/// function that registry of that id answers, so a resolution kept beyond
	/// a run is checked against it rather than against the registry's address,
	/// which a later registry may reuse.
	pub(crate) fn id(&self) -> u64 {
		self.id
	}

	// =========================================================================
	// Scalar function methods
	// =========================================================================

	/// Register one scalar function in the registry. The builtin families register through
	/// [`Self::register_scalars`].
	#[allow(unused)]
	pub fn register(&mut self, func: &'static dyn ScalarFunction) {
		self.functions.insert(func.name(), func);
		self.registered.insert(func.name());
		self.id = fresh_id();
	}

	/// Register the scalar functions of every family in `families`, in slice order. Renews
	/// [`Self::id`] once, however many functions it registers.
	pub(crate) fn register_scalars(&mut self, families: &[&[&'static dyn ScalarFunction]]) {
		self.functions.reserve(families.iter().map(|family| family.len()).sum());
		for family in families {
			for &func in *family {
				self.functions.insert(func.name(), func);
				self.registered.remove(func.name());
			}
		}
		self.id = fresh_id();
	}

	/// Look up a scalar function by name.
	pub fn get(&self, name: &str) -> Option<&'static dyn ScalarFunction> {
		self.functions.get(name).copied()
	}

	/// Whether `name` resolves to the scalar builtin its family registered, which code that
	/// answers a builtin without calling it may stand in for: `false` where [`Self::register`]
	/// has bound the name to another function, or where the registry holds no function of it.
	pub(crate) fn is_builtin(&self, name: &str) -> bool {
		self.functions.contains_key(name) && !self.registered.contains(name)
	}

	/// Check if a scalar function exists.
	#[cfg(test)]
	pub fn contains(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	/// Iterate all registered scalar functions.
	#[cfg(test)]
	pub(crate) fn scalar_functions(
		&self,
	) -> impl Iterator<Item = &'static dyn ScalarFunction> + '_ {
		self.functions.values().copied()
	}

	/// Iterate all registered projection functions.
	#[cfg(test)]
	pub(crate) fn projection_functions(
		&self,
	) -> impl Iterator<Item = &Arc<dyn ProjectionFunction>> {
		self.projections.values()
	}

	/// Iterate all registered index functions.
	#[cfg(test)]
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
		self.id = fresh_id();
	}

	/// Look up an aggregate function by name.
	pub fn get_aggregate(&self, name: &str) -> Option<&Arc<dyn AggregateFunction>> {
		self.aggregates.get(name)
	}

	/// Check if a function is registered as an aggregate.
	pub fn is_aggregate(&self, name: &str) -> bool {
		self.aggregates.contains_key(name)
	}

	/// Iterate all registered aggregate functions.
	#[cfg(test)]
	pub(crate) fn aggregate_functions(&self) -> impl Iterator<Item = &Arc<dyn AggregateFunction>> {
		self.aggregates.values()
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
		self.id = fresh_id();
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
		self.id = fresh_id();
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
	// Row order
	// =========================================================================

	/// The argument positions, the receiver not counted, at which a builtin
	/// the method `name` resolves to runs the closure it is handed.
	pub(crate) fn method_closure_positions(&self, name: &str) -> Vec<usize> {
		let Some(descriptor) = self.get_method(name) else {
			return Vec::new();
		};
		let mut positions: Vec<usize> = descriptor
			.type_impls
			.iter()
			.map(|(_, function)| function)
			.chain(descriptor.fallback.iter())
			.flat_map(|function| function.closure_args().iter().copied())
			.filter_map(|at| at.checked_sub(1))
			.collect();
		positions.sort_unstable();
		positions.dedup();
		positions
	}

	/// Whether calls of the builtin `name` must run one after another in the
	/// order their rows are read: its declared effects make a second call
	/// answer differently or leave something behind
	/// ([`EffectSet::is_deterministic`](crate::expr::logical::EffectSet::is_deterministic)),
	/// so calls made for several rows at once answer what a serial run would
	/// not — a sequence's values out of order, requests sent concurrently.
	///
	/// A projection or index function reads the row or the index it is bound
	/// to and keeps no order. A name the registry does not hold keeps it.
	pub(crate) fn keeps_row_order(&self, name: &str) -> bool {
		match self.get(name) {
			Some(function) => !function.is_deterministic(),
			None => !(self.is_projection(name) || self.is_index_function(name)),
		}
	}

	/// The effects the method `name` may have: the union of the declared effects of every builtin
	/// it may resolve to, for any receiver. A name the registry does not hold resolves to a
	/// closure the receiver carries, whose body cannot be seen, and is
	/// [`EffectSet::OPAQUE_BODY`].
	pub(crate) fn method_effects(&self, name: &str) -> EffectSet {
		match self.get_method(name) {
			Some(descriptor) => descriptor
				.type_impls
				.iter()
				.map(|(_, function)| function)
				.chain(descriptor.fallback.iter())
				.fold(EffectSet::PURE, |set, function| set.union(function.effects())),
			None => EffectSet::OPAQUE_BODY,
		}
	}

	/// [`Self::keeps_row_order`] for the method `name`: whether a builtin it
	/// may resolve to, for any receiver, keeps row order. A name the registry
	/// does not hold resolves to a closure the receiver carries, whose body
	/// cannot be seen, and keeps it.
	pub(crate) fn method_keeps_row_order(&self, name: &str) -> bool {
		self.get_method(name).is_none_or(|descriptor| {
			descriptor
				.type_impls
				.iter()
				.map(|(_, function)| function)
				.chain(descriptor.fallback.iter())
				.any(|function| !function.is_deterministic())
		})
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
		registry.id = fresh_id();
		registry
	}

	/// Create a registry that holds the aggregate functions alone: the aggregates the text form
	/// of an IR container may hold, and the only functions a morsel unit names.
	pub(crate) fn with_aggregates() -> Self {
		let mut registry = Self::new();
		builtin::aggregates::register(&mut registry);
		registry
	}

	/// Create a registry with the builtins an entry that serves only data statements resolves.
	/// A name outside the set is unknown.
	pub fn with_edge_builtins() -> Self {
		let mut registry = Self::new();
		builtin::register_edge(&mut registry);
		registry.methods = method::build_method_registry(&registry);
		registry.id = fresh_id();
		registry
	}
}

/// Where the builtins and methods run a closure they are handed, for the
/// call-graph scan of stored bodies ([`crate::fnc::mutability`]).
impl crate::expr::function_facts::ClosurePositions for FunctionRegistry {
	fn function(&self, name: &str) -> Vec<usize> {
		self.get(name).map_or_else(Vec::new, |function| function.closure_args().to_vec())
	}

	fn method(&self, name: &str) -> Vec<usize> {
		self.method_closure_positions(name)
	}
}

impl Default for FunctionRegistry {
	fn default() -> Self {
		Self::with_builtins()
	}
}

/// The registry a builtin is registered in, which a name alone does not settle: `count` is both
/// a scalar function and an aggregate, and `vector::sum` likewise.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FnKind {
	Scalar,
	Aggregate,
	Projection,
	Index,
}

/// Whether two names are equal, in a `const` context.
const fn same_name(a: &str, b: &str) -> bool {
	let (a, b) = (a.as_bytes(), b.as_bytes());
	if a.len() != b.len() {
		return false;
	}
	let mut i = 0;
	while i < a.len() {
		if a[i] != b[i] {
			return false;
		}
		i += 1;
	}
	true
}

/// Fails constant evaluation unless [`FALLIBILITY`] holds a row for `(kind, name)` whose class is
/// exactly `declared`. Evaluated by every [`declare_fallibility!`](crate::declare_fallibility)
/// expansion, so a builtin with no row, or a row that disagrees with its declaration, does not
/// compile.
pub(crate) const fn check_fallibility_row(kind: FnKind, name: &str, declared: Fallibility) {
	let mut i = 0;
	while i < FALLIBILITY.len() {
		let (row_kind, row_name, class) = FALLIBILITY[i];
		if row_kind as u8 == kind as u8 && same_name(row_name, name) {
			assert!(
				class.is_exactly(declared),
				"a builtin's declared fallibility differs from its FALLIBILITY row"
			);
			return;
		}
		i += 1;
	}
	panic!("a builtin declares fallibility but has no FALLIBILITY row");
}

/// What each builtin's failure can depend on, by kind and name: one row for every registered
/// scalar function, aggregate, projection function and index function, each stating its class
/// whether or not it is [`Fallibility::INFALLIBLE`], so that no omission means anything.
///
/// A method is a scalar function reached by dot syntax and shares its row. Sorted by
/// `(kind, name)`. Every declaration site names its row through
/// [`declare_fallibility!`](crate::declare_fallibility), which fails the build when the row is
/// absent or names a different class; `every_builtin_declares_fallibility` pins the converse,
/// that every row names a registered builtin.
#[allow(dead_code)]
pub(crate) const FALLIBILITY: &[(FnKind, &str, Fallibility)] = &[
	(FnKind::Scalar, crate::expr::logical::trace::ANCHOR, Fallibility::INFALLIBLE),
	(FnKind::Scalar, crate::expr::logical::trace::LINK, Fallibility::INFALLIBLE),
	(
		FnKind::Scalar,
		"api::invoke",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::req::body",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::req::max_body",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::res::body",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::res::header",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::res::headers",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::res::status",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"api::timeout",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "array::add", Fallibility::VALUE),
	(FnKind::Scalar, "array::all", Fallibility::VALUE),
	(FnKind::Scalar, "array::any", Fallibility::VALUE),
	(FnKind::Scalar, "array::append", Fallibility::VALUE),
	(FnKind::Scalar, "array::at", Fallibility::VALUE),
	(FnKind::Scalar, "array::boolean_and", Fallibility::VALUE),
	(FnKind::Scalar, "array::boolean_not", Fallibility::VALUE),
	(FnKind::Scalar, "array::boolean_or", Fallibility::VALUE),
	(FnKind::Scalar, "array::boolean_xor", Fallibility::VALUE),
	(FnKind::Scalar, "array::clump", Fallibility::VALUE),
	(FnKind::Scalar, "array::combine", Fallibility::VALUE),
	(FnKind::Scalar, "array::complement", Fallibility::VALUE),
	(FnKind::Scalar, "array::concat", Fallibility::VALUE),
	(FnKind::Scalar, "array::difference", Fallibility::VALUE),
	(FnKind::Scalar, "array::distinct", Fallibility::VALUE),
	(FnKind::Scalar, "array::every", Fallibility::VALUE),
	(FnKind::Scalar, "array::fill", Fallibility::VALUE),
	(FnKind::Scalar, "array::filter", Fallibility::VALUE),
	(FnKind::Scalar, "array::filter_index", Fallibility::VALUE),
	(FnKind::Scalar, "array::find", Fallibility::VALUE),
	(FnKind::Scalar, "array::find_index", Fallibility::VALUE),
	(FnKind::Scalar, "array::first", Fallibility::VALUE),
	(FnKind::Scalar, "array::flatten", Fallibility::VALUE),
	(FnKind::Scalar, "array::fold", Fallibility::VALUE),
	(FnKind::Scalar, "array::group", Fallibility::VALUE),
	(FnKind::Scalar, "array::includes", Fallibility::VALUE),
	(FnKind::Scalar, "array::index_of", Fallibility::VALUE),
	(FnKind::Scalar, "array::insert", Fallibility::VALUE),
	(FnKind::Scalar, "array::intersect", Fallibility::VALUE),
	(FnKind::Scalar, "array::is_empty", Fallibility::VALUE),
	(FnKind::Scalar, "array::join", Fallibility::VALUE),
	(FnKind::Scalar, "array::last", Fallibility::VALUE),
	(FnKind::Scalar, "array::len", Fallibility::VALUE),
	(FnKind::Scalar, "array::logical_and", Fallibility::VALUE),
	(FnKind::Scalar, "array::logical_or", Fallibility::VALUE),
	(FnKind::Scalar, "array::logical_xor", Fallibility::VALUE),
	(FnKind::Scalar, "array::map", Fallibility::VALUE),
	(FnKind::Scalar, "array::matches", Fallibility::VALUE),
	(FnKind::Scalar, "array::max", Fallibility::VALUE),
	(FnKind::Scalar, "array::min", Fallibility::VALUE),
	(FnKind::Scalar, "array::pop", Fallibility::VALUE),
	(FnKind::Scalar, "array::prepend", Fallibility::VALUE),
	(FnKind::Scalar, "array::push", Fallibility::VALUE),
	(FnKind::Scalar, "array::range", Fallibility::VALUE),
	(FnKind::Scalar, "array::reduce", Fallibility::VALUE),
	(FnKind::Scalar, "array::remove", Fallibility::VALUE),
	(FnKind::Scalar, "array::repeat", Fallibility::VALUE),
	(FnKind::Scalar, "array::reverse", Fallibility::VALUE),
	(FnKind::Scalar, "array::sequence", Fallibility::VALUE),
	(FnKind::Scalar, "array::shuffle", Fallibility::VALUE),
	(FnKind::Scalar, "array::slice", Fallibility::VALUE),
	(FnKind::Scalar, "array::some", Fallibility::VALUE),
	(FnKind::Scalar, "array::sort", Fallibility::VALUE),
	(FnKind::Scalar, "array::sort::asc", Fallibility::VALUE),
	(FnKind::Scalar, "array::sort::desc", Fallibility::VALUE),
	(FnKind::Scalar, "array::sort_lexical", Fallibility::VALUE),
	(FnKind::Scalar, "array::sort_natural", Fallibility::VALUE),
	(FnKind::Scalar, "array::sort_natural_lexical", Fallibility::VALUE),
	(FnKind::Scalar, "array::swap", Fallibility::VALUE),
	(FnKind::Scalar, "array::transpose", Fallibility::VALUE),
	(FnKind::Scalar, "array::union", Fallibility::VALUE),
	(FnKind::Scalar, "array::windows", Fallibility::VALUE),
	(FnKind::Scalar, "bytes::len", Fallibility::VALUE),
	(FnKind::Scalar, "count", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "crypto::argon2::compare", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::argon2::generate", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::bcrypt::compare", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::bcrypt::generate", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::blake3", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::joaat", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::md5", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::pbkdf2::compare", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::pbkdf2::generate", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::scrypt::compare", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::scrypt::generate", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::sha1", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::sha256", Fallibility::VALUE),
	(FnKind::Scalar, "crypto::sha512", Fallibility::VALUE),
	(FnKind::Scalar, "duration::days", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "duration::from_days", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_hours", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_micros", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_millis", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_mins", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_nanos", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_secs", Fallibility::VALUE),
	(FnKind::Scalar, "duration::from_weeks", Fallibility::VALUE),
	(FnKind::Scalar, "duration::hours", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "duration::micros", Fallibility::VALUE),
	(FnKind::Scalar, "duration::millis", Fallibility::VALUE),
	(FnKind::Scalar, "duration::mins", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "duration::nanos", Fallibility::VALUE),
	(FnKind::Scalar, "duration::secs", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "duration::weeks", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "duration::years", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "encoding::base64::decode", Fallibility::VALUE),
	(FnKind::Scalar, "encoding::base64::encode", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "encoding::cbor::decode", Fallibility::VALUE),
	(FnKind::Scalar, "encoding::cbor::encode", Fallibility::VALUE),
	(FnKind::Scalar, "encoding::json::decode", Fallibility::VALUE),
	(FnKind::Scalar, "encoding::json::encode", Fallibility::VALUE),
	(FnKind::Scalar, "enum::cast", Fallibility::DATA),
	(FnKind::Scalar, "enum::max", Fallibility::DATA),
	(FnKind::Scalar, "enum::min", Fallibility::DATA),
	(FnKind::Scalar, "enum::rank", Fallibility::DATA),
	(FnKind::Scalar, "enum::values", Fallibility::DATA),
	(
		FnKind::Scalar,
		"eval::gql",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"eval::surql",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "file::bucket", Fallibility::VALUE),
	(
		FnKind::Scalar,
		"file::copy",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::copy_if_not_exists",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::delete",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::exists",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::get",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::head",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "file::key", Fallibility::VALUE),
	(
		FnKind::Scalar,
		"file::list",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::presign_get",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::presign_put",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::put",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::put_if_not_exists",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::rename",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"file::rename_if_not_exists",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "geo::area", Fallibility::VALUE),
	(FnKind::Scalar, "geo::bearing", Fallibility::VALUE),
	(FnKind::Scalar, "geo::centroid", Fallibility::VALUE),
	(FnKind::Scalar, "geo::distance", Fallibility::VALUE),
	(FnKind::Scalar, "geo::hash::decode", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "geo::hash::encode", Fallibility::VALUE),
	(FnKind::Scalar, "geo::is_valid", Fallibility::VALUE),
	(
		FnKind::Scalar,
		"http::delete",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"http::get",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"http::head",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"http::patch",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"http::post",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Scalar,
		"http::put",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "interval::days", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "interval::hours", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "interval::justify_days", Fallibility::VALUE),
	(FnKind::Scalar, "interval::justify_hours", Fallibility::VALUE),
	(FnKind::Scalar, "interval::justify_interval", Fallibility::VALUE),
	(FnKind::Scalar, "interval::make", Fallibility::VALUE),
	(FnKind::Scalar, "interval::micros", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "interval::mins", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "interval::months", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "interval::secs", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "interval::years", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "math::abs", Fallibility::VALUE),
	(FnKind::Scalar, "math::acos", Fallibility::VALUE),
	(FnKind::Scalar, "math::acot", Fallibility::VALUE),
	(FnKind::Scalar, "math::asin", Fallibility::VALUE),
	(FnKind::Scalar, "math::atan", Fallibility::VALUE),
	(FnKind::Scalar, "math::bottom", Fallibility::VALUE),
	(FnKind::Scalar, "math::ceil", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "math::clamp", Fallibility::VALUE),
	(FnKind::Scalar, "math::cos", Fallibility::VALUE),
	(FnKind::Scalar, "math::cot", Fallibility::VALUE),
	(FnKind::Scalar, "math::deg2rad", Fallibility::VALUE),
	(FnKind::Scalar, "math::fixed", Fallibility::VALUE),
	(FnKind::Scalar, "math::floor", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "math::interquartile", Fallibility::VALUE),
	(FnKind::Scalar, "math::lerp", Fallibility::VALUE),
	(FnKind::Scalar, "math::lerpangle", Fallibility::VALUE),
	(FnKind::Scalar, "math::ln", Fallibility::VALUE),
	(FnKind::Scalar, "math::log", Fallibility::VALUE),
	(FnKind::Scalar, "math::log10", Fallibility::VALUE),
	(FnKind::Scalar, "math::log2", Fallibility::VALUE),
	(FnKind::Scalar, "math::max", Fallibility::VALUE),
	(FnKind::Scalar, "math::mean", Fallibility::VALUE),
	(FnKind::Scalar, "math::median", Fallibility::VALUE),
	(FnKind::Scalar, "math::midhinge", Fallibility::VALUE),
	(FnKind::Scalar, "math::min", Fallibility::VALUE),
	(FnKind::Scalar, "math::mode", Fallibility::VALUE),
	(FnKind::Scalar, "math::nearestrank", Fallibility::VALUE),
	(FnKind::Scalar, "math::percentile", Fallibility::VALUE),
	(FnKind::Scalar, "math::pow", Fallibility::VALUE),
	(FnKind::Scalar, "math::product", Fallibility::VALUE),
	(FnKind::Scalar, "math::rad2deg", Fallibility::VALUE),
	(FnKind::Scalar, "math::round", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "math::sign", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "math::sin", Fallibility::VALUE),
	(FnKind::Scalar, "math::spread", Fallibility::VALUE),
	(FnKind::Scalar, "math::sqrt", Fallibility::VALUE),
	(FnKind::Scalar, "math::stddev", Fallibility::VALUE),
	(FnKind::Scalar, "math::sum", Fallibility::VALUE),
	(FnKind::Scalar, "math::tan", Fallibility::VALUE),
	(FnKind::Scalar, "math::top", Fallibility::VALUE),
	(FnKind::Scalar, "math::trimean", Fallibility::VALUE),
	(FnKind::Scalar, "math::variance", Fallibility::VALUE),
	(FnKind::Scalar, "meta::id", Fallibility::VALUE),
	(FnKind::Scalar, "meta::tb", Fallibility::VALUE),
	(FnKind::Scalar, "not", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "object::entries", Fallibility::VALUE),
	(FnKind::Scalar, "object::extend", Fallibility::VALUE),
	(FnKind::Scalar, "object::from_entries", Fallibility::VALUE),
	(FnKind::Scalar, "object::is_empty", Fallibility::VALUE),
	(FnKind::Scalar, "object::keys", Fallibility::VALUE),
	(FnKind::Scalar, "object::len", Fallibility::VALUE),
	(FnKind::Scalar, "object::remove", Fallibility::VALUE),
	(FnKind::Scalar, "object::values", Fallibility::VALUE),
	(FnKind::Scalar, "parse::email::host", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::email::user", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::domain", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::fragment", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::host", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::path", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::port", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::query", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "parse::url::scheme", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "rand", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "rand::bool", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "rand::duration", Fallibility::VALUE),
	(FnKind::Scalar, "rand::enum", Fallibility::VALUE),
	(FnKind::Scalar, "rand::float", Fallibility::VALUE),
	(FnKind::Scalar, "rand::id", Fallibility::VALUE),
	(FnKind::Scalar, "rand::int", Fallibility::VALUE),
	(FnKind::Scalar, "rand::string", Fallibility::VALUE),
	(FnKind::Scalar, "rand::time", Fallibility::VALUE),
	(FnKind::Scalar, "rand::ulid", Fallibility::VALUE),
	(FnKind::Scalar, "rand::uuid", Fallibility::VALUE),
	(FnKind::Scalar, "rand::uuid::v4", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "rand::uuid::v7", Fallibility::VALUE),
	(
		FnKind::Scalar,
		"record::exists",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "record::id", Fallibility::VALUE),
	(
		FnKind::Scalar,
		"record::is_edge",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "record::table", Fallibility::VALUE),
	(FnKind::Scalar, "record::tb", Fallibility::VALUE),
	(FnKind::Scalar, "schema::table::exists", Fallibility::DATA),
	(
		FnKind::Scalar,
		"search::analyze",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "search::linear", Fallibility::VALUE),
	(FnKind::Scalar, "search::rrf", Fallibility::VALUE),
	(FnKind::Scalar, "sequence::nextval", Fallibility::DATA),
	(FnKind::Scalar, "session::ac", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::db", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::id", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::ip", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::ns", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::origin", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::rd", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "session::token", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "set::add", Fallibility::VALUE),
	(FnKind::Scalar, "set::all", Fallibility::VALUE),
	(FnKind::Scalar, "set::any", Fallibility::VALUE),
	(FnKind::Scalar, "set::at", Fallibility::VALUE),
	(FnKind::Scalar, "set::complement", Fallibility::VALUE),
	(FnKind::Scalar, "set::contains", Fallibility::VALUE),
	(FnKind::Scalar, "set::difference", Fallibility::VALUE),
	(FnKind::Scalar, "set::filter", Fallibility::VALUE),
	(FnKind::Scalar, "set::find", Fallibility::VALUE),
	(FnKind::Scalar, "set::first", Fallibility::VALUE),
	(FnKind::Scalar, "set::flatten", Fallibility::VALUE),
	(FnKind::Scalar, "set::fold", Fallibility::VALUE),
	(FnKind::Scalar, "set::intersect", Fallibility::VALUE),
	(FnKind::Scalar, "set::is_empty", Fallibility::VALUE),
	(FnKind::Scalar, "set::join", Fallibility::VALUE),
	(FnKind::Scalar, "set::last", Fallibility::VALUE),
	(FnKind::Scalar, "set::len", Fallibility::VALUE),
	(FnKind::Scalar, "set::map", Fallibility::VALUE),
	(FnKind::Scalar, "set::max", Fallibility::VALUE),
	(FnKind::Scalar, "set::min", Fallibility::VALUE),
	(FnKind::Scalar, "set::reduce", Fallibility::VALUE),
	(FnKind::Scalar, "set::remove", Fallibility::VALUE),
	(FnKind::Scalar, "set::slice", Fallibility::VALUE),
	(FnKind::Scalar, "set::union", Fallibility::VALUE),
	(
		FnKind::Scalar,
		"sleep",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Scalar, "sql::fit", Fallibility::VALUE),
	(FnKind::Scalar, "sql::network_key", Fallibility::VALUE),
	(FnKind::Scalar, "sql::network_text", Fallibility::VALUE),
	(FnKind::Scalar, "sql::range_contains", Fallibility::VALUE),
	(FnKind::Scalar, "sql::range_lower", Fallibility::VALUE),
	(FnKind::Scalar, "sql::range_overlaps", Fallibility::VALUE),
	(FnKind::Scalar, "sql::range_upper", Fallibility::VALUE),
	(FnKind::Scalar, "sql::text", Fallibility::VALUE),
	(FnKind::Scalar, "string::capitalize", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::concat", Fallibility::VALUE),
	(FnKind::Scalar, "string::contains", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::distance::damerau_levenshtein", Fallibility::VALUE),
	(FnKind::Scalar, "string::distance::hamming", Fallibility::VALUE),
	(FnKind::Scalar, "string::distance::levenshtein", Fallibility::VALUE),
	(FnKind::Scalar, "string::distance::normalized_damerau_levenshtein", Fallibility::VALUE),
	(FnKind::Scalar, "string::distance::normalized_levenshtein", Fallibility::VALUE),
	(FnKind::Scalar, "string::distance::osa", Fallibility::VALUE),
	(FnKind::Scalar, "string::ends_with", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::html::encode", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::html::sanitize", Fallibility::VALUE),
	(FnKind::Scalar, "string::is_alpha", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_alphanum", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_ascii", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_datetime", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_domain", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_email", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_hexadecimal", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_ip", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_ipv4", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_ipv6", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_latitude", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_longitude", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_numeric", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_record", Fallibility::VALUE),
	(FnKind::Scalar, "string::is_semver", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_ulid", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_url", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::is_uuid", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::join", Fallibility::VALUE),
	(FnKind::Scalar, "string::len", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::lowercase", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::matches", Fallibility::VALUE),
	(FnKind::Scalar, "string::repeat", Fallibility::VALUE),
	(FnKind::Scalar, "string::replace", Fallibility::VALUE),
	(FnKind::Scalar, "string::reverse", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::semver::compare", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::inc::major", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::inc::minor", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::inc::patch", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::major", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::minor", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::patch", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::set::major", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::set::minor", Fallibility::VALUE),
	(FnKind::Scalar, "string::semver::set::patch", Fallibility::VALUE),
	(FnKind::Scalar, "string::similarity::fuzzy", Fallibility::VALUE),
	(FnKind::Scalar, "string::similarity::jaro", Fallibility::VALUE),
	(FnKind::Scalar, "string::similarity::jaro_winkler", Fallibility::VALUE),
	(FnKind::Scalar, "string::similarity::smithwaterman", Fallibility::VALUE),
	(FnKind::Scalar, "string::similarity::sorensen_dice", Fallibility::VALUE),
	(FnKind::Scalar, "string::slice", Fallibility::VALUE),
	(FnKind::Scalar, "string::slug", Fallibility::VALUE),
	(FnKind::Scalar, "string::split", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::starts_with", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::trim", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::uppercase", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "string::words", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::age", Fallibility::VALUE),
	(FnKind::Scalar, "time::ceil", Fallibility::VALUE),
	(FnKind::Scalar, "time::day", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::extract", Fallibility::VALUE),
	(FnKind::Scalar, "time::floor", Fallibility::VALUE),
	(FnKind::Scalar, "time::format", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_micros", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_millis", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_nanos", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_secs", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_ulid", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_unix", Fallibility::VALUE),
	(FnKind::Scalar, "time::from_uuid", Fallibility::VALUE),
	(FnKind::Scalar, "time::group", Fallibility::VALUE),
	(FnKind::Scalar, "time::hour", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::is_leap_year", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::make_date", Fallibility::VALUE),
	(FnKind::Scalar, "time::make_time", Fallibility::VALUE),
	(FnKind::Scalar, "time::make_timestamp", Fallibility::VALUE),
	(FnKind::Scalar, "time::max", Fallibility::VALUE),
	(FnKind::Scalar, "time::micros", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::millis", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::min", Fallibility::VALUE),
	(FnKind::Scalar, "time::minute", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::month", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::nano", Fallibility::VALUE),
	(FnKind::Scalar, "time::now", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::round", Fallibility::VALUE),
	(FnKind::Scalar, "time::second", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::set_day", Fallibility::VALUE),
	(FnKind::Scalar, "time::set_hour", Fallibility::VALUE),
	(FnKind::Scalar, "time::set_minute", Fallibility::VALUE),
	(FnKind::Scalar, "time::set_month", Fallibility::VALUE),
	(FnKind::Scalar, "time::set_nanosecond", Fallibility::VALUE),
	(FnKind::Scalar, "time::set_second", Fallibility::VALUE),
	(FnKind::Scalar, "time::set_year", Fallibility::VALUE),
	(FnKind::Scalar, "time::timezone", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::trunc", Fallibility::VALUE),
	(FnKind::Scalar, "time::unix", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::wday", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::week", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::yday", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "time::year", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::array", Fallibility::VALUE),
	(FnKind::Scalar, "type::bool", Fallibility::VALUE),
	(FnKind::Scalar, "type::bytes", Fallibility::VALUE),
	(FnKind::Scalar, "type::date", Fallibility::VALUE),
	(FnKind::Scalar, "type::datetime", Fallibility::VALUE),
	(FnKind::Scalar, "type::decimal", Fallibility::VALUE),
	(FnKind::Scalar, "type::duration", Fallibility::VALUE),
	(FnKind::Scalar, "type::f16", Fallibility::VALUE),
	(FnKind::Scalar, "type::f32", Fallibility::VALUE),
	(FnKind::Scalar, "type::file", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::float", Fallibility::VALUE),
	(FnKind::Scalar, "type::geometry", Fallibility::VALUE),
	(FnKind::Scalar, "type::i16", Fallibility::VALUE),
	(FnKind::Scalar, "type::i32", Fallibility::VALUE),
	(FnKind::Scalar, "type::i8", Fallibility::VALUE),
	(FnKind::Scalar, "type::int", Fallibility::VALUE),
	(FnKind::Scalar, "type::interval", Fallibility::VALUE),
	(FnKind::Scalar, "type::is_array", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_bool", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_bytes", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_collection", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_date", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_datetime", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_decimal", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_duration", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_f16", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_f32", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_float", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_geometry", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_i16", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_i32", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_i8", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_int", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_interval", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_line", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_multiline", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_multipoint", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_multipolygon", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_none", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_null", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_number", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_object", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_point", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_polygon", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_range", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_record", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_set", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_string", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_time", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_timestamp", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_u16", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_u32", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_u64", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_u8", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::is_uuid", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::number", Fallibility::VALUE),
	(FnKind::Scalar, "type::of", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "type::point", Fallibility::VALUE),
	(FnKind::Scalar, "type::range", Fallibility::VALUE),
	(FnKind::Scalar, "type::record", Fallibility::VALUE),
	(FnKind::Scalar, "type::set", Fallibility::VALUE),
	(FnKind::Scalar, "type::string", Fallibility::VALUE),
	(FnKind::Scalar, "type::string_lossy", Fallibility::VALUE),
	(FnKind::Scalar, "type::table", Fallibility::VALUE),
	(FnKind::Scalar, "type::time", Fallibility::VALUE),
	(FnKind::Scalar, "type::timestamp", Fallibility::VALUE),
	(FnKind::Scalar, "type::u16", Fallibility::VALUE),
	(FnKind::Scalar, "type::u32", Fallibility::VALUE),
	(FnKind::Scalar, "type::u64", Fallibility::VALUE),
	(FnKind::Scalar, "type::u8", Fallibility::VALUE),
	(FnKind::Scalar, "type::uuid", Fallibility::VALUE),
	(FnKind::Scalar, "value::chain", Fallibility::VALUE),
	(FnKind::Scalar, "value::diff", Fallibility::INFALLIBLE),
	(FnKind::Scalar, "value::expect", Fallibility::VALUE),
	(FnKind::Scalar, "value::patch", Fallibility::VALUE),
	(FnKind::Scalar, "vector::add", Fallibility::VALUE),
	(FnKind::Scalar, "vector::angle", Fallibility::VALUE),
	(FnKind::Scalar, "vector::cross", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::chebyshev", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::cosine", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::euclidean", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::hamming", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::mahalanobis", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::manhattan", Fallibility::VALUE),
	(FnKind::Scalar, "vector::distance::minkowski", Fallibility::VALUE),
	(FnKind::Scalar, "vector::divide", Fallibility::VALUE),
	(FnKind::Scalar, "vector::dot", Fallibility::VALUE),
	(FnKind::Scalar, "vector::magnitude", Fallibility::VALUE),
	(FnKind::Scalar, "vector::multiply", Fallibility::VALUE),
	(FnKind::Scalar, "vector::normalize", Fallibility::VALUE),
	(FnKind::Scalar, "vector::project", Fallibility::VALUE),
	(FnKind::Scalar, "vector::scale", Fallibility::VALUE),
	(FnKind::Scalar, "vector::similarity::cosine", Fallibility::VALUE),
	(FnKind::Scalar, "vector::similarity::jaccard", Fallibility::VALUE),
	(FnKind::Scalar, "vector::similarity::pearson", Fallibility::VALUE),
	(FnKind::Scalar, "vector::similarity::spearman", Fallibility::VALUE),
	(FnKind::Scalar, "vector::subtract", Fallibility::VALUE),
	(FnKind::Scalar, "vector::sum", Fallibility::VALUE),
	(FnKind::Aggregate, "array::collect", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "array::distinct", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "array::group", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "array::join", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "array::max", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "array::min", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "count", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "count::present", Fallibility::INFALLIBLE),
	(FnKind::Aggregate, "math::max", Fallibility::VALUE),
	(FnKind::Aggregate, "math::mean", Fallibility::VALUE),
	(FnKind::Aggregate, "math::mean_distinct", Fallibility::VALUE),
	(FnKind::Aggregate, "math::median", Fallibility::VALUE),
	(FnKind::Aggregate, "math::min", Fallibility::VALUE),
	(FnKind::Aggregate, "math::stddev", Fallibility::VALUE),
	(FnKind::Aggregate, "math::sum", Fallibility::VALUE),
	(FnKind::Aggregate, "math::sum_distinct", Fallibility::VALUE),
	(FnKind::Aggregate, "math::variance", Fallibility::VALUE),
	(FnKind::Aggregate, "time::max", Fallibility::VALUE),
	(FnKind::Aggregate, "time::min", Fallibility::VALUE),
	(FnKind::Aggregate, "vector::sum", Fallibility::INFALLIBLE),
	(
		FnKind::Projection,
		"type::field",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(
		FnKind::Projection,
		"type::fields",
		Fallibility::VALUE.union(Fallibility::CONTEXT).union(Fallibility::DATA),
	),
	(FnKind::Index, "search::highlight", Fallibility::VALUE.union(Fallibility::DATA)),
	(FnKind::Index, "search::offsets", Fallibility::DATA),
	(FnKind::Index, "search::score", Fallibility::DATA),
	(FnKind::Index, "vector::distance::knn", Fallibility::DATA),
];

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

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
	fn a_registry_changed_by_a_registration_is_another_registry() {
		let mut registry = FunctionRegistry::with_builtins();
		let other = FunctionRegistry::with_builtins();
		assert_ne!(registry.id(), 0);
		assert_ne!(registry.id(), other.id());
		let id = registry.id();
		registry.register_aggregate(builtin::aggregates::CountField);
		assert_ne!(registry.id(), id);
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
		"file::presign_get",
		"file::presign_put",
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
	/// answer into the whole statement, and the membership recogniser would
	/// treat an expression containing it as pure and so build the subquery that
	/// holds it once for every outer row, where each owes a fresh answer.
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

		// The check only means something when the set is non-empty, so an empty result fails here.
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
		(crate::expr::logical::trace::ANCHOR, E::READS_ROW),
		(crate::expr::logical::trace::LINK, E::READS_ROW),
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
		("enum::cast", E::READS_CATALOG),
		("enum::max", E::READS_CATALOG),
		("enum::min", E::READS_CATALOG),
		("enum::rank", E::READS_CATALOG),
		("enum::values", E::READS_CATALOG),
		("eval::gql", E::OPAQUE_BODY.union(E::WRITES_DATA)),
		("eval::surql", E::OPAQUE_BODY.union(E::WRITES_DATA)),
		("file::copy", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("file::copy_if_not_exists", E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)),
		("file::delete", E::WRITES_EXTERNAL),
		("file::exists", E::READS_EXTERNAL),
		("file::get", E::READS_EXTERNAL),
		("file::head", E::READS_EXTERNAL),
		("file::list", E::READS_EXTERNAL),
		("file::presign_get", E::READS_EXTERNAL),
		("file::presign_put", E::READS_EXTERNAL),
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
		("session::ac", E::READS_PRINCIPAL),
		("session::db", E::READS_SESSION_DB),
		("session::id", E::READS_PRINCIPAL),
		("session::ip", E::READS_PRINCIPAL),
		("session::ns", E::READS_SESSION_DB),
		("session::origin", E::READS_PRINCIPAL),
		("session::rd", E::READS_PRINCIPAL),
		("session::token", E::READS_PRINCIPAL),
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

		// `mutates_state_builtin` is what the runtime write refusal asks about a
		// call, so a builtin that declares a mutating effect and is missing from
		// that list is a context that may not modify state being able to. The
		// list is a name table because the refusal runs in the expression layer,
		// which cannot see this registry; this is what keeps the two in step.
		for f in registry.scalar_functions() {
			assert_eq!(
				crate::expr::method::mutates_state_builtin(f.name()),
				f.effects().mutates_state(),
				"{}: `mutates_state_builtin` and `EffectSet::mutates_state` disagree",
				f.name()
			);
		}
	}

	/// Every registered builtin has a [`FALLIBILITY`] row equal to what its declaration answers,
	/// and every row names a registered builtin.
	///
	/// A method resolves to a scalar function the registry holds, so it is covered by that
	/// function's row; the check below reads each method's implementations by `name()` to pin
	/// that. A build that leaves a feature-gated family out registers stand-ins for it, so the
	/// name-set and row equality are only asserted when every family is present; the implications
	/// on a declaration hold in every build.
	#[test]
	fn every_builtin_declares_fallibility() {
		let registry = FunctionRegistry::with_builtins();

		assert!(
			FALLIBILITY.windows(2).all(|w| (w[0].0, w[0].1) < (w[1].0, w[1].1)),
			"keep `FALLIBILITY` sorted by (kind, name), one row per builtin"
		);

		let mut actual: BTreeMap<(FnKind, &str), Fallibility> = BTreeMap::new();
		for f in registry.scalar_functions() {
			actual.insert((FnKind::Scalar, f.name()), f.fallibility());
		}
		for f in registry.aggregate_functions() {
			actual.insert((FnKind::Aggregate, f.name()), f.fallibility());
		}
		for f in registry.projection_functions() {
			actual.insert((FnKind::Projection, f.name()), f.fallibility());
		}
		for f in registry.index_functions() {
			actual.insert((FnKind::Index, f.name()), f.fallibility());
		}
		let table: BTreeMap<(FnKind, &str), Fallibility> =
			FALLIBILITY.iter().map(|&(kind, name, class)| ((kind, name), class)).collect();

		if cfg!(all(
			feature = "api",
			feature = "encoding",
			feature = "files",
			feature = "search",
			feature = "url"
		)) {
			let missing: Vec<_> = actual.keys().filter(|key| !table.contains_key(*key)).collect();
			let extra: Vec<_> = table.keys().filter(|key| !actual.contains_key(*key)).collect();
			assert!(
				missing.is_empty() && extra.is_empty(),
				"`FALLIBILITY` and the registry name different builtins; \
				 missing a row: {missing:?}, a row naming no builtin: {extra:?}"
			);
			for (key, class) in &actual {
				assert_eq!(
					table[key], *class,
					"{key:?}: the `FALLIBILITY` row differs from the declaration"
				);
			}
		}

		// A method's implementations are scalar functions the registry holds, and the table
		// answers for them under the same name.
		for descriptor in registry.methods().descriptors() {
			let implementations = descriptor
				.type_impls
				.iter()
				.map(|(_, function)| function)
				.chain(descriptor.fallback.iter());
			for function in implementations {
				let held = registry.get(function.name()).unwrap_or_else(|| {
					panic!("method `{}`: `{}` is not registered", descriptor.name, function.name())
				});
				assert_eq!(
					held.fallibility(),
					function.fallibility(),
					"method `{}`: `{}` answers differently from the registered function",
					descriptor.name,
					function.name()
				);
				assert!(
					table.contains_key(&(FnKind::Scalar, function.name())),
					"method `{}`: `{}` has no `FALLIBILITY` row",
					descriptor.name,
					function.name()
				);
			}
		}

		// The floor a body's effects put under its class: a declaration may name more, never less.
		let data_and_context = Fallibility::CONTEXT | Fallibility::DATA;
		for f in registry.scalar_functions() {
			let (effects, class) = (f.effects(), f.fallibility());
			if effects.contains(E::READS_DATA) || effects.writes_data() {
				assert!(
					class.contains(Fallibility::DATA),
					"{}: reads or writes data, so `DATA`",
					f.name()
				);
			}
			if effects.contains(E::OPAQUE_BODY) {
				assert!(
					class.contains(data_and_context),
					"{}: runs a body it cannot see, so `CONTEXT | DATA`",
					f.name()
				);
			}
			if effects.intersects(E::READS_EXTERNAL.union(E::WRITES_EXTERNAL)) {
				assert!(
					class.contains(data_and_context),
					"{}: reaches outside the transaction, so `CONTEXT | DATA`",
					f.name()
				);
			}
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

	/// The synchronous `invoke` of a metered builtin drives the builtin's walk to
	/// its end, as the synchronous reference in `fnc` does: the same answer, the
	/// same fuel charged, and the same error for a call that does not decode.
	#[test]
	fn invoke_of_a_metered_builtin_is_its_synchronous_reference() {
		use surrealdb_runtime::fuel;

		use crate::fnc::args::{FromArgs, Optional};
		use crate::val::{Array, Number, Object, Set, Value};

		type Reference = fn(Vec<Value>) -> anyhow::Result<Value>;

		let ints = |range: std::ops::Range<i64>| -> Vec<Value> { range.map(Value::from).collect() };
		let reversed = Value::Array(Array(ints(0..10_000).into_iter().rev().collect()));
		let repeated = Value::Array(Array((0..10_000).map(|n| Value::from(n % 97)).collect()));
		let numbers = Value::Array(Array(ints(0..10_000)));
		let object = Value::Object(
			(0..5_000).map(|n| (format!("k{n}"), Value::from(n))).collect::<Object>(),
		);
		let set_of =
			|range: std::ops::Range<i64>| Value::Set(range.map(Value::from).collect::<Set>());
		let words: Vec<Value> = (0..2_000).map(|n| Value::from(format!("word-{n}"))).collect();

		let cases: Vec<(&str, Vec<Value>, Reference)> = vec![
			("array::sort", vec![reversed], |args| {
				crate::fnc::array::sort(<(Array, Optional<Value>)>::from_args("array::sort", args)?)
			}),
			("array::distinct", vec![repeated], |args| {
				crate::fnc::array::distinct(<(Array,)>::from_args("array::distinct", args)?)
			}),
			("math::sum", vec![numbers], |args| {
				crate::fnc::math::sum(<(Vec<Number>,)>::from_args("math::sum", args)?)
			}),
			("object::keys", vec![object], |args| {
				crate::fnc::object::keys(<(Object,)>::from_args("object::keys", args)?)
			}),
			("string::concat", words, |args| {
				crate::fnc::string::concat(FromArgs::from_args("string::concat", args)?)
			}),
			("set::union", vec![set_of(0..6_000), set_of(3_000..9_000)], |args| {
				crate::fnc::set::union(<(Set, Set)>::from_args("set::union", args)?)
			}),
		];

		let registry = FunctionRegistry::with_builtins();
		let outcome = |result: anyhow::Result<Value>| result.map_err(|e| e.to_string());
		for (name, args, reference) in &cases {
			let function = registry.get(name).expect("a registered builtin");
			assert!(function.metered().is_some(), "{name}: a metered builtin");
			for quantum in [64, 1_000_000] {
				fuel::refill(quantum);
				let got = outcome(function.invoke(args.clone()));
				let got_left = fuel::left();
				fuel::refill(quantum);
				let want = outcome(reference(args.clone()));
				let want_left = fuel::left();
				assert!(got.is_ok(), "{name}: {got:?}");
				assert_eq!(got, want, "{name}: the answer, at a quantum of {quantum}");
				assert_eq!(got_left, want_left, "{name}: the fuel left, at a quantum of {quantum}");
			}
		}

		// A call that does not decode raises what decoding raises.
		let sort = registry.get("array::sort").expect("a registered builtin");
		let sort_reference = &cases[0].2;
		for args in [vec![Value::from(1)], vec![]] {
			let got = outcome(sort.invoke(args.clone()));
			let want = outcome(sort_reference(args.clone()));
			assert!(got.is_err(), "{args:?}: a call that does not decode");
			assert_eq!(got, want, "{args:?}");
		}
	}

	/// The edge registry holds the edge tables alone, and the full registry holds
	/// every table's entries once: a name in two tables would register once and
	/// leave the sum of the table lengths above the registry's length.
	#[test]
	fn the_edge_registry_holds_the_edge_tables_alone() {
		let (edge, rest) = builtin::scalar_tables();
		let total =
			|tables: &[&[&dyn ScalarFunction]]| tables.iter().map(|t| t.len()).sum::<usize>();

		let edge_registry = FunctionRegistry::with_edge_builtins();
		for table in builtin::edge_excluded_tables() {
			for function in table {
				assert!(
					edge_registry.get(function.name()).is_none(),
					"{}: a builtin the edge registry leaves out",
					function.name()
				);
			}
		}
		assert_eq!(edge_registry.len(), total(edge));
		assert_eq!(FunctionRegistry::with_builtins().len(), total(edge) + total(rest));
	}
}
