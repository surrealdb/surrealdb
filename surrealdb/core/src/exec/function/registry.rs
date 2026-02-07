//! Function registry for storing and looking up scalar, aggregate, and projection functions.

use std::collections::HashMap;
use std::sync::Arc;

use super::method::{self, MethodDescriptor, MethodRegistry};
use super::{AggregateFunction, ProjectionFunction, ScalarFunction, builtin};

/// Registry of functions available during query execution.
///
/// The registry maps function names to their implementations for:
/// - Scalar functions (operate on individual values, return single value)
/// - Aggregate functions (operate over groups of values)
/// - Projection functions (produce field bindings for output objects)
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

	/// Register a scalar function wrapped in an Arc.
	pub fn register_arc(&mut self, func: Arc<dyn ScalarFunction>) {
		let name = func.name();
		self.functions.insert(name, func);
	}

	/// Look up a scalar function by name.
	pub fn get(&self, name: &str) -> Option<&Arc<dyn ScalarFunction>> {
		self.functions.get(name)
	}

	/// Check if a scalar function exists.
	pub fn contains(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	/// Get the number of registered scalar functions.
	pub fn len(&self) -> usize {
		self.functions.len()
	}

	/// Check if the scalar function registry is empty.
	pub fn is_empty(&self) -> bool {
		self.functions.is_empty()
	}

	/// Iterate over all registered scalar functions.
	pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Arc<dyn ScalarFunction>)> {
		self.functions.iter().map(|(k, v)| (*k, v))
	}

	// =========================================================================
	// Aggregate function methods
	// =========================================================================

	/// Register an aggregate function in the registry.
	pub fn register_aggregate(&mut self, func: impl AggregateFunction + 'static) {
		let name = func.name();
		self.aggregates.insert(name, Arc::new(func));
	}

	/// Register an aggregate function wrapped in an Arc.
	pub fn register_aggregate_arc(&mut self, func: Arc<dyn AggregateFunction>) {
		let name = func.name();
		self.aggregates.insert(name, func);
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
	pub fn aggregate_len(&self) -> usize {
		self.aggregates.len()
	}

	/// Iterate over all registered aggregate functions.
	pub fn iter_aggregates(
		&self,
	) -> impl Iterator<Item = (&'static str, &Arc<dyn AggregateFunction>)> {
		self.aggregates.iter().map(|(k, v)| (*k, v))
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
			self.aggregates.get("count").expect("count should be registered").clone()
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

	/// Register a projection function wrapped in an Arc.
	pub fn register_projection_arc(&mut self, func: Arc<dyn ProjectionFunction>) {
		let name = func.name();
		self.projections.insert(name, func);
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
	pub fn projection_len(&self) -> usize {
		self.projections.len()
	}

	/// Iterate over all registered projection functions.
	pub fn iter_projections(
		&self,
	) -> impl Iterator<Item = (&'static str, &Arc<dyn ProjectionFunction>)> {
		self.projections.iter().map(|(k, v)| (*k, v))
	}

	// =========================================================================
	// Method registry methods
	// =========================================================================

	/// Look up a method descriptor by method name.
	pub fn get_method(&self, name: &str) -> Option<&Arc<MethodDescriptor>> {
		self.methods.get(name)
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

#[cfg(test)]
mod tests {
	use super::*;

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
}
