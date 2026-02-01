//! Function registry for storing and looking up scalar and aggregate functions.

use std::collections::HashMap;
use std::sync::Arc;

use super::{AggregateFunction, ScalarFunction, builtin};

/// Registry of functions available during query execution.
///
/// The registry maps function names to their implementations for both
/// scalar functions (operate on individual values) and aggregate functions
/// (operate over groups of values).
///
/// It is typically created once at startup with all built-in functions
/// and then shared across execution contexts.
#[derive(Debug)]
pub struct FunctionRegistry {
	/// Scalar functions (e.g., math::abs, string::len)
	functions: HashMap<&'static str, Arc<dyn ScalarFunction>>,
	/// Aggregate functions (e.g., count, math::sum, math::mean)
	aggregates: HashMap<&'static str, Arc<dyn AggregateFunction>>,
}

impl FunctionRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			functions: HashMap::new(),
			aggregates: HashMap::new(),
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

	// =========================================================================
	// Combined methods
	// =========================================================================

	/// Create a registry with all built-in functions.
	pub fn with_builtins() -> Self {
		let mut registry = Self::new();
		builtin::register_all(&mut registry);
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
}
