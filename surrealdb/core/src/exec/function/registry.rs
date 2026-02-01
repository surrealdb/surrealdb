//! Function registry for storing and looking up scalar functions.

use std::collections::HashMap;
use std::sync::Arc;

use super::{ScalarFunction, builtin};

/// Registry of scalar functions available during query execution.
///
/// The registry maps function names to their implementations.
/// It is typically created once at startup with all built-in functions
/// and then shared across execution contexts.
#[derive(Debug)]
pub struct FunctionRegistry {
	functions: HashMap<&'static str, Arc<dyn ScalarFunction>>,
}

impl FunctionRegistry {
	/// Create a new empty registry.
	pub fn new() -> Self {
		Self {
			functions: HashMap::new(),
		}
	}

	/// Register a function in the registry.
	pub fn register(&mut self, func: impl ScalarFunction + 'static) {
		let name = func.name();
		self.functions.insert(name, Arc::new(func));
	}

	/// Register a function wrapped in an Arc.
	pub fn register_arc(&mut self, func: Arc<dyn ScalarFunction>) {
		let name = func.name();
		self.functions.insert(name, func);
	}

	/// Look up a function by name.
	pub fn get(&self, name: &str) -> Option<&Arc<dyn ScalarFunction>> {
		self.functions.get(name)
	}

	/// Check if a function exists.
	pub fn contains(&self, name: &str) -> bool {
		self.functions.contains_key(name)
	}

	/// Get the number of registered functions.
	pub fn len(&self) -> usize {
		self.functions.len()
	}

	/// Check if the registry is empty.
	pub fn is_empty(&self) -> bool {
		self.functions.is_empty()
	}

	/// Iterate over all registered functions.
	pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Arc<dyn ScalarFunction>)> {
		self.functions.iter().map(|(k, v)| (*k, v))
	}

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

		// Check some known functions exist
		assert!(registry.contains("math::abs"));
		assert!(registry.contains("string::len"));
		assert!(registry.contains("array::len"));

		// Check unknown function doesn't exist
		assert!(!registry.contains("unknown::function"));

		// Should have many functions
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
}
