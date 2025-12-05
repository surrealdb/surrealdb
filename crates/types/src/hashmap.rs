//! A concurrent hashmap wrapper around papaya's HashMap.
//!
//! This wrapper provides a safe and performant interface to papaya's HashMap
//! by using `pin()` internally rather than `pin_owned()`. Using `pin_owned()`
//! can be a significant performance killer, so this wrapper encourages the
//! correct usage pattern.
//!
//! # Performance Note
//!
//! Since this wrapper avoids returning `pin` lifetimes by cloning values where
//! necessary, you should ensure that your value types are cheap to clone. For
//! types that are expensive to clone, consider wrapping them in an [`Arc`].
//!
//! [`Arc`]: std::sync::Arc
//!
//! # Example
//!
//! ```rust
//! use surrealdb_types::HashMap;
//!
//! let map: HashMap<String, i32> = HashMap::new();
//! map.insert("key".to_string(), 42);
//!
//! if let Some(value) = map.get(&"key".to_string()) {
//!     assert_eq!(value, 42);
//! }
//! ```

use std::collections::hash_map::RandomState;
use std::{cmp, hash};

use papaya::Equivalent;

/// A concurrent hashmap wrapper around papaya's HashMap.
///
/// This wrapper uses `pin()` internally rather than `pin_owned()` to avoid
/// performance issues. All operations clone values on access, which is
/// suitable for types that implement `Clone`.
///
/// # Type Parameters
///
/// * `K` - The key type, must implement `Hash` and `Eq`
/// * `V` - The value type, must implement `Clone`
/// * `S` - The hasher type, defaults to `RandomState`
#[derive(Debug, Clone, Default)]
pub struct HashMap<K, V, S = RandomState>(papaya::HashMap<K, V, S>)
where
	K: hash::Hash + cmp::Eq,
	V: Clone,
	S: hash::BuildHasher + Default;

impl<K, V, S> HashMap<K, V, S>
where
	K: hash::Hash + cmp::Eq,
	V: Clone,
	S: hash::BuildHasher + Default,
{
	/// Creates a new empty HashMap.
	pub fn new() -> Self {
		Self(Default::default())
	}

	/// Inserts a key-value pair into the map.
	///
	/// If the map already contains this key, the value is updated.
	pub fn insert(&self, key: K, value: V) {
		self.0.pin().insert(key, value);
	}

	/// Returns a clone of the value corresponding to the key.
	///
	/// Returns `None` if the key is not present in the map.
	pub fn get<Q>(&self, key: &Q) -> Option<V>
	where
		Q: Equivalent<K> + hash::Hash + ?Sized,
	{
		self.0.pin().get(key).cloned()
	}

	/// Returns `true` if the map contains a value for the specified key.
	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		Q: Equivalent<K> + hash::Hash + ?Sized,
	{
		self.0.pin().contains_key(key)
	}

	/// Returns a vector containing clones of all values in the map.
	pub fn values(&self) -> Vec<V> {
		self.0.pin().values().cloned().collect()
	}

	/// Returns a vector containing clones of all key-value pairs in the map.
	pub fn to_vec(&self) -> Vec<(K, V)>
	where
		K: Clone,
	{
		let map = self.0.pin();
		let mut vec = Vec::with_capacity(map.len());
		for (k, v) in map.iter() {
			vec.push((k.clone(), v.clone()));
		}
		vec
	}

	/// Removes a key from the map.
	///
	/// Traditionally `remove` would return the value but this would require cloning the value,
	/// which would be an unnecessary expense when you don't need the value. If you need the value,
	/// use `take` instead.
	pub fn remove<Q>(&self, key: &Q)
	where
		Q: Equivalent<K> + hash::Hash + ?Sized,
	{
		self.0.pin().remove(key);
	}

	/// Removes a key from the map and returns the value.
	pub fn take<Q>(&self, key: &Q) -> Option<V>
	where
		Q: Equivalent<K> + hash::Hash + ?Sized,
	{
		self.0.pin().remove(key).cloned()
	}

	/// Removes all key-value pairs from the map.
	pub fn clear(&self) {
		self.0.pin().clear();
	}

	/// Returns the number of elements in the map.
	pub fn len(&self) -> usize {
		self.0.pin().len()
	}

	/// Returns `true` if the map contains no elements.
	pub fn is_empty(&self) -> bool {
		self.0.pin().is_empty()
	}

	/// Retains only the elements specified by the predicate.
	///
	/// In other words, removes all pairs `(k, v)` such that `f(&k, &v)` returns `false`.
	pub fn retain<F>(&self, f: F)
	where
		F: FnMut(&K, &V) -> bool,
	{
		self.0.pin().retain(f);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_basic_operations() {
		let map: HashMap<String, i32> = HashMap::new();

		// Test insert and get
		map.insert("key1".to_string(), 1);
		map.insert("key2".to_string(), 2);

		assert_eq!(map.get(&"key1".to_string()), Some(1));
		assert_eq!(map.get(&"key2".to_string()), Some(2));
		assert_eq!(map.get(&"key3".to_string()), None);

		// Test contains_key
		assert!(map.contains_key(&"key1".to_string()));
		assert!(!map.contains_key(&"key3".to_string()));

		// Test len
		assert_eq!(map.len(), 2);
		assert!(!map.is_empty());

		// Test remove
		map.remove(&"key1".to_string());
		assert_eq!(map.get(&"key1".to_string()), None);
		assert_eq!(map.len(), 1);

		// Test clear
		map.clear();
		assert!(map.is_empty());
	}

	#[test]
	fn test_values_and_to_vec() {
		let map: HashMap<String, i32> = HashMap::new();
		map.insert("a".to_string(), 1);
		map.insert("b".to_string(), 2);
		map.insert("c".to_string(), 3);

		let values = map.values();
		assert_eq!(values.len(), 3);

		let vec = map.to_vec();
		assert_eq!(vec.len(), 3);
	}

	#[test]
	fn test_retain() {
		let map: HashMap<String, i32> = HashMap::new();
		map.insert("a".to_string(), 1);
		map.insert("b".to_string(), 2);
		map.insert("c".to_string(), 3);
		map.insert("d".to_string(), 4);

		// Keep only even values
		map.retain(|_, v| *v % 2 == 0);

		assert_eq!(map.len(), 2);
		assert!(map.contains_key(&"b".to_string()));
		assert!(map.contains_key(&"d".to_string()));
		assert!(!map.contains_key(&"a".to_string()));
		assert!(!map.contains_key(&"c".to_string()));
	}

	#[test]
	fn test_clone() {
		let map: HashMap<String, i32> = HashMap::new();
		map.insert("key".to_string(), 42);

		let cloned = map.clone();
		assert_eq!(cloned.get(&"key".to_string()), Some(42));

		// Modifications to clone don't affect original
		cloned.insert("key".to_string(), 100);
		assert_eq!(map.get(&"key".to_string()), Some(42));
		assert_eq!(cloned.get(&"key".to_string()), Some(100));
	}
}
