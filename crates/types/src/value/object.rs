use std::{collections::{BTreeMap, HashMap}};

use revision::revisioned;
use serde::{Deserialize, Serialize};

use crate::{SurrealValue, Value};

/// Represents an object with key-value pairs in SurrealDB
///
/// An object is a collection of key-value pairs where keys are strings and values can be of any
/// type. The underlying storage is a `BTreeMap<String, Value>` which maintains sorted keys.
#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Object(pub(crate) BTreeMap<String, Value>);

impl Object {
	/// Create a new empty object
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	// pub fn clear(&mut self) {
	// 	self.0.clear()
	// }

	// pub fn get<Q>(&self, key: &Q) -> Option<&Value>
	// where
	// 	String: Borrow<Q>,
	// 	Q: Ord + ?Sized,
	// {
	// 	self.0.get(key).map(Value::from_inner_ref)
	// }

	// pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut Value>
	// where
	// 	String: Borrow<Q>,
	// 	Q: Ord + ?Sized,
	// {
	// 	self.0.get_mut(key).map(Value::from_inner_mut)
	// }

	// pub fn contains_key<Q>(&self, key: &Q) -> bool
	// where
	// 	String: Borrow<Q>,
	// 	Q: ?Sized + Ord,
	// {
	// 	self.0.contains_key(key)
	// }

	// pub fn remove<Q>(&mut self, key: &Q) -> Option<Value>
	// where
	// 	String: Borrow<Q>,
	// 	Q: ?Sized + Ord,
	// {
	// 	self.0.remove(key).map(Value::from_inner)
	// }

	// pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(String, Value)>
	// where
	// 	String: Borrow<Q>,
	// 	Q: ?Sized + Ord,
	// {
	// 	self.0.remove_entry(key).map(|(a, b)| (a, Value::from_inner(b)))
	// }

	/// Get an iterator over the key-value pairs in the object
	pub fn iter(&self) -> std::collections::btree_map::Iter<String, Value> {
		self.0.iter()
	}

	// pub fn iter_mut(&mut self) -> IterMut<'_> {
	// 	IterMut {
	// 		iter: self.0.iter_mut(),
	// 	}
	// }

	// pub fn len(&self) -> usize {
	// 	self.0.len()
	// }

	// pub fn is_empty(&self) -> bool {
	// 	self.0.is_empty()
	// }

	// pub fn insert<V>(&mut self, key: String, value: V) -> Option<Value>
	// where
	// 	V: Into<Value>,
	// {
	// 	self.0.insert(key, value.into().into_inner()).map(Value::from_inner)
	// }
}

impl<T: SurrealValue> From<BTreeMap<&str, T>> for Object {
	fn from(v: BTreeMap<&str, T>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val.into_value())).collect())
	}
}

impl<T: SurrealValue> From<BTreeMap<String, T>> for Object {
	fn from(v: BTreeMap<String, T>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key, val.into_value())).collect())
	}
}

impl<T: SurrealValue> FromIterator<(String, T)> for Object {
	fn from_iter<I: IntoIterator<Item = (String, T)>>(iter: I) -> Self {
		Self(BTreeMap::from_iter(iter.into_iter().map(|(k, v)| (k, v.into_value()))))
	}
}

impl<T: SurrealValue> From<HashMap<&str, T>> for Object {
	fn from(v: HashMap<&str, T>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key.to_string(), val.into_value())).collect())
	}
}

impl<T: SurrealValue> From<HashMap<String, T>> for Object {
	fn from(v: HashMap<String, T>) -> Self {
		Self(v.into_iter().map(|(key, val)| (key, val.into_value())).collect())
	}
}

impl IntoIterator for Object {
	type Item = (String, Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Object {
	/// Get the value of a key
	pub fn get(&self, key: &str) -> Option<&Value> {
		self.0.get(key)
	}

	/// Get a mutable reference to the value of a key
	pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
		self.0.get_mut(key)
	}

	/// Insert a key-value pair into the object
	pub fn insert(&mut self, key: String, value: Value) -> Option<Value> {
		self.0.insert(key, value)
	}

	/// Remove a key-value pair from the object
	pub fn remove(&mut self, key: &str) -> Option<Value> {
		self.0.remove(key)
	}

	/// Extend the object with the contents of another object
	pub fn extend(&mut self, other: Object) {
		self.0.extend(other.0);
	}

	/// Clear the object
	pub fn clear(&mut self) {
		self.0.clear();
	}

	/// Get the number of key-value pairs in the object
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Check if the object is empty
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Create new object from BTreeMap<String, Value>
	pub fn from_map(map: BTreeMap<String, Value>) -> Self {
		Self(map)
	}
	
	/// Get the inner BTreeMap<String, Value>
	pub fn inner(&self) -> &BTreeMap<String, Value> {
		&self.0
	}
}
