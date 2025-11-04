use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::utils::escape::EscapeKey;
use crate::{SurrealValue, Value, write_sql};

/// Represents an object with key-value pairs in SurrealDB
///
/// An object is a collection of key-value pairs where keys are strings and values can be of any
/// type. The underlying storage is a `BTreeMap<String, Value>` which maintains sorted keys.

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Object(pub(crate) BTreeMap<String, Value>);

impl Object {
	/// Create a new empty object
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	/// Get an iterator over the key-value pairs in the object
	pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
		self.0.iter()
	}

	/// Get the value of a key
	pub fn get(&self, key: &str) -> Option<&Value> {
		self.0.get(key)
	}

	/// Get a mutable reference to the value of a key
	pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
		self.0.get_mut(key)
	}

	/// Get an iterator over the keys in the object
	pub fn keys(&self) -> std::collections::btree_map::Keys<'_, String, Value> {
		self.0.keys()
	}

	/// Insert a key-value pair into the object
	pub fn insert(&mut self, key: impl Into<String>, value: impl SurrealValue) -> Option<Value> {
		self.0.insert(key.into(), value.into_value())
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

impl ToSql for Object {
	fn fmt_sql(&self, f: &mut String) {
		if self.is_empty() {
			return f.push_str("{  }");
		}

		f.push_str("{ ");

		for (i, (k, v)) in self.iter().enumerate() {
			write_sql!(f, "{}: ", EscapeKey(k));
			v.fmt_sql(f);

			if i < self.len() - 1 {
				f.push_str(", ");
			}
		}

		f.push_str(" }")
	}
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
