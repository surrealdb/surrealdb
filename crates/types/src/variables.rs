use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate as surrealdb_types;
use crate::{Object, SurrealValue, Value};

/// Represents a set of variables that can be used in a query.
#[derive(Clone, Debug, Default, Eq, PartialEq, SurrealValue, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Variables(BTreeMap<String, Value>);

impl Variables {
	/// Create a new empty variables map.
	pub fn new() -> Self {
		Self(BTreeMap::new())
	}

	/// Check if the variables map is empty.
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Get the number of variables in the map.
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Get an iterator over the variables in the map.
	pub fn iter(&self) -> std::collections::btree_map::Iter<'_, String, Value> {
		self.0.iter()
	}

	/// Get a variable from the map.
	pub fn get(&self, key: &str) -> Option<&Value> {
		self.0.get(key)
	}

	/// Insert a new variable into the map.
	pub fn insert(&mut self, key: impl Into<String>, value: impl SurrealValue) {
		self.0.insert(key.into(), value.into_value());
	}

	/// Remove a variable from the map.
	pub fn remove(&mut self, key: &str) {
		self.0.remove(key);
	}

	/// Extend the variables map with another variables map.
	pub fn extend(&mut self, other: Variables) {
		self.0.extend(other.0);
	}
}

impl IntoIterator for Variables {
	type Item = (String, Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl FromIterator<(String, Value)> for Variables {
	fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
		Self(iter.into_iter().collect())
	}
}

impl From<BTreeMap<String, Value>> for Variables {
	fn from(map: BTreeMap<String, Value>) -> Self {
		Self(map)
	}
}

impl From<Object> for Variables {
	fn from(obj: Object) -> Self {
		Self(obj.0)
	}
}

impl From<Variables> for Object {
	fn from(vars: Variables) -> Self {
		Object(vars.0)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_variables_serde() {
		let vars = Variables::new();
		let json = serde_json::to_string(&vars).unwrap();
		assert_eq!(json, "{}");

		let vars1 = serde_json::from_str::<Variables>("{\"name\":{\"String\":\"John\"}}").unwrap();
		assert_eq!(vars1.get("name"), Some(&Value::String("John".to_string())));

		let vars2 =
			Variables::from_iter(vec![("name".to_string(), "John".to_string().into_value())]);
		let json = serde_json::to_string(&vars2).unwrap();
		assert_eq!(json, "{\"name\":{\"String\":\"John\"}}");
	}
}
