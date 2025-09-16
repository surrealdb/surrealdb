use std::collections::BTreeMap;

use crate::{Object, Value};

/// Represents a set of variables that can be used in a query.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Variables(BTreeMap<String, Value>);


impl Variables {
	/// Create a new empty variables map.
	pub fn new() -> Self {
		Self(BTreeMap::new())
	}

	/// Get a variable from the map.
	pub fn get(&self, key: &str) -> Option<&Value> {
		self.0.get(key)
	}

	/// Insert a new variable into the map.
	pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) {
		self.0.insert(key.into(), value.into());
	}

	/// Remove a variable from the map.
	pub fn remove(&mut self, key: &str) {
		self.0.remove(key);
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