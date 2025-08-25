use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::{SurrealValue, Value};

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

impl Deref for Object {
	type Target = BTreeMap<String, Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Object {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl std::ops::Add for Object {
	type Output = Self;

	fn add(self, rhs: Self) -> Self::Output {
		let mut lhs = self;
		lhs.0.extend(rhs.0);
		lhs
	}
}
