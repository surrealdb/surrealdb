use std::collections::{BTreeMap, HashMap};
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::sql::{SqlFormat, ToSql};
use crate::{SurrealValue, Value};

/// Represents an object with key-value pairs in SurrealDB
///
/// An object is a collection of key-value pairs where keys are strings and values can be of any
/// type. The underlying storage is a `BTreeMap<String, Value>` which maintains sorted keys.

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Object(pub(crate) BTreeMap<String, Value>);

impl Object {
	/// Create a new empty object
	pub fn new() -> Self {
		Object(BTreeMap::new())
	}

	/// Insert a key-value pair into the object
	pub fn insert(&mut self, key: impl Into<String>, value: impl SurrealValue) -> Option<Value> {
		self.0.insert(key.into(), value.into_value())
	}

	/// Convert into the inner BTreeMap<String, Value>
	pub fn into_inner(self) -> BTreeMap<String, Value> {
		self.0
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

impl ToSql for Object {
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		use crate::sql::fmt_sql_key_value;

		if self.is_empty() {
			return f.push_str("{  }");
		}

		if fmt.is_pretty() {
			f.push('{');
		} else {
			f.push_str("{ ");
		}

		if !self.is_empty() {
			let inner_fmt = fmt.increment();
			fmt_sql_key_value(self.iter(), f, inner_fmt);
		}

		if fmt.is_pretty() {
			f.push('}');
		} else {
			f.push_str(" }");
		}
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

impl<'a> IntoIterator for &'a Object {
	type Item = (&'a String, &'a Value);
	type IntoIter = std::collections::btree_map::Iter<'a, String, Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.iter()
	}
}

impl<'a> IntoIterator for &'a mut Object {
	type Item = (&'a String, &'a mut Value);
	type IntoIter = std::collections::btree_map::IterMut<'a, String, Value>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.iter_mut()
	}
}
