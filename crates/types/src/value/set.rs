use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::Value;
use crate::sql::ToSql;

/// A set of unique values in SurrealDB
///
/// Sets are collections that maintain uniqueness and ordering of elements.
/// The underlying storage is a `BTreeSet<Value>` which provides automatic
/// deduplication and sorted iteration based on `Value`'s `Ord` implementation.
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Set(pub(crate) BTreeSet<Value>);

impl Set {
	/// Create a new empty set
	pub fn new() -> Self {
		Self(BTreeSet::new())
	}

	/// Get the number of elements in the set
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Check if the set is empty
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Get an iterator over the values in the set
	pub fn iter(&self) -> impl Iterator<Item = &Value> {
		self.0.iter()
	}

	/// Insert a value into the set
	///
	/// Returns `true` if the value was newly inserted, `false` if it was already present
	pub fn insert(&mut self, value: Value) -> bool {
		self.0.insert(value)
	}

	/// Remove a value from the set
	///
	/// Returns `true` if the value was present and removed, `false` if it wasn't present
	pub fn remove(&mut self, value: &Value) -> bool {
		self.0.remove(value)
	}

	/// Check if the set contains a value
	pub fn contains(&self, value: &Value) -> bool {
		self.0.contains(value)
	}

	/// Remove all values from the set
	pub fn clear(&mut self) {
		self.0.clear()
	}
}

impl From<Vec<Value>> for Set {
	fn from(vec: Vec<Value>) -> Self {
		Self(vec.into_iter().collect())
	}
}

impl From<BTreeSet<Value>> for Set {
	fn from(set: BTreeSet<Value>) -> Self {
		Self(set)
	}
}

impl From<Set> for Vec<Value> {
	fn from(set: Set) -> Self {
		set.0.into_iter().collect()
	}
}

impl IntoIterator for Set {
	type Item = Value;
	type IntoIter = std::collections::btree_set::IntoIter<Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Deref for Set {
	type Target = BTreeSet<Value>;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Set {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl ToSql for Set {
	fn fmt_sql(&self, f: &mut String) {
		// Format as Python-style set literal: {val, val, val}
		f.push('{');
		for (i, v) in self.iter().enumerate() {
			v.fmt_sql(f);
			if i < self.len() - 1 {
				f.push_str(", ");
			}
		}
		f.push('}');
	}
}
