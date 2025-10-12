use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::sql::ToSql;
use crate::{SurrealValue, Value};

/// Represents an array of values in SurrealDB
///
/// An array is an ordered collection of values that can contain elements of any type.
/// The underlying storage is a `Vec<Value>`.

#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Array(pub(crate) Vec<Value>);

impl Array {
	/// Create a new empty array
	pub fn new() -> Self {
		Array(Vec::new())
	}
	/// Create a new array with capacity
	pub fn with_capacity(len: usize) -> Self {
		Self(Vec::with_capacity(len))
	}
	/// Get the length of the array
	pub fn len(&self) -> usize {
		self.0.len()
	}
	/// Check if there array is empty
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Convert the array into a vector of values.
	pub fn into_vec(self) -> Vec<Value> {
		self.0
	}

	/// Create array from existing Vec<Value>
	pub fn from_values(values: Vec<Value>) -> Self {
		Self(values)
	}

	/// Get the inner Vec<Value>
	pub fn inner(&self) -> &Vec<Value> {
		&self.0
	}
}

impl<T: SurrealValue> From<Vec<T>> for Array {
	fn from(v: Vec<T>) -> Self {
		v.into_iter().map(T::into_value).collect()
	}
}

impl From<Array> for Vec<Value> {
	fn from(s: Array) -> Self {
		s.0
	}
}

impl<T: SurrealValue> FromIterator<T> for Array {
	fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
		Array(iter.into_iter().map(T::into_value).collect())
	}
}

impl Deref for Array {
	type Target = Vec<Value>;
	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl DerefMut for Array {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.0
	}
}

impl IntoIterator for Array {
	type Item = Value;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl ToSql for Array {
	fn fmt_sql(&self, f: &mut String) {
		f.push('[');
		for (i, v) in self.iter().enumerate() {
			v.fmt_sql(f);
			if i < self.len() - 1 {
				f.push_str(", ");
			}
		}
		f.push(']');
	}
}
