use crate::sql::{
	SqlValue,
	fmt::{Fmt, Pretty, pretty_indent},
};
use anyhow::Result;
use revision::revisioned;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter, Write};
use std::ops;
use std::ops::Deref;
use std::ops::DerefMut;

#[revisioned(revision = 1)]
#[derive(Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize, Hash)]
#[serde(rename = "$surrealdb::private::sql::Array")]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[non_exhaustive]
pub struct Array(pub Vec<SqlValue>);

impl From<SqlValue> for Array {
	fn from(v: SqlValue) -> Self {
		vec![v].into()
	}
}

impl<T> From<Vec<T>> for Array
where
	SqlValue: From<T>,
{
	fn from(v: Vec<T>) -> Self {
		v.into_iter().map(SqlValue::from).collect()
	}
}

impl From<Array> for Vec<SqlValue> {
	fn from(s: Array) -> Self {
		s.0
	}
}

impl FromIterator<SqlValue> for Array {
	fn from_iter<I: IntoIterator<Item = SqlValue>>(iter: I) -> Self {
		Array(iter.into_iter().collect())
	}
}

impl From<Array> for crate::expr::Array {
	fn from(s: Array) -> Self {
		Self(s.0.into_iter().map(Into::into).collect())
	}
}

impl From<crate::expr::Array> for Array {
	fn from(s: crate::expr::Array) -> Self {
		Self(s.0.into_iter().map(Into::into).collect())
	}
}

impl Deref for Array {
	type Target = Vec<SqlValue>;
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
	type Item = SqlValue;
	type IntoIter = std::vec::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Array {
	// Create a new empty array
	pub fn new() -> Self {
		Self::default()
	}
	// Create a new array with capacity
	pub fn with_capacity(len: usize) -> Self {
		Self(Vec::with_capacity(len))
	}
	// Get the length of the array
	pub fn len(&self) -> usize {
		self.0.len()
	}
	// Check if there array is empty
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}
}

impl Array {
	/// Checks whether all array values are static values
	pub(crate) fn is_static(&self) -> bool {
		self.iter().all(SqlValue::is_static)
	}

	/// Validate that an Array contains only computed Values
	pub fn validate_computed(&self) -> Result<()> {
		self.iter().try_for_each(|v| v.validate_computed())
	}
}

impl Display for Array {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		let mut f = Pretty::from(f);
		f.write_char('[')?;
		if !self.is_empty() {
			let indent = pretty_indent();
			write!(f, "{}", Fmt::pretty_comma_separated(self.as_slice()))?;
			drop(indent);
		}
		f.write_char(']')
	}
}

// ------------------------------

impl ops::Add<SqlValue> for Array {
	type Output = Self;
	fn add(mut self, other: SqlValue) -> Self {
		self.0.push(other);
		self
	}
}

impl ops::Add for Array {
	type Output = Self;
	fn add(mut self, mut other: Self) -> Self {
		self.0.append(&mut other.0);
		self
	}
}

// ------------------------------

impl ops::Sub<SqlValue> for Array {
	type Output = Self;
	fn sub(mut self, other: SqlValue) -> Self {
		if let Some(p) = self.0.iter().position(|x| *x == other) {
			self.0.remove(p);
		}
		self
	}
}

impl ops::Sub for Array {
	type Output = Self;
	fn sub(mut self, other: Self) -> Self {
		for v in other.0 {
			if let Some(p) = self.0.iter().position(|x| *x == v) {
				self.0.remove(p);
			}
		}
		self
	}
}

// ------------------------------

pub(crate) trait Uniq<T> {
	fn uniq(self) -> T;
}

impl Uniq<Array> for Array {
	fn uniq(mut self) -> Array {
		#[expect(clippy::mutable_key_type)]
		let mut set: HashSet<&SqlValue> = HashSet::new();
		let mut to_remove: Vec<usize> = Vec::new();
		for (i, item) in self.iter().enumerate() {
			if !set.insert(item) {
				to_remove.push(i);
			}
		}
		for i in to_remove.iter().rev() {
			self.remove(*i);
		}
		self
	}
}
