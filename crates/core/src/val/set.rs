use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter, Write};
use std::ops::{Deref, DerefMut};

use revision::revisioned;
use serde::{Deserialize, Serialize};
use storekey::{BorrowDecode, Encode};

use crate::expr::Expr;
use crate::val::{IndexFormat, Value};

/// Internal Set type that stores unique values
///
/// Sets use BTreeSet internally to maintain uniqueness and sorted order.
/// The storage traits (Encode/Decode) are derived and work with BTreeSet.
#[revisioned(revision = 1)]
#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Ord,
	PartialEq,
	PartialOrd,
	Serialize,
	Deserialize,
	Hash,
	Encode,
	BorrowDecode,
)]
#[serde(rename = "$surrealdb::private::Set")]
#[storekey(format = "()")]
#[storekey(format = "IndexFormat")]
pub(crate) struct Set(pub(crate) BTreeSet<Value>);

impl Set {
	/// Create a new empty set
	#[allow(dead_code)]
	pub fn new() -> Self {
		Set(BTreeSet::new())
	}

	/// Get the number of elements in the set
	#[allow(dead_code)]
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
	/// Returns true if the value was newly inserted
	#[allow(dead_code)]
	pub fn insert(&mut self, value: Value) -> bool {
		self.0.insert(value)
	}

	/// Check if the set contains a value
	pub fn contains(&self, value: &Value) -> bool {
		self.0.contains(value)
	}

	/// Remove a value from the set
	/// Returns true if the value was present
	#[allow(dead_code)]
	pub fn remove(&mut self, value: &Value) -> bool {
		self.0.remove(value)
	}

	/// Convert into a literal expression
	pub fn into_literal(self) -> Vec<Expr> {
		self.0.into_iter().map(Value::into_literal).collect()
	}

	/// Return the union of this set with another (A ∪ B)
	pub fn union(self, other: Set) -> Set {
		Set(self.0.union(&other.0).cloned().collect())
	}

	/// Return the intersection of this set with another (A ∩ B)
	pub fn intersection(&self, other: &Set) -> Set {
		Set(self.0.intersection(&other.0).cloned().collect())
	}

	/// Return the symmetric difference (A △ B) - elements in either but not both
	pub fn symmetric_difference(self, other: Set) -> Set {
		Set(self.0.symmetric_difference(&other.0).cloned().collect())
	}

	/// Return the relative complement (A \ B) - elements in self but not in other
	pub fn complement(self, other: Set) -> Set {
		Set(self.0.difference(&other.0).cloned().collect())
	}
}

impl<T> From<Vec<T>> for Set
where
	Value: From<T>,
{
	fn from(v: Vec<T>) -> Self {
		Set(v.into_iter().map(Value::from).collect())
	}
}

impl From<BTreeSet<Value>> for Set {
	fn from(set: BTreeSet<Value>) -> Self {
		Set(set)
	}
}

impl From<Set> for Vec<Value> {
	fn from(s: Set) -> Self {
		s.0.into_iter().collect()
	}
}

impl TryFrom<Set> for surrealdb_types::Set {
	type Error = anyhow::Error;

	fn try_from(s: Set) -> Result<Self, Self::Error> {
		Ok(surrealdb_types::Set::from(
			s.0.into_iter().map(surrealdb_types::Value::try_from).collect::<Result<Vec<_>, _>>()?,
		))
	}
}

impl From<surrealdb_types::Set> for Set {
	fn from(s: surrealdb_types::Set) -> Self {
		Set(s.into_iter().map(Value::from).collect())
	}
}

impl FromIterator<Value> for Set {
	fn from_iter<I: IntoIterator<Item = Value>>(iter: I) -> Self {
		Set(iter.into_iter().collect())
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

impl IntoIterator for Set {
	type Item = Value;
	type IntoIter = std::collections::btree_set::IntoIter<Self::Item>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl Display for Set {
	fn fmt(&self, f: &mut Formatter) -> fmt::Result {
		// Format as Python-style set literal: {val, val, val}
		f.write_char('{')?;
		for (i, v) in self.iter().enumerate() {
			if i > 0 {
				f.write_str(", ")?;
			}
			Display::fmt(v, f)?;
		}
		f.write_char('}')
	}
}
