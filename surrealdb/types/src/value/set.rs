use std::collections::BTreeSet;
use std::ops::{Deref, DerefMut};

use serde::{Deserialize, Serialize};

use crate::Value;
use crate::sql::{SqlFormat, ToSql};

/// A set of unique values in SurrealDB
///
/// Sets are collections that maintain uniqueness and ordering of elements.
/// The underlying storage is a `BTreeSet<Value>` which provides automatic
/// deduplication and sorted iteration based on `Value`'s `Ord` implementation.
#[derive(Clone, Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct Set(pub(crate) BTreeSet<Value>);

impl Set {
	/// Create a new empty set
	pub fn new() -> Self {
		Self(BTreeSet::new())
	}

	/// Convert into the inner `BTreeSet<Value>`
	pub fn into_inner(self) -> BTreeSet<Value> {
		self.0
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

impl<'a> IntoIterator for &'a Set {
	type Item = &'a Value;
	type IntoIter = std::collections::btree_set::Iter<'a, Value>;

	fn into_iter(self) -> Self::IntoIter {
		self.0.iter()
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
	fn fmt_sql(&self, f: &mut String, fmt: SqlFormat) {
		if self.is_empty() {
			f.push_str("{,}");
			return;
		}

		// Format as Python-style set literal: `{,}`, `{val,}`, `{val, val, val}`
		f.push('{');
		let len = self.len();
		for (i, v) in self.iter().enumerate() {
			v.fmt_sql(f, fmt);
			if len == 1 {
				f.push(',');
			} else if i < len - 1 {
				f.push_str(", ");
			}
		}
		f.push('}');
	}
}
