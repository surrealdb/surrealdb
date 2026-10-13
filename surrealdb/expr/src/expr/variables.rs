use std::collections::BTreeMap;

use surrealdb_strand::Strand;

use crate::types::PublicVariables;
use crate::val::convert_public::convert_public_value_to_internal;
use crate::val::{Object, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Hash)]
#[repr(transparent)]
pub struct Variables(pub BTreeMap<Strand, Value>);

impl Variables {
	/// Create a new empty variables map.
	#[allow(dead_code)]
	pub fn new() -> Self {
		Self(BTreeMap::new())
	}

	/// Insert a new variable into the map.
	#[allow(dead_code)]
	pub fn insert(&mut self, key: impl Into<Strand>, value: Value) {
		self.0.insert(key.into(), value);
	}
}

impl IntoIterator for Variables {
	type Item = (Strand, Value);
	type IntoIter = std::collections::btree_map::IntoIter<Strand, Value>;

	#[inline]
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl FromIterator<(Strand, Value)> for Variables {
	fn from_iter<T: IntoIterator<Item = (Strand, Value)>>(iter: T) -> Self {
		Self(iter.into_iter().collect())
	}
}

impl FromIterator<(String, Value)> for Variables {
	fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
		Self(iter.into_iter().map(|(k, v)| (k.into(), v)).collect())
	}
}

impl From<Object> for Variables {
	fn from(obj: Object) -> Self {
		Self(obj.0.into_iter().collect())
	}
}

impl From<BTreeMap<String, Value>> for Variables {
	fn from(map: BTreeMap<String, Value>) -> Self {
		Self(map.into_iter().map(|(k, v)| (k.into(), v)).collect())
	}
}

impl From<BTreeMap<Strand, Value>> for Variables {
	fn from(map: BTreeMap<Strand, Value>) -> Self {
		Self(map)
	}
}

impl From<PublicVariables> for Variables {
	fn from(vars: PublicVariables) -> Self {
		let mut map = BTreeMap::new();
		for (key, val) in vars {
			let internal_val = convert_public_value_to_internal(val);
			map.insert(key.into(), internal_val);
		}
		Self(map)
	}
}
