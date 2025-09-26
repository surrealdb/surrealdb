use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::sql::expression::convert_public_value_to_internal;
use crate::types::PublicVariables;
use crate::val::{Object, Value};

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Variables(pub BTreeMap<String, Value>);

impl Variables {
	/// Create a new empty variables map.
	#[allow(dead_code)]
	pub fn new() -> Self {
		Self(BTreeMap::new())
	}

	/// Insert a new variable into the map.
	#[allow(dead_code)]
	pub fn insert(&mut self, key: String, value: Value) {
		self.0.insert(key, value);
	}
}

impl IntoIterator for Variables {
	type Item = (String, Value);
	type IntoIter = std::collections::btree_map::IntoIter<String, Value>;

	#[inline]
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_iter()
	}
}

impl FromIterator<(String, Value)> for Variables {
	fn from_iter<T: IntoIterator<Item = (String, Value)>>(iter: T) -> Self {
		Self(iter.into_iter().collect())
	}
}

impl From<Object> for Variables {
	fn from(obj: Object) -> Self {
		Self(obj.0)
	}
}

impl From<BTreeMap<String, Value>> for Variables {
	fn from(map: BTreeMap<String, Value>) -> Self {
		Self(map)
	}
}

impl From<PublicVariables> for Variables {
	fn from(vars: PublicVariables) -> Self {
		let mut map = BTreeMap::new();
		for (key, val) in vars {
			let internal_val = convert_public_value_to_internal(val);
			map.insert(key, internal_val);
		}
		Self(map)
	}
}
