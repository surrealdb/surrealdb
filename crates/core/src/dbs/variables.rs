use crate::expr::{value::Value, Thing};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use surrealdb_protocol::proto::rpc::v1 as rpc_proto;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Variables(pub BTreeMap<String, Value>);

impl Variables {
	/// Create a new empty variables map.
	#[inline]
	pub fn new() -> Self {
		Self(BTreeMap::new())
	}

	/// Insert a new variable into the map.
	#[inline]
	pub fn insert(&mut self, key: String, value: Value) {
		self.0.insert(key, value);
	}

	/// Remove a variable from the map.
	#[inline]
	pub fn remove(&mut self, key: &str) {
		self.0.remove(key);
	}

	#[inline]
	pub fn append(&mut self, other: &mut Variables) {
		self.0.append(&mut other.0);
	}

	/// Extend the variables map with another variables map.
	#[inline]
	pub fn extend(&mut self, other: Variables) {
		self.0.extend(other.0);
	}

	/// Check if the variables map is empty.
	#[inline]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Get the number of variables in the map.
	#[inline]
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Get an iterator over the variables in the map.
	#[inline]
	pub fn iter(&self) -> std::collections::btree_map::Iter<String, Value> {
		self.0.iter()
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

impl TryFrom<rpc_proto::Variables> for Variables {
	type Error = anyhow::Error;

	fn try_from(value: rpc_proto::Variables) -> Result<Self, Self::Error> {
		let mut vars = Self::new();
		for (k, v) in value.variables.into_iter() {
			vars.insert(k, v.try_into()?);
		}
		Ok(vars)
	}
}

impl TryFrom<Variables> for rpc_proto::Variables {
	type Error = anyhow::Error;

	fn try_from(value: Variables) -> Result<Self, Self::Error> {
		let mut vars = Self {
			variables: BTreeMap::new(),
		};
		for (k, v) in value.0.into_iter() {
			vars.variables.insert(k, v.try_into()?);
		}
		Ok(vars)
	}
}

impl TryFrom<(&str, &str)> for Variables {
	type Error = anyhow::Error;

	fn try_from(value: (&str, &str)) -> Result<Self, Self::Error> {
		let mut vars = Self::new();
		vars.insert(value.0.to_string(), value.1.into());
		Ok(vars)
	}
}

impl TryFrom<(&str, Thing)> for Variables {
	type Error = anyhow::Error;

	fn try_from(value: (&str, Thing)) -> Result<Self, Self::Error> {
		let mut vars = Self::new();
		vars.insert(value.0.to_string(), value.1.into());
		Ok(vars)
	}
}
