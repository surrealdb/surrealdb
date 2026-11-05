use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::iter::FusedIterator;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use surrealdb_collections::{VecMap, VecMapIterMut};

use crate::sql::{SqlFormat, ToSql};
use crate::{SurrealValue, Value};

/// Represents an object with key-value pairs in SurrealDB
///
/// An object is a collection of key-value pairs where keys are strings and values can be of any
/// type. Its entries are held in a [`VecMap`], a vector in ascending key order with each key once:
/// a lookup is a binary search, iteration visits the entries in key order, and an object built
/// from entries already in key order (as one decoded from the wire is) is built without a search
/// per key.
///
/// Equality, ordering and hashing are those of the entries in key order, as they are for a
/// `BTreeMap<String, Value>` holding the same entries.
#[derive(Clone, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Object(pub(crate) VecMap<String, Value>);

impl Object {
	/// Create a new empty object
	pub fn new() -> Self {
		Object(VecMap::new())
	}

	/// Create a new empty object with room for `capacity` entries
	pub fn with_capacity(capacity: usize) -> Self {
		Object(VecMap::with_capacity(capacity))
	}

	/// An object of `entries`, which must be in strictly ascending key order.
	#[cfg(feature = "cbor")]
	pub(crate) fn from_sorted(entries: Vec<(String, Value)>) -> Self {
		Object(VecMap::from_sorted_vec_unchecked(entries))
	}

	/// The number of entries
	pub fn len(&self) -> usize {
		self.0.len()
	}

	/// Whether the object has no entries
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	/// Insert a key-value pair into the object, returning the value the key held before
	pub fn insert(&mut self, key: impl Into<String>, value: impl SurrealValue) -> Option<Value> {
		self.0.insert(key.into(), value.into_value())
	}

	/// The value of `key`
	pub fn get<Q>(&self, key: &Q) -> Option<&Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get(key)
	}

	/// The value of `key`, mutably
	pub fn get_mut<Q>(&mut self, key: &Q) -> Option<&mut Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get_mut(key)
	}

	/// The entry of `key`
	pub fn get_key_value<Q>(&self, key: &Q) -> Option<(&String, &Value)>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.get_key_value(key)
	}

	/// Whether the object holds `key`
	pub fn contains_key<Q>(&self, key: &Q) -> bool
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.contains_key(key)
	}

	/// Remove `key`, returning its value
	pub fn remove<Q>(&mut self, key: &Q) -> Option<Value>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.remove(key)
	}

	/// Remove `key`, returning its entry
	pub fn remove_entry<Q>(&mut self, key: &Q) -> Option<(String, Value)>
	where
		String: Borrow<Q>,
		Q: Ord + ?Sized,
	{
		self.0.remove_entry(key)
	}

	/// Remove every entry
	pub fn clear(&mut self) {
		self.0.clear();
	}

	/// Keep only the entries `f` accepts
	pub fn retain(&mut self, f: impl FnMut(&String, &mut Value) -> bool) {
		self.0.retain(f);
	}

	/// The first entry, in key order
	pub fn first_key_value(&self) -> Option<(&String, &Value)> {
		self.0.first_key_value()
	}

	/// The last entry, in key order
	pub fn last_key_value(&self) -> Option<(&String, &Value)> {
		self.0.last_key_value()
	}

	/// Remove and return the first entry, in key order
	pub fn pop_first(&mut self) -> Option<(String, Value)> {
		self.0.pop_first()
	}

	/// Remove and return the last entry, in key order
	pub fn pop_last(&mut self) -> Option<(String, Value)> {
		self.0.pop_last()
	}

	/// The entries, in key order
	pub fn iter(&self) -> Iter<'_> {
		Iter(self.0.as_slice().iter())
	}

	/// The entries with their values mutable, in key order
	pub fn iter_mut(&mut self) -> IterMut<'_> {
		IterMut(self.0.iter_mut())
	}

	/// The keys, in order
	pub fn keys(&self) -> Keys<'_> {
		Keys(self.0.as_slice().iter())
	}

	/// The values, in key order
	pub fn values(&self) -> Values<'_> {
		Values(self.0.as_slice().iter())
	}

	/// The values, mutable, in key order
	pub fn values_mut(&mut self) -> ValuesMut<'_> {
		ValuesMut(self.0.iter_mut())
	}

	/// The keys, in order, taking the object
	pub fn into_keys(self) -> impl DoubleEndedIterator<Item = String> + ExactSizeIterator {
		self.0.into_keys()
	}

	/// The values, in key order, taking the object
	pub fn into_values(self) -> impl DoubleEndedIterator<Item = Value> + ExactSizeIterator {
		self.0.into_values()
	}

	/// The entry of `key`, to read or change in place
	pub fn entry(&mut self, key: impl Into<String>) -> Entry<'_> {
		match self.0.entry(key.into()) {
			surrealdb_collections::Entry::Occupied(entry) => Entry::Occupied(OccupiedEntry(entry)),
			surrealdb_collections::Entry::Vacant(entry) => Entry::Vacant(VacantEntry(entry)),
		}
	}

	/// Move every entry of `other` into this object; an entry of `other` replaces one with the
	/// same key here.
	pub fn append(&mut self, other: &mut Object) {
		self.0.append(&mut other.0);
	}

	/// The entries as a `BTreeMap`
	pub fn into_inner(self) -> BTreeMap<String, Value> {
		self.0.into_iter().collect()
	}
}

impl fmt::Debug for Object {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		/// The entries as a map, as a `BTreeMap` prints them.
		struct Entries<'a>(&'a Object);
		impl fmt::Debug for Entries<'_> {
			fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
				f.debug_map().entries(self.0.iter()).finish()
			}
		}
		f.debug_tuple("Object").field(&Entries(self)).finish()
	}
}

/// Serialised as a map, in key order.
impl Serialize for Object {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		self.0.serialize(serializer)
	}
}

/// Deserialised from a map with keys in any order; of entries with the same key, the last is
/// kept, as inserting them in turn keeps it.
impl<'de> Deserialize<'de> for Object {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
		VecMap::deserialize(deserializer).map(Object)
	}
}

#[cfg(feature = "arbitrary")]
impl<'a> arbitrary::Arbitrary<'a> for Object {
	fn arbitrary(u: &mut arbitrary::Unstructured<'a>) -> arbitrary::Result<Self> {
		VecMap::arbitrary(u).map(Object)
	}
}

impl std::ops::Index<&str> for Object {
	type Output = Value;

	/// The value of `key`; panics where the object does not hold it, as a `BTreeMap` does.
	fn index(&self, key: &str) -> &Value {
		self.get(key).expect("no entry found for key")
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

impl From<Object> for BTreeMap<String, Value> {
	fn from(v: Object) -> Self {
		v.into_inner()
	}
}

impl<T: SurrealValue> FromIterator<(String, T)> for Object {
	fn from_iter<I: IntoIterator<Item = (String, T)>>(iter: I) -> Self {
		Self(iter.into_iter().map(|(k, v)| (k, v.into_value())).collect())
	}
}

impl<T: SurrealValue> Extend<(String, T)> for Object {
	fn extend<I: IntoIterator<Item = (String, T)>>(&mut self, iter: I) {
		self.0.extend(iter.into_iter().map(|(k, v)| (k, v.into_value())));
	}
}

impl<T: SurrealValue> From<HashMap<&str, T>> for Object {
	fn from(v: HashMap<&str, T>) -> Self {
		v.into_iter().map(|(key, val)| (key.to_string(), val)).collect()
	}
}

impl<T: SurrealValue> From<HashMap<String, T>> for Object {
	fn from(v: HashMap<String, T>) -> Self {
		v.into_iter().collect()
	}
}

impl IntoIterator for Object {
	type Item = (String, Value);
	type IntoIter = std::vec::IntoIter<(String, Value)>;
	fn into_iter(self) -> Self::IntoIter {
		self.0.into_vec().into_iter()
	}
}

impl<'a> IntoIterator for &'a Object {
	type Item = (&'a String, &'a Value);
	type IntoIter = Iter<'a>;
	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

impl<'a> IntoIterator for &'a mut Object {
	type Item = (&'a String, &'a mut Value);
	type IntoIter = IterMut<'a>;
	fn into_iter(self) -> Self::IntoIter {
		self.iter_mut()
	}
}

/// Defines an iterator over an object's entries that maps each through `$map`.
macro_rules! entry_iter {
	($(#[$doc:meta])* $name:ident<$a:lifetime>, $inner:ty, $item:ty, $map:expr) => {
		$(#[$doc])*
		#[derive(Debug)]
		pub struct $name<$a>($inner);

		impl<$a> Iterator for $name<$a> {
			type Item = $item;
			fn next(&mut self) -> Option<Self::Item> {
				self.0.next().map($map)
			}
			fn size_hint(&self) -> (usize, Option<usize>) {
				self.0.size_hint()
			}
		}

		impl<$a> DoubleEndedIterator for $name<$a> {
			fn next_back(&mut self) -> Option<Self::Item> {
				self.0.next_back().map($map)
			}
		}

		impl<$a> ExactSizeIterator for $name<$a> {}

		impl<$a> FusedIterator for $name<$a> {}
	};
}

entry_iter!(
	/// An object's entries, in key order ([`Object::iter`]).
	Iter<'a>,
	std::slice::Iter<'a, (String, Value)>,
	(&'a String, &'a Value),
	|(k, v): &'a (String, Value)| (k, v)
);
entry_iter!(
	/// An object's entries with their values mutable, in key order ([`Object::iter_mut`]).
	IterMut<'a>,
	VecMapIterMut<'a, String, Value>,
	(&'a String, &'a mut Value),
	|entry: (&'a String, &'a mut Value)| entry
);
entry_iter!(
	/// An object's keys, in order ([`Object::keys`]).
	Keys<'a>,
	std::slice::Iter<'a, (String, Value)>,
	&'a String,
	|(k, _): &'a (String, Value)| k
);
entry_iter!(
	/// An object's values, in key order ([`Object::values`]).
	Values<'a>,
	std::slice::Iter<'a, (String, Value)>,
	&'a Value,
	|(_, v): &'a (String, Value)| v
);
entry_iter!(
	/// An object's values, mutable, in key order ([`Object::values_mut`]).
	ValuesMut<'a>,
	VecMapIterMut<'a, String, Value>,
	&'a mut Value,
	|(_, v): (&'a String, &'a mut Value)| v
);

/// An object's entry for one key ([`Object::entry`]).
pub enum Entry<'a> {
	/// The object holds the key.
	Occupied(OccupiedEntry<'a>),
	/// The object does not hold the key.
	Vacant(VacantEntry<'a>),
}

/// An entry the object holds.
pub struct OccupiedEntry<'a>(surrealdb_collections::OccupiedEntry<'a, String, Value>);

/// An entry the object does not hold, and where it would go.
pub struct VacantEntry<'a>(surrealdb_collections::VacantEntry<'a, String, Value>);

impl<'a> Entry<'a> {
	/// The entry's value, inserting `default` where the object does not hold the key.
	pub fn or_insert(self, default: Value) -> &'a mut Value {
		self.or_insert_with(|| default)
	}

	/// The entry's value, inserting what `default` makes where the object does not hold the key.
	pub fn or_insert_with(self, default: impl FnOnce() -> Value) -> &'a mut Value {
		match self {
			Entry::Occupied(entry) => entry.into_mut(),
			Entry::Vacant(entry) => entry.insert(default()),
		}
	}

	/// The entry's value, inserting the default value where the object does not hold the key.
	pub fn or_default(self) -> &'a mut Value {
		self.or_insert_with(Value::default)
	}

	/// The entry, with `f` run on its value where the object holds the key.
	pub fn and_modify(mut self, f: impl FnOnce(&mut Value)) -> Self {
		if let Entry::Occupied(entry) = &mut self {
			f(entry.get_mut());
		}
		self
	}

	/// The entry's key.
	pub fn key(&self) -> &String {
		match self {
			Entry::Occupied(entry) => entry.key(),
			Entry::Vacant(entry) => entry.key(),
		}
	}
}

impl<'a> OccupiedEntry<'a> {
	/// The entry's key.
	pub fn key(&self) -> &String {
		self.0.key()
	}

	/// The entry's value.
	pub fn get(&self) -> &Value {
		self.0.get()
	}

	/// The entry's value, mutably.
	pub fn get_mut(&mut self) -> &mut Value {
		self.0.get_mut()
	}

	/// The entry's value, for as long as the object is borrowed.
	pub fn into_mut(self) -> &'a mut Value {
		self.0.into_mut()
	}

	/// Replace the entry's value, returning the one it held.
	pub fn insert(&mut self, value: Value) -> Value {
		self.0.insert(value)
	}

	/// Remove the entry, returning its value.
	pub fn remove(self) -> Value {
		self.0.remove()
	}

	/// Remove the entry, returning it.
	pub fn remove_entry(self) -> (String, Value) {
		self.0.remove_entry()
	}
}

impl<'a> VacantEntry<'a> {
	/// The key the entry would hold.
	pub fn key(&self) -> &String {
		self.0.key()
	}

	/// The key the entry would hold, taken.
	pub fn into_key(self) -> String {
		self.0.into_key()
	}

	/// Insert `value` under the entry's key, returning the value in place.
	pub fn insert(self, value: Value) -> &'a mut Value {
		self.0.insert(value)
	}
}

impl PartialEq<BTreeMap<String, Value>> for Object {
	fn eq(&self, other: &BTreeMap<String, Value>) -> bool {
		self.len() == other.len() && self.iter().eq(other.iter())
	}
}

impl PartialOrd<BTreeMap<String, Value>> for Object {
	fn partial_cmp(&self, other: &BTreeMap<String, Value>) -> Option<Ordering> {
		self.iter().partial_cmp(other.iter())
	}
}

#[cfg(test)]
mod tests {
	use std::hash::{BuildHasher, RandomState};

	use super::*;

	fn map(entries: &[(&str, i64)]) -> BTreeMap<String, Value> {
		entries.iter().map(|(k, v)| (k.to_string(), Value::from_t(*v))).collect()
	}

	fn object(entries: &[(&str, i64)]) -> Object {
		let mut object = Object::new();
		for (k, v) in entries {
			object.insert(*k, *v);
		}
		object
	}

	#[test]
	fn an_object_behaves_as_a_map_of_its_entries() {
		let cases: &[&[(&str, i64)]] = &[
			&[],
			&[("a", 1)],
			&[("b", 2), ("a", 1), ("c", 3)],
			&[("a", 1), ("a", 2), ("b", 3), ("a", 4)],
			&[("c", 1), ("a", 2), ("c", 3), ("b", 4), ("a", 5), ("c", 6)],
		];
		let hasher = RandomState::new();
		for &entries in cases {
			let built = object(entries);
			let map = map(entries);
			assert_eq!(built.clone().into_inner(), map);
			let object = Object::from(map.clone());
			assert_eq!(built, object);
			assert!(object == map);
			assert_eq!(format!("{object:?}"), format!("Object({map:?})"));
			assert_eq!(hasher.hash_one(&object), hasher.hash_one(&map));
			assert_eq!(object.keys().collect::<Vec<_>>(), map.keys().collect::<Vec<_>>());
			assert_eq!(object.values().collect::<Vec<_>>(), map.values().collect::<Vec<_>>());
			assert_eq!(
				object.iter().rev().collect::<Vec<_>>(),
				map.iter().rev().collect::<Vec<_>>()
			);
			assert_eq!(object.len(), map.len());
			let from_iter: Object = entries.iter().map(|(k, v)| (k.to_string(), *v)).collect();
			assert_eq!(from_iter, object);
			let json = serde_json::to_string(&object).unwrap();
			assert_eq!(json, serde_json::to_string(&map).unwrap());
			assert_eq!(serde_json::from_str::<Object>(&json).unwrap(), object);
		}
	}

	#[test]
	fn objects_order_as_their_maps_do() {
		let cases = [
			map(&[]),
			map(&[("a", 1)]),
			map(&[("a", 2)]),
			map(&[("a", 1), ("b", 1)]),
			map(&[("b", 0)]),
		];
		for a in &cases {
			for b in &cases {
				let (oa, ob) = (Object::from(a.clone()), Object::from(b.clone()));
				assert_eq!(oa.cmp(&ob), a.cmp(b), "{a:?} vs {b:?}");
				assert_eq!(oa == ob, a == b);
			}
		}
	}

	#[test]
	fn entries_are_found_inserted_and_removed_in_key_order() {
		let mut object = object(&[("m", 1), ("c", 2), ("x", 3)]);
		assert_eq!(object.insert("c", 20i64), Some(Value::from_t(2i64)));
		assert_eq!(object.get("c"), Some(&Value::from_t(20i64)));
		assert_eq!(object.remove("m"), Some(Value::from_t(1i64)));
		assert!(!object.contains_key("m"));
		*object.entry("a").or_insert(Value::from_t(0i64)) = Value::from_t(5i64);
		let _ = object.entry("x").and_modify(|v| *v = Value::from_t(30i64));
		assert_eq!(object, Object::from(map(&[("a", 5), ("c", 20), ("x", 30)])));
		object.retain(|k, _| k != "c");
		assert_eq!(object.keys().collect::<Vec<_>>(), ["a", "x"]);
		assert_eq!(object["x"], Value::from_t(30i64));
		let mut other = Object::from(map(&[("x", 1), ("b", 2)]));
		object.append(&mut other);
		assert!(other.is_empty());
		assert_eq!(object, Object::from(map(&[("a", 5), ("b", 2), ("x", 1)])));
		// A merge of interleaved keys, the appended value winning a key both hold.
		let mut left = Object::from(map(&[("b", 1), ("d", 1), ("f", 1)]));
		let mut right = Object::from(map(&[("a", 2), ("d", 2), ("e", 2), ("g", 2)]));
		left.append(&mut right);
		assert_eq!(
			left,
			Object::from(map(&[("a", 2), ("b", 1), ("d", 2), ("e", 2), ("f", 1), ("g", 2)]))
		);
		let mut empty = Object::new();
		empty.append(&mut left.clone());
		assert_eq!(empty, left);
		assert_eq!(object.pop_first(), Some(("a".to_string(), Value::from_t(5i64))));
		assert_eq!(object.pop_last(), Some(("x".to_string(), Value::from_t(1i64))));
	}
}
