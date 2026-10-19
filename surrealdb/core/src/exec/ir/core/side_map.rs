//! A dense side table keyed by an arena id, allocated on its first insert.

use slotmap::secondary::Entry;
use slotmap::{Key, SecondaryMap};

/// A [`SecondaryMap`] that holds no storage until a value is inserted.
///
/// `SecondaryMap::new` allocates a sentinel slot, so every empty side table of a container would
/// cost one allocation whether or not a pass ever writes to it. Most side tables of a small
/// program stay empty, and containers and fact caches are built several times per compile, so
/// this type answers reads on an empty table without storage and allocates on the first insert.
/// Lookups on a populated table index the dense map exactly as `SecondaryMap` does.
#[derive(Clone, Debug)]
pub(crate) struct SideMap<K: Key, V>(Option<SecondaryMap<K, V>>);

impl<K: Key, V> Default for SideMap<K, V> {
	fn default() -> Self {
		Self(None)
	}
}

impl<K: Key, V> SideMap<K, V> {
	/// An empty table, holding no storage.
	pub(crate) const fn new() -> Self {
		Self(None)
	}

	pub(crate) fn get(&self, key: K) -> Option<&V> {
		self.0.as_ref()?.get(key)
	}

	pub(crate) fn get_mut(&mut self, key: K) -> Option<&mut V> {
		self.0.as_mut()?.get_mut(key)
	}

	pub(crate) fn contains_key(&self, key: K) -> bool {
		self.0.as_ref().is_some_and(|map| map.contains_key(key))
	}

	/// Inserts `value` under `key` and answers the value it replaced. A null or otherwise
	/// unusable key inserts nothing, as with [`SecondaryMap::insert`].
	pub(crate) fn insert(&mut self, key: K, value: V) -> Option<V> {
		self.0.get_or_insert_with(SecondaryMap::new).insert(key, value)
	}

	/// The entry of `key`, allocating the table's storage; `None` for a key whose slot is gone, as
	/// with [`SecondaryMap::entry`].
	pub(crate) fn entry(&mut self, key: K) -> Option<Entry<'_, K, V>> {
		self.0.get_or_insert_with(SecondaryMap::new).entry(key)
	}

	pub(crate) fn remove(&mut self, key: K) -> Option<V> {
		self.0.as_mut()?.remove(key)
	}

	pub(crate) fn len(&self) -> usize {
		self.0.as_ref().map_or(0, SecondaryMap::len)
	}

	pub(crate) fn is_empty(&self) -> bool {
		self.0.as_ref().is_none_or(SecondaryMap::is_empty)
	}

	pub(crate) fn clear(&mut self) {
		if let Some(map) = &mut self.0 {
			map.clear();
		}
	}

	pub(crate) fn retain(&mut self, f: impl FnMut(K, &mut V) -> bool) {
		if let Some(map) = &mut self.0 {
			map.retain(f);
		}
	}

	pub(crate) fn iter(&self) -> impl Iterator<Item = (K, &V)> {
		self.0.iter().flat_map(SecondaryMap::iter)
	}

	pub(crate) fn keys(&self) -> impl Iterator<Item = K> + '_ {
		self.0.iter().flat_map(SecondaryMap::keys)
	}

	pub(crate) fn values(&self) -> impl Iterator<Item = &V> {
		self.0.iter().flat_map(SecondaryMap::values)
	}
}
