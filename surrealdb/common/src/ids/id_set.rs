use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash, RandomState};
use std::ops::{Index, IndexMut};

use hashbrown::HashTable;

use super::Id;

pub trait SetEntry<T>: Hash {
	fn into_owned(self) -> T;

	fn equal(&self, other: &T) -> bool;
}

impl<T: Hash + Eq> SetEntry<T> for T {
	fn into_owned(self) -> T {
		self
	}

	fn equal(&self, other: &T) -> bool {
		self == other
	}
}

impl SetEntry<String> for &str {
	fn into_owned(self) -> String {
		self.to_owned()
	}

	fn equal(&self, other: &String) -> bool {
		self == other
	}
}

/// A collection which will ensure that the storage only contains unique values.
/// If two values are pushed which are equal to each-other this collection will instead return the
/// id of the previous value.
#[derive(Default)]
pub struct IdSet<I, V, S = RandomState> {
	map: HashTable<I>,
	storage: Vec<V>,
	hasher: S,
}

impl<I, V> IdSet<I, V>
where
	I: Id,
	V: Eq + Hash,
{
	pub fn new() -> Self {
		IdSet {
			map: HashTable::new(),
			storage: Vec::new(),
			hasher: RandomState::new(),
		}
	}
}

impl<I, V> Debug for IdSet<I, V>
where
	I: Id + Debug,
	V: Eq + Hash + Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		let mut fmt = f.debug_list();
		for idx in self.map.iter() {
			fmt.entry(&self.storage[idx.idx()]);
		}
		fmt.finish()
	}
}

impl<I, V, S> IdSet<I, V, S>
where
	I: Id,
	V: Eq + Hash,
	S: BuildHasher,
{
	pub fn push<T>(&mut self, v: T) -> Option<I>
	where
		T: SetEntry<V>,
	{
		let hash = self.hasher.hash_one(&v);
		match self.map.find_entry(hash, |s| v.equal(&self.storage[s.idx()])) {
			Ok(x) => Some(*x.get()),
			Err(slot) => {
				let idx = I::from_idx(self.storage.len())?;
				self.storage.push(v.into_owned());
				slot.into_table()
					.insert_unique(hash, idx, |x| self.hasher.hash_one(&self.storage[x.idx()]));
				Some(idx)
			}
		}
	}

	pub fn clear(&mut self) {
		self.map.clear();
		self.storage.clear();
	}

	pub fn len(&self) -> usize {
		self.storage.len()
	}

	pub fn is_empty(&self) -> bool {
		self.storage.is_empty()
	}
}

impl<I: Id, V, S> IdSet<I, V, S> {
	pub fn get(&self, index: I) -> Option<&V> {
		self.storage.get(index.idx())
	}

	pub fn get_mut(&mut self, index: I) -> Option<&mut V> {
		self.storage.get_mut(index.idx())
	}
}

impl<I, V, S> Index<I> for IdSet<I, V, S>
where
	I: Id,
	V: Eq + Hash,
	S: BuildHasher,
{
	type Output = V;

	fn index(&self, index: I) -> &Self::Output {
		let Some(x) = self.get(index) else {
			panic!("Tired to index into id set with out of range index {}", index.idx())
		};
		x
	}
}

impl<I, V, S> IndexMut<I> for IdSet<I, V, S>
where
	I: Id,
	V: Eq + Hash,
	S: BuildHasher,
{
	#[track_caller]
	fn index_mut(&mut self, index: I) -> &mut Self::Output {
		let Some(x) = self.get_mut(index) else {
			panic!("Tired to index into id set with out of range index {}", index.idx())
		};
		x
	}
}
