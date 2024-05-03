use hashbrown::HashSet;
use smallvec::{Array, SmallVec};
use std::hash::Hash;

#[derive(Debug)]
pub enum DynamicSet<T>
where
	T: Eq + Hash + Clone + Copy + Default + 'static,
{
	Small5(SmallVecSet<T, [T; 5]>),
	Small9(SmallVecSet<T, [T; 9]>),
	Small13(SmallVecSet<T, [T; 13]>),
	Small17(SmallVecSet<T, [T; 17]>),
	Array21(ArraySet<T, 21>),
	Array25(ArraySet<T, 25>),
	Array29(ArraySet<T, 29>),
	Hash(HashBrownSet<T>),
}

impl<T> DynamicSet<T>
where
	T: Eq + Hash + Clone + Copy + Default + 'static,
{
	pub fn with_capacity(capacity: usize) -> Self {
		// We need one more in capacity due to temporary overflow during neighborhood selection
		match capacity {
			0 => unreachable!(),
			1..=4 => Self::Small5(SmallVecSet(SmallVec::<[T; 5]>::new())),
			5..=8 => Self::Small9(SmallVecSet(SmallVec::<[T; 9]>::new())),
			9..=12 => Self::Small13(SmallVecSet(SmallVec::<[T; 13]>::new())),
			13..=16 => Self::Small17(SmallVecSet(SmallVec::<[T; 17]>::new())),
			17..=20 => Self::Array21(ArraySet::<T, 21>::default()),
			21..=24 => Self::Array25(ArraySet::<T, 25>::default()),
			25..=28 => Self::Array29(ArraySet::<T, 29>::default()),
			_ => Self::Hash(HashBrownSet::with_capacity(capacity)),
		}
	}
}

impl<T> DynamicSetImpl<T> for DynamicSet<T>
where
	T: Eq + Hash + Clone + Copy + Default + 'static,
{
	fn insert(&mut self, v: T) -> bool {
		match self {
			DynamicSet::Small5(s) => s.insert(v),
			DynamicSet::Small9(s) => s.insert(v),
			DynamicSet::Small13(s) => s.insert(v),
			DynamicSet::Small17(s) => s.insert(v),
			DynamicSet::Array21(s) => s.insert(v),
			DynamicSet::Array25(s) => s.insert(v),
			DynamicSet::Array29(s) => s.insert(v),
			DynamicSet::Hash(s) => s.insert(v),
		}
	}

	fn contains(&self, v: &T) -> bool {
		match self {
			DynamicSet::Small5(s) => s.contains(v),
			DynamicSet::Small9(s) => s.contains(v),
			DynamicSet::Small13(s) => s.contains(v),
			DynamicSet::Small17(s) => s.contains(v),
			DynamicSet::Array21(s) => s.contains(v),
			DynamicSet::Array25(s) => s.contains(v),
			DynamicSet::Array29(s) => s.contains(v),
			DynamicSet::Hash(s) => s.contains(v),
		}
	}

	fn remove(&mut self, v: &T) -> bool {
		match self {
			DynamicSet::Small5(s) => s.remove(v),
			DynamicSet::Small9(s) => s.remove(v),
			DynamicSet::Small13(s) => s.remove(v),
			DynamicSet::Small17(s) => s.remove(v),
			DynamicSet::Array21(s) => s.remove(v),
			DynamicSet::Array25(s) => s.remove(v),
			DynamicSet::Array29(s) => s.remove(v),
			DynamicSet::Hash(s) => s.remove(v),
		}
	}

	fn len(&self) -> usize {
		match self {
			DynamicSet::Small5(s) => s.len(),
			DynamicSet::Small9(s) => s.len(),
			DynamicSet::Small13(s) => s.len(),
			DynamicSet::Small17(s) => s.len(),
			DynamicSet::Array21(s) => s.len(),
			DynamicSet::Array25(s) => s.len(),
			DynamicSet::Array29(s) => s.len(),
			DynamicSet::Hash(s) => s.len(),
		}
	}

	fn is_empty(&self) -> bool {
		match self {
			DynamicSet::Small5(s) => s.is_empty(),
			DynamicSet::Small9(s) => s.is_empty(),
			DynamicSet::Small13(s) => s.is_empty(),
			DynamicSet::Small17(s) => s.is_empty(),
			DynamicSet::Array21(s) => s.is_empty(),
			DynamicSet::Array25(s) => s.is_empty(),
			DynamicSet::Array29(s) => s.is_empty(),
			DynamicSet::Hash(s) => s.is_empty(),
		}
	}

	fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_> {
		match self {
			DynamicSet::Small5(s) => s.iter(),
			DynamicSet::Small9(s) => s.iter(),
			DynamicSet::Small13(s) => s.iter(),
			DynamicSet::Small17(s) => s.iter(),
			DynamicSet::Array21(s) => s.iter(),
			DynamicSet::Array25(s) => s.iter(),
			DynamicSet::Array29(s) => s.iter(),
			DynamicSet::Hash(s) => s.iter(),
		}
	}
}

pub trait DynamicSetImpl<T>
where
	T: Eq + Hash + Clone + Default + 'static,
{
	fn insert(&mut self, v: T) -> bool;
	fn contains(&self, v: &T) -> bool;
	fn remove(&mut self, v: &T) -> bool;
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool;
	fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_>;
}

#[derive(Debug)]
pub struct SmallVecSet<T, A>(SmallVec<A>)
where
	A: Array<Item = T>,
	T: Eq + Hash + Clone + Default + 'static;

impl<A, T> DynamicSetImpl<T> for SmallVecSet<T, A>
where
	A: Array<Item = T>,
	T: Eq + Hash + Clone + Default + 'static,
{
	#[inline]
	fn insert(&mut self, v: T) -> bool {
		if !self.0.contains(&v) {
			#[cfg(debug_assertions)]
			if self.0.len() == self.0.capacity() {
				unreachable!()
			}
			self.0.push(v);
			true
		} else {
			false
		}
	}

	#[inline]
	fn contains(&self, v: &T) -> bool {
		self.0.contains(v)
	}

	#[inline]
	fn remove(&mut self, v: &T) -> bool {
		if let Some(p) = self.0.iter().position(|e| e.eq(v)) {
			self.0.remove(p);
			true
		} else {
			false
		}
	}

	#[inline]
	fn len(&self) -> usize {
		self.0.len()
	}

	fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	#[inline]
	fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_> {
		Box::new(self.0.iter())
	}
}

#[derive(Debug)]
pub struct HashBrownSet<T>(HashSet<T>);

impl<T> HashBrownSet<T>
where
	T: Eq + Hash + Clone + Default + 'static,
{
	pub(super) fn with_capacity(capacity: usize) -> Self {
		Self(HashSet::with_capacity(capacity))
	}
}
impl<T> DynamicSetImpl<T> for HashBrownSet<T>
where
	T: Eq + Hash + Clone + Default + 'static,
{
	#[inline]
	fn insert(&mut self, v: T) -> bool {
		self.0.insert(v)
	}

	#[inline]
	fn contains(&self, v: &T) -> bool {
		self.0.contains(v)
	}

	#[inline]
	fn remove(&mut self, v: &T) -> bool {
		self.0.remove(v)
	}

	#[inline]
	fn len(&self) -> usize {
		self.0.len()
	}

	fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	#[inline]
	fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_> {
		Box::new(self.0.iter())
	}
}

#[derive(Debug)]
pub struct ArraySet<T, const N: usize>
where
	T: Eq + Hash + Clone + Default + 'static,
{
	array: [T; N],
	size: usize,
}

impl<T, const N: usize> Default for ArraySet<T, N>
where
	T: Eq + Hash + Copy + Clone + Default + 'static,
{
	#[inline]
	fn default() -> Self {
		Self {
			array: [T::default(); N],
			size: 0,
		}
	}
}

impl<T, const N: usize> DynamicSetImpl<T> for ArraySet<T, N>
where
	T: Eq + Hash + Clone + Default + 'static,
{
	#[inline]
	fn insert(&mut self, v: T) -> bool {
		if !self.contains(&v) {
			self.array[self.size] = v;
			self.size += 1;
			true
		} else {
			false
		}
	}

	#[inline]
	fn contains(&self, v: &T) -> bool {
		self.array[0..self.size].contains(v)
	}

	#[inline]
	fn remove(&mut self, v: &T) -> bool {
		if let Some(p) = self.array[0..self.size].iter().position(|e| e.eq(v)) {
			self.array[p..].rotate_left(1);
			self.size -= 1;
			true
		} else {
			false
		}
	}

	#[inline]
	fn len(&self) -> usize {
		self.size
	}

	fn is_empty(&self) -> bool {
		self.size == 0
	}

	#[inline]
	fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_> {
		Box::new(self.array[0..self.size].iter())
	}
}

#[cfg(test)]
mod tests {
	use crate::idx::trees::dynamicset::{DynamicSet, DynamicSetImpl};
	use std::collections::HashSet;

	#[test]
	fn test_dynamic_set() {
		for capacity in 1..50 {
			let mut dyn_set = DynamicSet::with_capacity(capacity);
			let mut control = HashSet::new();
			// Test insertions
			for sample in 0..capacity {
				assert_eq!(dyn_set.len(), control.len(), "{capacity} - {sample}");
				let v: HashSet<usize> = dyn_set.iter().cloned().collect();
				assert_eq!(v, control, "{capacity} - {sample}");
				// We should not have the element yet
				assert_eq!(dyn_set.contains(&sample), false, "{capacity} - {sample}");
				// The first insertion returns true
				assert_eq!(dyn_set.insert(sample), true);
				assert_eq!(dyn_set.contains(&sample), true, "{capacity} - {sample}");
				// The second insertion returns false
				assert_eq!(dyn_set.insert(sample), false);
				assert_eq!(dyn_set.contains(&sample), true, "{capacity} - {sample}");
				// We update the control structure
				control.insert(sample);
			}
			// Test removals
			for sample in 0..capacity {
				// The first removal returns true
				assert_eq!(dyn_set.remove(&sample), true);
				assert_eq!(dyn_set.contains(&sample), false, "{capacity} - {sample}");
				// The second removal returns false
				assert_eq!(dyn_set.remove(&sample), false);
				assert_eq!(dyn_set.contains(&sample), false, "{capacity} - {sample}");
				// We update the control structure
				control.remove(&sample);
				// The control structure and the dyn_set should be identical
				assert_eq!(dyn_set.len(), control.len(), "{capacity} - {sample}");
				let v: HashSet<usize> = dyn_set.iter().cloned().collect();
				assert_eq!(v, control, "{capacity} - {sample}");
			}
		}
	}
}
