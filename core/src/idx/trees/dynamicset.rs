use hashbrown::HashSet;
use std::fmt::Debug;
use std::hash::Hash;

pub trait DynamicSet<T>: Debug + Send + Sync
where
	T: Eq + Hash + Clone + Default + 'static + Send + Sync,
{
	fn with_capacity(capacity: usize) -> Self;
	fn insert(&mut self, v: T) -> bool;
	fn contains(&self, v: &T) -> bool;
	fn remove(&mut self, v: &T) -> bool;
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool;
	fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_>;
}

#[derive(Debug)]
pub struct HashBrownSet<T>(HashSet<T>);

impl<T> DynamicSet<T> for HashBrownSet<T>
where
	T: Eq + Hash + Clone + Default + Debug + 'static + Send + Sync,
{
	#[inline]
	fn with_capacity(capacity: usize) -> Self {
		Self(HashSet::with_capacity(capacity))
	}

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

	#[inline]
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
	T: Eq + Hash + Clone + Default + 'static + Send + Sync,
{
	array: [T; N],
	size: usize,
}

impl<T, const N: usize> DynamicSet<T> for ArraySet<T, N>
where
	T: Eq + Hash + Clone + Copy + Default + Debug + 'static + Send + Sync,
{
	fn with_capacity(_capacity: usize) -> Self {
		#[cfg(debug_assertions)]
		assert!(_capacity <= N);
		Self {
			array: [T::default(); N],
			size: 0,
		}
	}

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

	#[inline]
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
	use crate::idx::trees::dynamicset::{ArraySet, DynamicSet, HashBrownSet};
	use hashbrown::HashSet;

	fn test_dynamic_set<S: DynamicSet<usize>>(capacity: usize) {
		let mut dyn_set = S::with_capacity(capacity);
		let mut control = HashSet::new();
		// Test insertions
		for sample in 0..capacity {
			assert_eq!(dyn_set.len(), control.len(), "{capacity} - {sample}");
			let v: HashSet<usize> = dyn_set.iter().cloned().collect();
			assert_eq!(v, control, "{capacity} - {sample}");
			// We should not have the element yet
			assert!(!dyn_set.contains(&sample), "{capacity} - {sample}");
			// The first insertion returns true
			assert!(dyn_set.insert(sample));
			assert!(dyn_set.contains(&sample), "{capacity} - {sample}");
			// The second insertion returns false
			assert!(!dyn_set.insert(sample));
			assert!(dyn_set.contains(&sample), "{capacity} - {sample}");
			// We update the control structure
			control.insert(sample);
		}
		// Test removals
		for sample in 0..capacity {
			// The first removal returns true
			assert!(dyn_set.remove(&sample));
			assert!(!dyn_set.contains(&sample), "{capacity} - {sample}");
			// The second removal returns false
			assert!(!dyn_set.remove(&sample));
			assert!(!dyn_set.contains(&sample), "{capacity} - {sample}");
			// We update the control structure
			control.remove(&sample);
			// The control structure and the dyn_set should be identical
			assert_eq!(dyn_set.len(), control.len(), "{capacity} - {sample}");
			let v: HashSet<usize> = dyn_set.iter().cloned().collect();
			assert_eq!(v, control, "{capacity} - {sample}");
		}
	}

	#[test]
	fn test_dynamic_set_hash() {
		for capacity in 1..50 {
			test_dynamic_set::<HashBrownSet<usize>>(capacity);
		}
	}

	#[test]
	fn test_dynamic_set_array() {
		test_dynamic_set::<ArraySet<usize, 1>>(1);
		test_dynamic_set::<ArraySet<usize, 2>>(2);
		test_dynamic_set::<ArraySet<usize, 4>>(4);
		test_dynamic_set::<ArraySet<usize, 10>>(10);
		test_dynamic_set::<ArraySet<usize, 20>>(20);
		test_dynamic_set::<ArraySet<usize, 30>>(30);
	}
}
