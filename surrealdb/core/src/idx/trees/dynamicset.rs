use std::fmt::Debug;

use ahash::{HashSet, HashSetExt};

use crate::idx::trees::hnsw::ElementId;

pub trait DynamicSet: Debug + Send + Sync {
	fn with_capacity(capacity: usize) -> Self;
	fn insert(&mut self, v: ElementId) -> bool;
	fn contains(&self, v: &ElementId) -> bool;
	fn remove(&mut self, v: &ElementId) -> bool;
	fn len(&self) -> usize;
	fn is_empty(&self) -> bool;
	fn iter(&self) -> impl Iterator<Item = &ElementId>;
}

#[derive(Debug)]
pub struct AHashSet(HashSet<ElementId>);

impl DynamicSet for AHashSet {
	#[inline]
	fn with_capacity(capacity: usize) -> Self {
		Self(HashSet::with_capacity(capacity))
	}

	#[inline]
	fn insert(&mut self, v: ElementId) -> bool {
		self.0.insert(v)
	}

	#[inline]
	fn contains(&self, v: &ElementId) -> bool {
		self.0.contains(v)
	}

	#[inline]
	fn remove(&mut self, v: &ElementId) -> bool {
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
	fn iter(&self) -> impl Iterator<Item = &ElementId> {
		self.0.iter()
	}
}

#[derive(Debug)]
pub struct ArraySet<const N: usize> {
	array: [ElementId; N],
	size: usize,
}

impl<const N: usize> DynamicSet for ArraySet<N> {
	fn with_capacity(_capacity: usize) -> Self {
		#[cfg(debug_assertions)]
		assert!(_capacity <= N);
		Self {
			array: [0; N],
			size: 0,
		}
	}

	#[inline]
	fn insert(&mut self, v: ElementId) -> bool {
		if !self.contains(&v) {
			self.array[self.size] = v;
			self.size += 1;
			true
		} else {
			false
		}
	}

	#[inline]
	fn contains(&self, v: &ElementId) -> bool {
		self.array[0..self.size].contains(v)
	}

	#[inline]
	fn remove(&mut self, v: &ElementId) -> bool {
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
	fn iter(&self) -> impl Iterator<Item = &ElementId> {
		self.array[0..self.size].iter()
	}
}

#[cfg(test)]
mod tests {
	use ahash::HashSet;

	use crate::idx::trees::dynamicset::{AHashSet, ArraySet, DynamicSet};
	use crate::idx::trees::hnsw::ElementId;

	fn test_dynamic_set<S: DynamicSet>(capacity: ElementId) {
		let mut dyn_set = S::with_capacity(capacity as usize);
		let mut control = HashSet::default();
		// Test insertions
		for sample in 0..capacity {
			assert_eq!(dyn_set.len(), control.len(), "{capacity} - {sample}");
			let v: HashSet<ElementId> = dyn_set.iter().copied().collect();
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
			let v: HashSet<ElementId> = dyn_set.iter().copied().collect();
			assert_eq!(v, control, "{capacity} - {sample}");
		}
	}

	#[test]
	fn test_dynamic_set_hash() {
		for capacity in 1..50 {
			test_dynamic_set::<AHashSet>(capacity);
		}
	}

	#[test]
	fn test_dynamic_set_array() {
		test_dynamic_set::<ArraySet<1>>(1);
		test_dynamic_set::<ArraySet<2>>(2);
		test_dynamic_set::<ArraySet<4>>(4);
		test_dynamic_set::<ArraySet<10>>(10);
		test_dynamic_set::<ArraySet<20>>(20);
		test_dynamic_set::<ArraySet<30>>(30);
	}
}
