use std::collections::BinaryHeap;

use crate::val::Number;

pub trait Bottom {
	/// Find the lowest `k` records from the collection in arbetrary order
	/// O(n*k*log(k)) time complex
	fn bottom(self, k: i64) -> Vec<Number>;
}

impl Bottom for Vec<Number> {
	fn bottom(self, k: i64) -> Vec<Number> {
		// Convert to usize
		let k = k as usize;
		// Create a heap to store the numbers
		let mut heap = BinaryHeap::new();
		// Iterate and store the bottom numbers
		for (i, v) in self.into_iter().enumerate() {
			heap.push(v);
			if i >= k {
				heap.pop();
			}
		}
		// Return the numbers as a vector
		heap.into_iter().collect()
	}
}
