use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::val::Number;

pub trait Top {
	/// Find the greatest `k` records from the collection in arbetrary order
	/// O(n*k*log(k)) time complex
	fn top(self, k: i64) -> Vec<Number>;
}

impl Top for Vec<Number> {
	fn top(self, k: i64) -> Vec<Number> {
		// Convert to usize
		let k = k as usize;
		// Create a heap to store the numbers
		let mut heap = BinaryHeap::new();
		// Iterate and store the top numbers
		for (i, v) in self.into_iter().enumerate() {
			heap.push(Reverse(v));
			if i >= k {
				heap.pop();
			}
		}
		// Return the numbers as a vector
		heap.into_iter().map(|x| x.0).collect()
	}
}
