use crate::sql::number::Number;
use std::cmp::Reverse;
use std::collections::BinaryHeap;

pub trait Top {
	/// Find the greatest `k` records from the collection, returned in arbetrary order  
	/// O(n*k*log(k)) time complex
	fn top(self, _c: i64) -> Vec<Number>;
}

impl Top for Vec<Number> {
	fn top(self, _c: i64) -> Vec<Number> {
		let mut heap = BinaryHeap::new();

		for idx in 0..self.len() {
			let n = Reverse(self.get(idx).unwrap().clone());

			if idx > _c as usize {
				heap.push(n);
				heap.pop();
			} else {
				heap.push(n);
			}
		}

		heap.into_iter().map(|x| x.0).collect()
	}
}
