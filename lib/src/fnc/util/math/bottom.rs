use crate::sql::number::Number;
use std::collections::BinaryHeap;

pub trait Bottom {
	/// Find the lowest `k` records from the collection in arbetrary order  
	/// O(n*k*log(k)) time complex 
	fn bottom(self, _c: i64) -> Vec<Number>;
}

impl Bottom for Vec<Number> {
	fn bottom(self, _c: i64) -> Vec<Number> {


		let mut heap = BinaryHeap::new();

		for idx in 0..self.len(){
			let n = self.get(idx).unwrap().clone();
			heap.push(n.clone());

			if idx>_c as usize{
				heap.pop();
			}


		}

		heap.into_iter().collect()
		
	}
}
