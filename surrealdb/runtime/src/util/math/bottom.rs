use std::collections::BinaryHeap;

use surrealdb_expr::val::Number;

use crate::fuel;

/// The least `k` numbers of `array`, in no particular order: a walk keeping
/// them in a heap.
pub async fn bottom(array: Vec<Number>, k: i64) -> Vec<Number> {
	let k = k as usize;
	let mut heap = BinaryHeap::new();
	fuel::walk(array.into_iter().enumerate(), |(i, v)| {
		heap.push(v);
		if i >= k {
			heap.pop();
		}
		std::ops::ControlFlow::Continue(())
	})
	.await;
	heap.into_iter().collect()
}
