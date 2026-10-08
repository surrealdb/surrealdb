use std::cmp::Reverse;
use std::collections::BinaryHeap;

use surrealdb_expr::val::Number;

use crate::fuel;

/// The greatest `k` numbers of `array`, in no particular order: a walk
/// keeping them in a heap.
pub async fn top(array: Vec<Number>, k: i64) -> Vec<Number> {
	let k = k as usize;
	let mut heap = BinaryHeap::new();
	fuel::walk(array.into_iter().enumerate(), |(i, v)| {
		heap.push(Reverse(v));
		if i >= k {
			heap.pop();
		}
		std::ops::ControlFlow::Continue(())
	})
	.await;
	heap.into_iter().map(|x| x.0).collect()
}
