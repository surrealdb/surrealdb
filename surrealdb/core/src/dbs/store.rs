use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::mem;
use std::sync::Arc;

use rand::prelude::SliceRandom;
use rand::{Rng, thread_rng};
#[cfg(not(target_family = "wasm"))]
use rayon::prelude::ParallelSliceMut;
#[cfg(not(target_family = "wasm"))]
use tokio::task::spawn_blocking;

use crate::dbs::plan::Explanation;
#[cfg(not(target_family = "wasm"))]
use crate::err::Error;
use crate::expr::order::OrderList;
use crate::val::Value;

#[derive(Default, Debug)]
pub(super) struct MemoryCollector(Vec<Value>);

impl MemoryCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	pub(super) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		vec_start_limit(&mut self.0, start, limit);
	}

	pub(super) fn take_vec(&mut self) -> Vec<Value> {
		mem::take(&mut self.0)
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("Memory", vec![]);
	}
}

impl From<Vec<Value>> for MemoryCollector {
	fn from(values: Vec<Value>) -> Self {
		Self(values)
	}
}

pub(super) const DEFAULT_BATCH_SIZE: usize = 1024;

/// The struct MemoryRandom represents an in-memory store that aggregates data
/// randomly.
pub(in crate::dbs) struct MemoryRandom {
	/// Collected values
	values: Vec<Value>,
	/// Ordered index (empty after sort is finalized)
	ordered: Vec<usize>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current batch of values to be merged once full
	batch: Vec<Value>,
}

impl MemoryRandom {
	pub(in crate::dbs) fn new(batch_size: Option<usize>) -> Self {
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
		Self {
			batch_size,
			values: Vec::new(),
			ordered: Vec::new(),
			batch: Vec::with_capacity(batch_size),
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		// If sorted (ordered is empty and no pending batch), return values.len()
		// Otherwise return values + batch
		if self.ordered.is_empty() && self.batch.is_empty() {
			self.values.len()
		} else {
			self.values.len() + self.batch.len()
		}
	}

	fn shuffle_batch(values: &mut Vec<Value>, ordered: &mut Vec<usize>, batch: Vec<Value>) {
		let mut rng = thread_rng();

		// Compute the index range of this new batch
		let start = ordered.len();
		let end = start + batch.len();

		// We extend the ordered indexes
		ordered.extend(start..end);

		// We add the batch to the value vector
		values.extend(batch);

		// Is this the first batch?
		if start == 0 {
			// We add the indexes to the ordered vector
			// Then we just shuffle the order
			ordered.shuffle(&mut rng);
			return;
		}

		// Fisher-Yates shuffle to shuffle the elements as they are merged
		for idx in start..end {
			let j = rng.gen_range(0..=idx);
			ordered.swap(idx, j);
		}
	}

	fn send_batch(&mut self) {
		let batch = mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size));
		Self::shuffle_batch(&mut self.values, &mut self.ordered, batch);
	}

	pub(in crate::dbs) fn push(&mut self, value: Value) {
		// Add the value to the current batch
		self.batch.push(value);
		if self.batch_size == self.batch.len() {
			// The batch is full, we can add it to the main vectors
			self.send_batch();
		}
	}

	pub(in crate::dbs) fn sort(&mut self) {
		// Make sure there is no pending batch
		if !self.batch.is_empty() {
			self.send_batch();
		}
		// Apply permutation in-place
		apply_permutation_in_place(&mut self.values, &mut self.ordered);
		// Clear ordered to mark as finalized (and free memory)
		self.ordered.clear();
	}

	pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		// Only apply if sorted (ordered is empty)
		if self.ordered.is_empty() {
			vec_start_limit(&mut self.values, start, limit);
		}
	}

	pub(in crate::dbs) fn take_vec(&mut self) -> Vec<Value> {
		// If sorted (ordered is empty and batch is empty), just take values
		if self.ordered.is_empty() && self.batch.is_empty() {
			mem::take(&mut self.values)
		} else {
			// Otherwise, assemble values + batch (unsorted fallback path)
			let mut vec = mem::take(&mut self.values);
			vec.append(&mut mem::take(&mut self.batch));
			vec
		}
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryRandom", vec![]);
	}
}

/// The struct MemoryOrdered represents an in-memory store that aggregates
/// ordered data.
pub(in crate::dbs) struct MemoryOrdered {
	/// Collected values
	values: Vec<Value>,
	/// Ordered index (empty after sort is finalized)
	ordered: Vec<usize>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current batch of values to be merged once full
	batch: Vec<Value>,
	/// The order specification
	orders: OrderList,
}

impl MemoryOrdered {
	pub(in crate::dbs) fn new(orders: OrderList, batch_size: Option<usize>) -> Self {
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
		Self {
			batch_size,
			values: Vec::new(),
			ordered: Vec::new(),
			batch: Vec::with_capacity(batch_size),
			orders,
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		// If sorted (ordered is empty and no pending batch), return values.len()
		// Otherwise return values + batch
		if self.ordered.is_empty() && self.batch.is_empty() {
			self.values.len()
		} else {
			self.values.len() + self.batch.len()
		}
	}

	fn send_batch(&mut self) {
		self.values.append(&mut self.batch);
		self.ordered.extend(self.ordered.len()..self.values.len());
	}

	pub(in crate::dbs) fn push(&mut self, value: Value) {
		// Add the value to the current batch
		self.batch.push(value);
		if self.batch_size == self.batch.len() {
			// The batch is full, we can add it to the main vectors
			self.send_batch();
		}
	}

	#[cfg(target_family = "wasm")]
	pub(super) fn sort(&mut self) {
		// Make sure there is no pending batch
		if !self.batch.is_empty() {
			self.send_batch();
		}
		// If ordered is empty, nothing to sort (already sorted or empty)
		if self.ordered.is_empty() {
			return;
		}
		let mut ordered = mem::take(&mut self.ordered);
		ordered.sort_unstable_by(|a, b| self.orders.compare(&self.values[*a], &self.values[*b]));
		apply_permutation_in_place(&mut self.values, &mut ordered);
		// ordered is already empty from mem::take, so it stays cleared
	}

	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn sort(&mut self) -> Result<(), Error> {
		// Make sure there is no pending batch
		if !self.batch.is_empty() {
			self.send_batch();
		}
		// If ordered is empty, nothing to sort (already sorted or empty)
		if self.ordered.is_empty() {
			return Ok(());
		}
		let mut ordered = mem::take(&mut self.ordered);
		let mut values = mem::take(&mut self.values);
		let orders = self.orders.clone();
		self.values = spawn_blocking(move || {
			ordered.par_sort_unstable_by(|a, b| orders.compare(&values[*a], &values[*b]));
			apply_permutation_in_place(&mut values, &mut ordered);
			values
		})
		.await
		.map_err(|e| Error::OrderingError(format!("{e}")))?;
		// ordered is already empty from mem::take, so it stays cleared
		Ok(())
	}

	pub(super) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		// Only apply if sorted (ordered is empty)
		if self.ordered.is_empty() {
			vec_start_limit(&mut self.values, start, limit);
		}
	}

	pub(super) fn take_vec(&mut self) -> Vec<Value> {
		// If sorted (ordered is empty and batch is empty), just take values
		if self.ordered.is_empty() && self.batch.is_empty() {
			mem::take(&mut self.values)
		} else {
			// Otherwise, assemble values + batch (unsorted fallback path)
			let mut vec = mem::take(&mut self.values);
			vec.append(&mut mem::take(&mut self.batch));
			vec
		}
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryOrdered", vec![]);
	}
}

pub(super) struct OrderedValue {
	value: Value,
	orders: Arc<OrderList>,
}
impl PartialOrd<Self> for OrderedValue {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Eq for OrderedValue {}

impl Ord for OrderedValue {
	fn cmp(&self, other: &Self) -> Ordering {
		self.orders.compare(&other.value, &self.value)
	}
}

impl PartialEq<Self> for OrderedValue {
	fn eq(&self, other: &Self) -> bool {
		self.value.eq(&other.value)
	}
}

pub(super) struct MemoryOrderedLimit {
	/// The priority list
	heap: BinaryHeap<Reverse<OrderedValue>>,
	/// The maximum size of the priority list
	limit: usize,
	/// The order specification
	orders: Arc<OrderList>,
	/// The finalized result
	result: Option<Vec<Value>>,
}

impl MemoryOrderedLimit {
	pub(super) fn new(limit: usize, orders: OrderList) -> Self {
		Self {
			heap: BinaryHeap::with_capacity(limit + 1),
			limit,
			orders: Arc::new(orders),
			result: None,
		}
	}

	pub(in crate::dbs) fn push(&mut self, value: Value) {
		if self.heap.len() >= self.limit {
			// When the heap is full, first check if the new value
			// if smaller that the top of this min-heap in order to
			// prevent unnecessary push/pop and Arc::clone.
			if let Some(top) = self.heap.peek() {
				let cmp = self.orders.compare(&value, &top.0.value);
				if cmp == Ordering::Less {
					self.heap.push(Reverse(OrderedValue {
						value,
						orders: self.orders.clone(),
					}));
					self.heap.pop();
				}
			}
		} else {
			// Push the value onto the heap because it's not full.
			self.heap.push(Reverse(OrderedValue {
				value,
				orders: self.orders.clone(),
			}));
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		self.heap.len()
	}

	pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		if let Some(ref mut result) = self.result {
			vec_start_limit(result, start, limit);
		}
	}

	pub(super) fn sort(&mut self) {
		if self.result.is_none() {
			let mut sorted_vec: Vec<_> = Vec::with_capacity(self.heap.len());
			while let Some(i) = self.heap.pop() {
				sorted_vec.push(i.0.value);
			}
			sorted_vec.reverse();
			self.result = Some(sorted_vec);
		}
	}

	pub(in crate::dbs) fn take_vec(&mut self) -> Vec<Value> {
		self.result.take().unwrap_or_default()
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryOrderedLimit", vec![("limit", self.limit.into())]);
	}
}

fn vec_start_limit<T>(vec: &mut Vec<T>, start: Option<u32>, limit: Option<u32>) {
	if let Some(start) = start {
		let start = start as usize;
		if start > 0 {
			let drain_end = start.min(vec.len());
			vec.drain(..drain_end);
		}
	}
	if let Some(limit) = limit {
		let limit = limit as usize;
		if vec.len() > limit {
			vec.truncate(limit);
		}
	}
}

/// Applies a permutation in-place to a vector of values.
///
/// The `ordered` vector is expected to contain the indices of the `values` in the
/// positions they should be in order to be sorted.
///
/// This function iterates over the `ordered` vector and swaps the values in the `values` vector
/// to the positions they should be in order to be sorted. Whenever a value is swapped, the index
/// of the value in the `ordered` vector is updated to the new position.
///
/// The values and ordered vectors must have the same length.
pub(crate) fn apply_permutation_in_place<T>(values: &mut [T], ordered: &mut [usize]) {
	debug_assert!(values.len() == ordered.len());

	for i in 0..ordered.len() {
		// If already in correct position, skip
		if ordered[i] == i {
			continue;
		}

		// Follow the cycle
		let mut current = i;
		loop {
			let target = ordered[current];

			// Mark as visited
			ordered[current] = current;

			if target == i {
				// Cycle complete
				break;
			}

			values.swap(current, target);
			current = target;
		}
	}
}

#[cfg(test)]
mod tests {
	use rstest::rstest;

	use super::*;

	#[rstest]
	#[case::empty(vec![], vec![])]
	#[case::single(vec![1], vec![0])]
	#[case::two(vec![1, 2], vec![0, 1])]
	#[case::two(vec![2, 1], vec![1, 0])]
	#[case::three(vec![1, 2, 3], vec![0, 1, 2])]
	#[case::three(vec![1, 3, 2], vec![0, 2, 1])]
	#[case::three(vec![2, 1, 3], vec![1, 0, 2])]
	#[case::three(vec![2, 3, 1], vec![1, 2, 0])]
	#[case::three(vec![3, 1, 2], vec![2, 0, 1])]
	#[case::three(vec![3, 2, 1], vec![2, 1, 0])]
	fn test_apply_permutation_in_place(
		#[case] mut values: Vec<i32>,
		#[case] mut ordered: Vec<usize>,
	) {
		let mut expected = values.clone();
		expected.sort_by(|a, b| a.cmp(b));
		apply_permutation_in_place(&mut values, &mut ordered);
		assert_eq!(values, expected);
	}

	#[rstest]
	#[case::none(vec![], None, None, vec![])]
	#[case::none(vec![1, 2], None, None, vec![1, 2])]
	#[case::start(vec![], Some(0), None, vec![])]
	#[case::start(vec![1, 2], Some(0), None, vec![1, 2])]
	#[case::start(vec![1, 2], Some(1), None, vec![2])]
	#[case::start(vec![1, 2], Some(2), None, vec![])]
	#[case::start_overflow(vec![1, 2], Some(3), None, vec![])]
	#[case::limit(vec![], None, Some(0), vec![])]
	#[case::limit(vec![1, 2], None, Some(0), vec![])]
	#[case::limit(vec![1, 2], None, Some(1), vec![1])]
	#[case::limit(vec![1, 2], None, Some(2), vec![1, 2])]
	#[case::limit_overflow(vec![1, 2], None, Some(3), vec![1, 2])]
	#[case::start_limit(vec![], Some(0), Some(0), vec![])]
	#[case::start_limit(vec![1, 2], Some(0), Some(0), vec![])]
	#[case::start_limit(vec![1, 2], Some(0), Some(1), vec![1])]
	#[case::start_limit(vec![1, 2], Some(0), Some(2), vec![1, 2])]
	#[case::start_limit(vec![1, 2], Some(1), Some(0), vec![])]
	#[case::start_limit(vec![1, 2], Some(1), Some(1), vec![2])]
	#[case::start_limit(vec![1, 2], Some(1), Some(2), vec![2])]
	#[case::start_limit_overflow(vec![1, 2], Some(3), Some(0), vec![])]
	fn test_vec_start_limit(
		#[case] mut vec: Vec<i32>,
		#[case] start: Option<u32>,
		#[case] limit: Option<u32>,
		#[case] expected: Vec<i32>,
	) {
		vec_start_limit(&mut vec, start, limit);
		assert_eq!(vec, expected);
	}
}
