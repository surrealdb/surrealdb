#[cfg(not(target_arch = "wasm32"))]
pub(super) mod asynchronous;
#[cfg(storage)]
pub(super) mod file;
#[cfg(not(target_arch = "wasm32"))]
mod llrbtree;

use crate::dbs::plan::Explanation;
#[cfg(not(target_arch = "wasm32"))]
use crate::err::Error;
use crate::sql::order::OrderList;
use crate::sql::value::Value;

use rand::prelude::SliceRandom;
use rand::{thread_rng, Rng};
#[cfg(not(target_arch = "wasm32"))]
use rayon::prelude::ParallelSliceMut;
use std::mem;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::spawn_blocking;

#[derive(Default)]
pub(super) struct MemoryCollector(Vec<Value>);

impl MemoryCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	fn vec_start_limit(start: Option<u32>, limit: Option<u32>, vec: &mut Vec<Value>) {
		match (start, limit) {
			(Some(start), Some(limit)) => {
				*vec =
					mem::take(vec).into_iter().skip(start as usize).take(limit as usize).collect()
			}
			(Some(start), None) => *vec = mem::take(vec).into_iter().skip(start as usize).collect(),
			(None, Some(limit)) => *vec = mem::take(vec).into_iter().take(limit as usize).collect(),
			(None, None) => {}
		}
	}

	pub(super) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		Self::vec_start_limit(start, limit, &mut self.0);
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

/// The struct MemoryRandom represents an in-memory store that aggregates data randomly.
pub(in crate::dbs) struct MemoryRandom {
	/// Collected values
	values: Vec<Value>,
	/// Ordered index
	ordered: Vec<usize>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current batch of values to be merged once full
	batch: Vec<Value>,
	/// The finalized result
	result: Option<Vec<Value>>,
}

impl MemoryRandom {
	pub(in crate::dbs) fn new(batch_size: Option<usize>) -> Self {
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
		Self {
			batch_size,
			values: Vec::new(),
			ordered: Vec::new(),
			batch: Vec::with_capacity(batch_size),
			result: None,
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		if let Some(result) = &self.result {
			// If we have a finalized result, we return its size
			result.len()
		} else {
			// If we don't have a finalized result, we return the current size
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

	fn ordered_values_to_vec(values: &mut [Value], ordered: &[usize]) -> Vec<Value> {
		let mut vec = Vec::with_capacity(values.len());
		for idx in ordered {
			vec.push(mem::take(&mut values[*idx]));
		}
		vec
	}

	pub(in crate::dbs) fn sort(&mut self) {
		// Make sure there is no pending batch
		if !self.batch.is_empty() {
			self.send_batch();
		}
		// We build final sorted vector
		let res = Self::ordered_values_to_vec(&mut self.values, &self.ordered);
		self.result = Some(res);
	}

	pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		if let Some(ref mut result) = self.result {
			MemoryCollector::vec_start_limit(start, limit, result);
		}
	}

	pub(in crate::dbs) fn take_vec(&mut self) -> Vec<Value> {
		self.result.take().unwrap_or_default()
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryRandom", vec![]);
	}
}

/// The struct MemoryRandom represents an in-memory store that aggregates data randomly.
pub(in crate::dbs) struct MemoryOrdered {
	/// Collected values
	values: Vec<Value>,
	/// Ordered index
	ordered: Vec<usize>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current batch of values to be merged once full
	batch: Vec<Value>,
	/// The finalized result
	result: Option<Vec<Value>>,
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
			result: None,
			orders,
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		if let Some(result) = &self.result {
			// If we have a finalized result, we return its size
			result.len()
		} else {
			// If we don't have a finalized result, we return the number of value summed with the current batch
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

	#[cfg(target_arch = "wasm32")]
	pub(super) fn sort(&mut self) {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch();
			}
			let mut ordered = mem::take(&mut self.ordered);
			let mut values = mem::take(&mut self.values);
			ordered.sort_unstable_by(|a, b| self.orders.compare(&values[*a], &values[*b]));
			let res = MemoryRandom::ordered_values_to_vec(&mut values, &ordered);
			self.result = Some(res);
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub(super) async fn sort(&mut self) -> Result<(), Error> {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch();
			}
			let mut ordered = mem::take(&mut self.ordered);
			let mut values = mem::take(&mut self.values);
			let orders = self.orders.clone();
			let result = spawn_blocking(move || {
				ordered.par_sort_unstable_by(|a, b| orders.compare(&values[*a], &values[*b]));
				MemoryRandom::ordered_values_to_vec(&mut values, &ordered)
			})
			.await
			.map_err(|e| Error::OrderingError(format!("{e}")))?;
			self.result = Some(result);
		}
		Ok(())
	}

	pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		if let Some(ref mut result) = self.result {
			MemoryCollector::vec_start_limit(start, limit, result);
		}
	}

	pub(in crate::dbs) fn take_vec(&mut self) -> Vec<Value> {
		self.result.take().unwrap_or_default()
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryOrdered", vec![]);
	}
}
