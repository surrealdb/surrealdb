#[cfg(storage)]
pub(super) mod file;
#[cfg(not(target_arch = "wasm32"))]
pub(super) mod parallel;

use crate::dbs::plan::Explanation;
#[cfg(not(target_arch = "wasm32"))]
use crate::err::Error;
use crate::sql::order::Ordering;
use crate::sql::value::Value;

#[cfg(not(target_arch = "wasm32"))]
use crate::dbs::spawn_blocking;
use rand::seq::SliceRandom;
use rand::{thread_rng, Rng};
#[cfg(not(target_arch = "wasm32"))]
use rayon::prelude::ParallelSliceMut;
use std::mem;

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

pub(super) const DEFAULT_BATCH_MAX_SIZE: usize = 4096;

pub(super) struct OrderedResult {
	values: Vec<Value>,
	ordered: Vec<usize>,
}

impl OrderedResult {
	fn with_capacity(capacity: usize) -> Self {
		Self {
			values: vec![], // The first batch is set with `self.values = batch`, so we don't need any capacity here
			ordered: Vec::with_capacity(capacity),
		}
	}

	fn into_vec(mut self) -> Vec<Value> {
		let mut vec = Vec::with_capacity(self.values.len());
		for idx in self.ordered {
			vec.push(mem::take(&mut self.values[idx]));
		}
		vec
	}

	fn add_batch(&mut self, batch: Vec<Value>) {
		let pos = self.ordered.len();
		self.ordered.extend(pos..(pos + batch.len()));
		self.values.extend(batch);
	}
	fn add_random_batch(&mut self, batch: Vec<Value>) {
		let mut rng = thread_rng();

		let batch_len = batch.len();

		if self.values.is_empty() {
			self.values.extend(batch);
			// This is the fastest way or inserting a range inside a vector
			self.ordered = Vec::with_capacity(batch_len);
			self.ordered.extend(0..batch_len);
			// Then we just shuffle the order
			self.ordered.shuffle(&mut rng);
			return;
		}

		// Add the values
		self.values.extend(batch);
		// Reserve capacity in the merged vector
		self.ordered.reserve(batch_len);
		let start = self.ordered.len();
		let end = start + batch_len;

		// Fisher-Yates shuffle to shuffle the elements as they are merged
		for idx in start..end {
			self.ordered.push(idx);
			let i = self.ordered.len() - 1;
			let j = rng.gen_range(0..=i);
			self.ordered.swap(i, j);
		}
	}
}

/// The struct MemoryOrdered represents an in-memory store that aggregates data in batches.
pub(in crate::dbs) struct MemoryOrdered {
	/// true if it is Ordering::Random
	is_random: bool,
	/// Current batch of values to be merged once full
	batch: Vec<Value>,
	/// Ordered values
	ordered: OrderedResult,
	/// The maximum size of a batch
	batch_size: usize,
	/// The finalized result
	result: Option<Vec<Value>>,
}

impl MemoryOrdered {
	pub(in crate::dbs) fn new(ordering: &Ordering, batch_size: Option<usize>) -> Self {
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_MAX_SIZE);
		Self {
			is_random: matches!(ordering, Ordering::Random),
			batch_size,
			batch: Vec::with_capacity(batch_size),
			ordered: OrderedResult::with_capacity(batch_size),
			result: None,
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		if let Some(result) = &self.result {
			// If we have a finalized result, we return its size
			result.len()
		} else {
			// If we don't have a finalized result, we return the current size
			self.ordered.values.len() + self.batch.len()
		}
	}

	pub(in crate::dbs) fn push(&mut self, val: Value) {
		self.batch.push(val);
		if self.batch.len() >= self.batch_size {
			self.send_batch();
		}
	}

	fn send_batch(&mut self) {
		let batch = mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size));
		if self.is_random {
			self.ordered.add_random_batch(batch);
		} else {
			self.ordered.add_batch(batch);
		}
	}

	#[cfg(target_arch = "wasm32")]
	pub(super) fn sort(&mut self, ordering: &Ordering) {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch();
			}
			let mut to_sort = mem::replace(&mut self.ordered, OrderedResult::with_capacity(0));
			if let Ordering::Order(o) = ordering {
				to_sort
					.ordered
					.sort_unstable_by(|a, b| o.compare(&to_sort.values[*a], &to_sort.values[*b]));
			}
			self.result = Some(to_sort.into_vec());
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub(super) async fn sort(&mut self, ordering: &Ordering) -> Result<(), Error> {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch();
			}
			let mut to_sort = mem::replace(&mut self.ordered, OrderedResult::with_capacity(0));
			let ordering = ordering.clone();
			let result = spawn_blocking(
				move || {
					if let Ordering::Order(o) = ordering {
						to_sort.ordered.par_sort_unstable_by(|a, b| {
							o.compare(&to_sort.values[*a], &to_sort.values[*b])
						});
					}
					Ok(to_sort.into_vec())
				},
				|e| Error::OrderingError(format!("{e}")),
			)
			.await?;
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
