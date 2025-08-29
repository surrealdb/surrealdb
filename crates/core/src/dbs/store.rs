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
use crate::val::record::Record;

#[derive(Default)]
pub(super) struct MemoryCollector(Vec<Record>);

impl MemoryCollector {
	pub(super) fn push(&mut self, record: Record) {
		self.0.push(record);
	}

	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	fn vec_start_limit(start: Option<u32>, limit: Option<u32>, vec: &mut Vec<Record>) {
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

	pub(super) fn take_vec(&mut self) -> Vec<Record> {
		mem::take(&mut self.0)
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("Memory", vec![]);
	}
}

impl From<Vec<Record>> for MemoryCollector {
	fn from(records: Vec<Record>) -> Self {
		Self(records)
	}
}

pub(super) const DEFAULT_BATCH_SIZE: usize = 1024;

/// The struct MemoryRandom represents an in-memory store that aggregates data
/// randomly.
pub(in crate::dbs) struct MemoryRandom {
	/// Collected records
	records: Vec<Record>,
	/// Ordered index
	ordered: Vec<usize>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current batch of records to be merged once full
	batch: Vec<Record>,
	/// The finalized result
	result: Option<Vec<Record>>,
}

impl MemoryRandom {
	pub(in crate::dbs) fn new(batch_size: Option<usize>) -> Self {
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
		Self {
			batch_size,
			records: Vec::new(),
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
			self.records.len() + self.batch.len()
		}
	}

	fn shuffle_batch(records: &mut Vec<Record>, ordered: &mut Vec<usize>, batch: Vec<Record>) {
		let mut rng = thread_rng();

		// Compute the index range of this new batch
		let start = ordered.len();
		let end = start + batch.len();

		// We extend the ordered indexes
		ordered.extend(start..end);

		// We add the batch to the value vector
		records.extend(batch);

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
		Self::shuffle_batch(&mut self.records, &mut self.ordered, batch);
	}

	pub(in crate::dbs) fn push(&mut self, record: Record) {
		// Add the value to the current batch
		self.batch.push(record);
		if self.batch_size == self.batch.len() {
			// The batch is full, we can add it to the main vectors
			self.send_batch();
		}
	}

	fn ordered_records_to_vec(records: &mut [Record], ordered: &[usize]) -> Vec<Record> {
		let mut vec = Vec::with_capacity(records.len());
		for idx in ordered {
			vec.push(mem::take(&mut records[*idx]));
		}
		vec
	}

	pub(in crate::dbs) fn sort(&mut self) {
		// Make sure there is no pending batch
		if !self.batch.is_empty() {
			self.send_batch();
		}
		// We build final sorted vector
		let res = Self::ordered_records_to_vec(&mut self.records, &self.ordered);
		self.result = Some(res);
	}

	pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		if let Some(ref mut result) = self.result {
			MemoryCollector::vec_start_limit(start, limit, result);
		}
	}

	pub(in crate::dbs) fn take_vec(&mut self) -> Vec<Record> {
		self.result.take().unwrap_or_default()
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryRandom", vec![]);
	}
}

/// The struct MemoryOrdered represents an in-memory store that aggregates
/// ordered data.
pub(in crate::dbs) struct MemoryOrdered {
	/// Collected records
	records: Vec<Record>,
	/// Ordered index
	ordered: Vec<usize>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current batch of records to be merged once full
	batch: Vec<Record>,
	/// The finalized result
	result: Option<Vec<Record>>,
	/// The order specification
	orders: OrderList,
}

impl MemoryOrdered {
	pub(in crate::dbs) fn new(orders: OrderList, batch_size: Option<usize>) -> Self {
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
		Self {
			batch_size,
			records: Vec::new(),
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
			// If we don't have a finalized result, we return the amount of records summed
			// with the current batch
			self.records.len() + self.batch.len()
		}
	}

	fn send_batch(&mut self) {
		self.records.append(&mut self.batch);
		self.ordered.extend(self.ordered.len()..self.records.len());
	}

	pub(in crate::dbs) fn push(&mut self, record: Record) {
		// Add the record to the current batch
		self.batch.push(record);
		if self.batch_size == self.batch.len() {
			// The batch is full, we can add it to the main vectors
			self.send_batch();
		}
	}

	#[cfg(target_family = "wasm")]
	pub(super) fn sort(&mut self) {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch();
			}
			let mut ordered = mem::take(&mut self.ordered);
			let mut records = mem::take(&mut self.records);
			ordered.sort_unstable_by(|a, b| self.orders.compare(&records[*a], &records[*b]));
			let res = MemoryRandom::ordered_records_to_vec(&mut records, &ordered);
			self.result = Some(res);
		}
	}

	#[cfg(not(target_family = "wasm"))]
	pub(super) async fn sort(&mut self) -> Result<(), Error> {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch();
			}
			let mut ordered = mem::take(&mut self.ordered);
			let mut records = mem::take(&mut self.records);
			let orders = self.orders.clone();
			let result = spawn_blocking(move || {
				ordered.par_sort_unstable_by(|a, b| orders.compare(&records[*a], &records[*b]));
				MemoryRandom::ordered_records_to_vec(&mut records, &ordered)
			})
			.await
			.map_err(|e| Error::OrderingError(format!("{e}")))?;
			self.result = Some(result);
		}
		Ok(())
	}

	pub(super) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		if let Some(ref mut result) = self.result {
			MemoryCollector::vec_start_limit(start, limit, result);
		}
	}

	pub(super) fn take_vec(&mut self) -> Vec<Record> {
		self.result.take().unwrap_or_default()
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryOrdered", vec![]);
	}
}

pub(super) struct OrderedRecord {
	record: Record,
	orders: Arc<OrderList>,
}
impl PartialOrd<Self> for OrderedRecord {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Eq for OrderedRecord {}

impl Ord for OrderedRecord {
	fn cmp(&self, other: &Self) -> Ordering {
		self.orders.compare(&self.record, &other.record)
	}
}

impl PartialEq<Self> for OrderedRecord {
	fn eq(&self, other: &Self) -> bool {
		self.record.eq(&other.record)
	}
}

pub(super) struct MemoryOrderedLimit {
	/// The priority list
	heap: BinaryHeap<Reverse<OrderedRecord>>,
	/// The maximum size of the priority list
	limit: usize,
	/// The order specification
	orders: Arc<OrderList>,
	/// The finalized result
	result: Option<Vec<Record>>,
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

	pub(in crate::dbs) fn push(&mut self, record: Record) {
		if self.heap.len() >= self.limit {
			// When the heap is full, first check if the new record
			// if smaller that the top of this min-heap in order to
			// prevent unnecessary push/pop and Arc::clone.
			if let Some(top) = self.heap.peek() {
				let cmp = self.orders.compare(&record, &top.0.record);
				if cmp == Ordering::Less {
					self.heap.push(Reverse(OrderedRecord {
						record,
						orders: self.orders.clone(),
					}));
					self.heap.pop();
				}
			}
		} else {
			// Push the value onto the heap because it's not full.
			self.heap.push(Reverse(OrderedRecord {
				record,
				orders: self.orders.clone(),
			}));
		}
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		self.heap.len()
	}

	pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		if let Some(ref mut result) = self.result {
			MemoryCollector::vec_start_limit(start, limit, result);
		}
	}

	pub(super) fn sort(&mut self) {
		if self.result.is_none() {
			let mut sorted_vec: Vec<_> = Vec::with_capacity(self.heap.len());
			while let Some(i) = self.heap.pop() {
				sorted_vec.push(i.0.record);
			}
			sorted_vec.reverse();
			self.result = Some(sorted_vec);
		}
	}

	pub(in crate::dbs) fn take_vec(&mut self) -> Vec<Record> {
		self.result.take().unwrap_or_default()
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("MemoryOrderedLimit", vec![("limit", self.limit.into())]);
	}
}
