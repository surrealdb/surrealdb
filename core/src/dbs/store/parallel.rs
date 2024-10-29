use crate::dbs::plan::Explanation;
use crate::dbs::store::{MemoryCollector, OrderedResult, DEFAULT_BATCH_MAX_SIZE};
use crate::err::Error;
use crate::sql::order::{OrderList, Ordering};
use crate::sql::Value;
use std::{cmp, mem};
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

const CHANNEL_BUFFER_SIZE: usize = 128;

impl OrderedResult {
	fn add_sorted_batch<F>(&mut self, mut batch: Vec<Value>, cmp: F)
	where
		F: Fn(&Value, &Value) -> cmp::Ordering,
	{
		// Ensure the batch is sorted
		batch.sort_unstable_by(|a, b| cmp(a, b));
		let batch_len = batch.len();

		// If merged is empty, we just move the batch,
		if self.values.is_empty() {
			self.values = batch;
			// This is the fastest way or inserting a range inside a vector
			self.ordered = Vec::with_capacity(batch_len);
			self.ordered.extend(0..batch_len);
			assert_eq!(self.ordered.len(), self.values.len());
			return;
		}

		// Reserve capacity in the merged vector
		self.values.extend(batch);
		self.ordered.reserve(batch_len);

		let mut start_idx = 0;
		let start = self.ordered.len();
		let end = start + batch_len;

		// Iterator over the new values that must be ordered
		for idx in start..end {
			let val = &self.values[idx];
			// Perform binary search between start_idx and merged.len()
			// As the batch is sorted, when a value is inserted,
			// we know that the next value will be inserted after.
			// Therefore, we can reduce the scope of the next binary search.
			let insert_pos = self.ordered[start_idx..]
				.binary_search_by(|a| cmp(&self.values[*a], val))
				.map(|pos| start_idx + pos)
				.unwrap_or_else(|pos| start_idx + pos);

			// Insert the element at the found position
			self.ordered.insert(insert_pos, idx);

			// Update start_idx for the next iteration
			start_idx = insert_pos + 1; // +1 because we just inserted an element
		}
	}
}
#[cfg(not(target_arch = "wasm32"))]
pub(in crate::dbs) struct AsyncMemoryOrdered {
	/// Sender-side of an asynchronous channel to send batches
	tx: Option<Sender<Vec<Value>>>,
	/// Handle for the merge task that processes incoming batches
	rx: Option<JoinHandle<OrderedResult>>,
	/// Current batch of values to be merged once full
	batch: Vec<Value>,
	/// Vector containing ordered values after finalization
	result: Option<Vec<Value>>,
	/// The maximum size of a batch
	batch_size: usize,
	/// Current len
	len: usize,
}

impl AsyncMemoryOrdered {
	pub(in crate::dbs) fn new(ordering: &Ordering, batch_size: Option<usize>) -> Self {
		let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
		let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_MAX_SIZE);
		let result = OrderedResult::with_capacity(batch_size);
		// Spawns a merge task to process and merge incoming batches asynchronously.finalize
		let rx = match ordering {
			Ordering::Random => tokio::spawn(Self::merge_random_task(result, rx)),
			Ordering::Order(orders) => {
				tokio::spawn(Self::merge_sort_task(result, rx, orders.clone()))
			}
		};
		Self {
			tx: Some(tx),
			rx: Some(rx),
			batch_size,
			batch: Vec::with_capacity(batch_size),
			result: None,
			len: 0,
		}
	}

	async fn merge_sort_task(
		mut result: OrderedResult,
		mut rx: Receiver<Vec<Value>>,
		orders: OrderList,
	) -> OrderedResult {
		while let Some(batch) = rx.recv().await {
			result.add_sorted_batch(batch, |a, b| orders.compare(a, b));
		}
		result
	}

	async fn merge_random_task(
		mut result: OrderedResult,
		mut rx: Receiver<Vec<Value>>,
	) -> OrderedResult {
		while let Some(batch) = rx.recv().await {
			result.add_random_batch(batch);
		}
		result
	}

	pub(in crate::dbs) fn len(&self) -> usize {
		if let Some(result) = &self.result {
			result.len()
		} else {
			self.len
		}
	}

	pub(in crate::dbs) async fn push(&mut self, val: Value) -> Result<(), Error> {
		self.batch.push(val);
		self.len += 1;
		if self.batch.len() >= self.batch_size {
			self.send_batch().await?;
		}
		Ok(())
	}

	fn tx(&self) -> Result<&Sender<Vec<Value>>, Error> {
		if let Some(tx) = &self.tx {
			Ok(tx)
		} else {
			Err(Error::Internal("No channel".to_string()))
		}
	}

	async fn send_batch(&mut self) -> Result<(), Error> {
		let batch = mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size));
		self.tx()?.send(batch).await.map_err(|e| Error::Internal(format!("{e}")))?;
		Ok(())
	}

	pub(in crate::dbs) async fn finalize(&mut self) -> Result<(), Error> {
		if self.result.is_none() {
			if !self.batch.is_empty() {
				self.send_batch().await?;
			}
			if let Some(tx) = self.tx.take() {
				drop(tx);
			}
			if let Some(rx) = self.rx.take() {
				let result = rx.await.map_err(|e| Error::Internal(format!("{e}")))?;
				self.result = Some(result.into_vec());
			}
		}
		Ok(())
	}

	pub(in crate::dbs) async fn start_limit(
		&mut self,
		start: Option<u32>,
		limit: Option<u32>,
	) -> Result<(), Error> {
		self.finalize().await?;
		if let Some(ref mut result) = self.result {
			MemoryCollector::vec_start_limit(start, limit, result);
		}
		Ok(())
	}

	pub(in crate::dbs) async fn take_vec(&mut self) -> Result<Vec<Value>, Error> {
		self.finalize().await?;
		Ok(self.result.take().unwrap_or_default())
	}

	pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("AsyncMemoryOrdered", vec![]);
	}
}
