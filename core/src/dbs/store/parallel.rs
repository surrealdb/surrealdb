use crate::dbs::plan::Explanation;
use crate::dbs::spawn_blocking;
use crate::dbs::store::llrbtree::LLRBTree;
use crate::dbs::store::{MemoryCollector, OrderedResult, DEFAULT_BATCH_MAX_SIZE};
use crate::err::Error;
use crate::sql::order::{OrderList, Ordering};
use crate::sql::Value;
use std::mem;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task::JoinHandle;

const CHANNEL_BUFFER_SIZE: usize = 128;

pub(in crate::dbs) struct AsyncMemoryOrdered {
	/// Sender-side of an asynchronous channel to send batches
	tx: Option<Sender<Vec<Value>>>,
	/// Handle for the merge task that processes incoming batches
	rx: Option<JoinHandle<Result<Vec<Value>, Error>>>,
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
		// Spawns a merge task to process and merge incoming batches asynchronously.finalize
		let rx = match ordering {
			Ordering::Random => tokio::spawn(Self::batch_random_task(batch_size, rx)),
			Ordering::Order(orders) => tokio::spawn(Self::batch_ordered_task(rx, orders.clone())),
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

	async fn batch_random_task(
		batch_size: usize,
		mut rx: Receiver<Vec<Value>>,
	) -> Result<Vec<Value>, Error> {
		let mut result = OrderedResult::with_capacity(batch_size);
		while let Some(batch) = rx.recv().await {
			result.add_random_batch(batch);
		}
		spawn_blocking(|| Ok(result.into_vec()), |e| Error::OrderingError(format!("{e}"))).await
	}

	async fn batch_ordered_task(
		mut rx: Receiver<Vec<Value>>,
		orders: OrderList,
	) -> Result<Vec<Value>, Error> {
		let values = Vec::new();
		let mut tree = LLRBTree::new();
		let mut values = values;
		while let Some(batch) = rx.recv().await {
			let new_idx = values.len()..(values.len() + batch.len());
			values.extend(batch);
			for idx in new_idx {
				tree.insert(idx, idx, |a, b| orders.compare(&values[a], &values[b]));
			}
		}
		let iter = tree.into_iter();
		spawn_blocking(
			move || {
				let vec = iter.map(|(_, v)| mem::take(&mut values[v])).collect();
				Ok(vec)
			},
			|e| Error::OrderingError(format!("{e}")),
		)
		.await
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
				self.result = Some(result?);
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
