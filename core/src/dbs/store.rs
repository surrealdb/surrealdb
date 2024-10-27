use rand::seq::SliceRandom;

use crate::dbs::plan::Explanation;
#[cfg(not(target_arch = "wasm32"))]
use crate::err::Error;
use crate::sql::order::Ordering;
use crate::sql::value::Value;
use std::mem;
#[cfg(not(target_arch = "wasm32"))]
use tokio::task::spawn_blocking;

#[derive(Default)]
pub(super) struct MemoryCollector(Vec<Value>);

impl MemoryCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	#[cfg(target_arch = "wasm32")]
	pub(super) fn sort(&mut self, ordering: &Ordering) {
		Self::sorting(&mut self.0, ordering)
	}

	fn sorting(values: &mut Vec<Value>, ordering: &Ordering) {
		match ordering {
			Ordering::Random => values.shuffle(&mut rand::thread_rng()),
			Ordering::Order(orders) => {
				values.sort_unstable_by(|a, b| orders.compare(a, b));
			}
		}
	}

	#[cfg(not(target_arch = "wasm32"))]
	pub(super) async fn async_sort(&mut self, ordering: &Ordering) -> Result<(), Error> {
		let ordering = ordering.clone();
		let mut values = mem::take(&mut self.0);
		let values = spawn_blocking(move || {
			Self::sorting(&mut values, &ordering);
			values
		})
		.await
		.map_err(|e| Error::Internal(format!("{e}")))?;
		self.0 = values;
		Ok(())
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

#[cfg(storage)]
pub(super) mod file_store {
	use crate::cnf::EXTERNAL_SORTING_BUFFER_LIMIT;
	use crate::dbs::plan::Explanation;
	use crate::err::Error;
	#[cfg(not(target_arch = "wasm32"))]
	use crate::err::Error::OrderingError;
	use crate::sql::order::Ordering;
	use crate::sql::Value;
	use ext_sort::{ExternalChunk, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
	use rand::seq::SliceRandom as _;
	use revision::Revisioned;
	use std::fs::{File, OpenOptions};
	use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
	use std::path::{Path, PathBuf};
	use std::{fs, io, mem};
	use tempfile::{Builder, TempDir};
	#[cfg(not(target_arch = "wasm32"))]
	use tokio::task::spawn_blocking;

	pub(in crate::dbs) struct FileCollector {
		dir: TempDir,
		len: usize,
		writer: Option<FileWriter>,
		reader: Option<FileReader>,
		orders: Option<Ordering>,
		paging: FilePaging,
	}

	impl FileCollector {
		const INDEX_FILE_NAME: &'static str = "ix";
		const RECORDS_FILE_NAME: &'static str = "re";

		const SORT_DIRECTORY_NAME: &'static str = "so";

		const USIZE_SIZE: usize = mem::size_of::<usize>();

		pub(in crate::dbs) fn new(temp_dir: &Path) -> Result<Self, Error> {
			let dir = Builder::new().prefix("SURREAL").tempdir_in(temp_dir)?;
			Ok(Self {
				len: 0,
				writer: Some(FileWriter::new(&dir)?),
				reader: None,
				orders: None,
				paging: Default::default(),
				dir,
			})
		}
		pub(in crate::dbs) async fn push(&mut self, value: Value) -> Result<(), Error> {
			if let Some(mut writer) = self.writer.take() {
				#[cfg(not(target_arch = "wasm32"))]
				let writer = spawn_blocking(move || {
					writer.push(value)?;
					Ok::<FileWriter, Error>(writer)
				})
				.await
				.map_err(|e| Error::Internal(format!("{e}")))??;
				#[cfg(target_arch = "wasm32")]
				writer.push(value)?;
				self.len += 1;
				self.writer = Some(writer);
				Ok(())
			} else {
				Err(Error::Internal("No FileWriter available.".to_string()))
			}
		}

		fn check_reader(&mut self) -> Result<(), Error> {
			if self.reader.is_none() {
				if let Some(writer) = self.writer.take() {
					writer.flush()?;
					self.reader = Some(FileReader::new(self.len, &self.dir)?);
				}
			}
			Ok(())
		}
		pub(in crate::dbs) fn sort(&mut self, orders: &Ordering) {
			self.orders = Some(orders.clone());
		}

		pub(in crate::dbs) fn len(&self) -> usize {
			self.len
		}

		pub(in crate::dbs) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
			self.paging.start = start;
			self.paging.limit = limit;
		}

		pub(in crate::dbs) async fn take_vec(&mut self) -> Result<Vec<Value>, Error> {
			self.check_reader()?;
			if let Some(mut reader) = self.reader.take() {
				if let Some((start, num)) = self.paging.get_start_num(reader.len as u32) {
					if let Some(orders) = self.orders.take() {
						return self.sort_and_take_vec(reader, orders, start, num).await;
					}
					return reader.take_vec(start, num);
				}
			}
			Ok(vec![])
		}

		async fn sort_and_take_vec(
			&mut self,
			reader: FileReader,
			orders: Ordering,
			start: u32,
			num: u32,
		) -> Result<Vec<Value>, Error> {
			match orders {
				Ordering::Random => {
					let mut res: Vec<Value> = Vec::with_capacity(num as usize);
					#[cfg(not(target_arch = "wasm32"))]
					let res = spawn_blocking(move || {
						for r in reader.into_iter().skip(start as usize).take(num as usize) {
							res.push(r?);
						}
						res.shuffle(&mut rand::thread_rng());
						Ok::<_, Error>(res)
					})
					.await
					.map_err(|e| OrderingError(format!("{e}")))??;
					#[cfg(target_arch = "wasm32")]
					{
						for r in reader.into_iter().skip(start as usize).take(num as usize) {
							res.push(r?);
						}
						res.shuffle(&mut rand::thread_rng());
					}
					Ok(res)
				}
				Ordering::Order(orders) => {
					let sort_dir = self.dir.path().join(Self::SORT_DIRECTORY_NAME);
					let f = move || -> Result<Vec<Value>, Error> {
						fs::create_dir(&sort_dir)?;
						let sorter: ExternalSorter<
							Value,
							Error,
							LimitedBufferBuilder,
							ValueExternalChunk,
						> = ExternalSorterBuilder::new()
							.with_tmp_dir(&sort_dir)
							.with_buffer(LimitedBufferBuilder::new(
								*EXTERNAL_SORTING_BUFFER_LIMIT,
								true,
							))
							.build()?;

						let sorted = sorter.sort_by(reader, |a, b| orders.compare(a, b))?;
						let iter = sorted.map(Result::unwrap);
						let r: Vec<Value> = iter.skip(start as usize).take(num as usize).collect();
						Ok(r)
					};
					#[cfg(not(target_arch = "wasm32"))]
					let res = spawn_blocking(f).await.map_err(|e| OrderingError(format!("{e}")))??;
					#[cfg(target_arch = "wasm32")]
					let res = f()?;
					Ok(res)
				}
			}
		}

		pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
			exp.add_collector("TempFiles", vec![]);
		}
	}

	struct FileWriter {
		index: BufWriter<File>,
		records: BufWriter<File>,
		offset: usize,
	}

	impl FileWriter {
		fn new(dir: &TempDir) -> Result<Self, Error> {
			let index = OpenOptions::new()
				.create_new(true)
				.append(true)
				.open(dir.path().join(FileCollector::INDEX_FILE_NAME))?;
			let records = OpenOptions::new()
				.create_new(true)
				.append(true)
				.open(dir.path().join(FileCollector::RECORDS_FILE_NAME))?;
			Ok(Self {
				index: BufWriter::new(index),
				records: BufWriter::new(records),
				offset: 0,
			})
		}

		fn write_usize<W: Write>(writer: &mut W, u: usize) -> Result<(), Error> {
			let buf = u.to_be_bytes();
			writer.write_all(&buf)?;
			Ok(())
		}

		fn write_value<W: Write>(writer: &mut W, value: Value) -> Result<usize, Error> {
			let mut val = Vec::new();
			value.serialize_revisioned(&mut val)?;
			// Write the size of the buffer in the index
			Self::write_usize(writer, val.len())?;
			// Write the buffer in the records
			writer.write_all(&val)?;
			Ok(val.len())
		}

		fn push(&mut self, value: Value) -> Result<(), Error> {
			// Serialize the value in a buffer
			let len = Self::write_value(&mut self.records, value)?;
			// Increment the offset of the next record
			self.offset += len + FileCollector::USIZE_SIZE;
			Self::write_usize(&mut self.index, self.offset)?;
			Ok(())
		}

		fn flush(mut self) -> Result<(), Error> {
			self.records.flush()?;
			self.index.flush()?;
			Ok(())
		}
	}

	struct FileReader {
		len: usize,
		index: PathBuf,
		records: PathBuf,
	}

	impl FileReader {
		fn new(len: usize, dir: &TempDir) -> Result<Self, Error> {
			let index = dir.path().join(FileCollector::INDEX_FILE_NAME);
			let records = dir.path().join(FileCollector::RECORDS_FILE_NAME);
			Ok(Self {
				len,
				index,
				records,
			})
		}

		fn read_value<R: Read>(reader: &mut R) -> Result<Value, Error> {
			let len = FileReader::read_usize(reader)?;
			let mut buf = vec![0u8; len];
			if let Err(e) = reader.read_exact(&mut buf) {
				return Err(Error::Io(e));
			}
			let val = Value::deserialize_revisioned(&mut buf.as_slice())?;
			Ok(val)
		}

		fn read_usize<R: Read>(reader: &mut R) -> Result<usize, io::Error> {
			let mut buf = vec![0u8; FileCollector::USIZE_SIZE];
			reader.read_exact(&mut buf)?;
			// Safe to call unwrap because we know the slice length matches the expected length
			let u = usize::from_be_bytes(buf.try_into().unwrap());
			Ok(u)
		}

		fn take_vec(&mut self, start: u32, num: u32) -> Result<Vec<Value>, Error> {
			let mut iter = FileRecordsIterator::new(self.records.clone(), self.len);
			if start > 0 {
				// Get the start offset of the first record
				let mut index = OpenOptions::new().read(true).open(&self.index)?;
				index.seek(SeekFrom::Start(
					((start as usize - 1) * FileCollector::USIZE_SIZE) as u64,
				))?;
				let start_offset = Self::read_usize(&mut index)?;

				// Set records to the position of the first record
				iter.seek(start_offset, start as usize)?;
			}

			// Collect the records
			let mut res = Vec::with_capacity(num as usize);
			for _ in 0..num {
				if let Some(val) = iter.next() {
					res.push(val?);
				} else {
					break;
				}
			}
			Ok(res)
		}
	}

	impl IntoIterator for FileReader {
		type Item = Result<Value, Error>;
		type IntoIter = FileRecordsIterator;

		fn into_iter(self) -> Self::IntoIter {
			FileRecordsIterator::new(self.records.clone(), self.len)
		}
	}

	struct FileRecordsIterator {
		path: PathBuf,
		reader: Option<BufReader<File>>,
		len: usize,
		pos: usize,
	}

	impl FileRecordsIterator {
		fn new(path: PathBuf, len: usize) -> Self {
			Self {
				path,
				reader: None,
				len,
				pos: 0,
			}
		}

		fn check_reader(&mut self) -> Result<(), Error> {
			if self.reader.is_none() {
				let f = OpenOptions::new().read(true).open(&self.path)?;
				self.reader = Some(BufReader::new(f));
			}
			Ok(())
		}

		fn seek(&mut self, seek_pos: usize, pos: usize) -> Result<(), Error> {
			self.check_reader()?;
			if let Some(reader) = &mut self.reader {
				reader.seek(SeekFrom::Start(seek_pos as u64))?;
				self.pos = pos;
			}
			Ok(())
		}
	}

	impl Iterator for FileRecordsIterator {
		type Item = Result<Value, Error>;

		fn next(&mut self) -> Option<Self::Item> {
			if self.pos == self.len {
				return None;
			}
			if let Err(e) = self.check_reader() {
				return Some(Err(e));
			}
			if let Some(reader) = &mut self.reader {
				match FileReader::read_value(reader) {
					Ok(val) => {
						self.pos += 1;
						Some(Ok(val))
					}
					Err(e) => Some(Err(e)),
				}
			} else {
				None
			}
		}
	}

	#[derive(Default)]
	struct FilePaging {
		start: Option<u32>,
		limit: Option<u32>,
	}

	impl FilePaging {
		fn get_start_num(&self, len: u32) -> Option<(u32, u32)> {
			let start = self.start.unwrap_or(0);
			if start >= len {
				return None;
			}
			let max = len - start;
			let num = if let Some(limit) = self.limit {
				limit.min(max)
			} else {
				max
			};
			Some((start, num))
		}
	}

	struct ValueExternalChunk {
		reader: Take<BufReader<File>>,
	}

	impl ExternalChunk<Value> for ValueExternalChunk {
		type SerializationError = Error;
		type DeserializationError = Error;

		fn new(reader: Take<BufReader<File>>) -> Self {
			Self {
				reader,
			}
		}

		fn dump(
			chunk_writer: &mut BufWriter<File>,
			items: impl IntoIterator<Item = Value>,
		) -> Result<(), Self::SerializationError> {
			for item in items {
				FileWriter::write_value(chunk_writer, item)?;
			}
			Ok(())
		}
	}

	impl Iterator for ValueExternalChunk {
		type Item = Result<Value, Error>;

		fn next(&mut self) -> Option<Self::Item> {
			if self.reader.limit() == 0 {
				None
			} else {
				match FileReader::read_value(&mut self.reader) {
					Ok(val) => Some(Ok(val)),
					Err(err) => Some(Err(err)),
				}
			}
		}
	}
}

#[cfg(not(target_arch = "wasm32"))]
pub(super) mod memory_ordered {
	use crate::dbs::plan::Explanation;
	use crate::dbs::store::MemoryCollector;
	use crate::err::Error;
	use crate::sql::order::{OrderList, Ordering};
	use crate::sql::Value;
	use rand::prelude::SliceRandom;
	use rand::{thread_rng, Rng};
	use std::{cmp, mem};
	use tokio::sync::mpsc;
	use tokio::sync::mpsc::{Receiver, Sender};
	use tokio::task::JoinHandle;

	const CHANNEL_BUFFER_SIZE: usize = 128;
	const DEFAULT_BATCH_MAX_SIZE: usize = 1024;

	struct OrderedResult {
		values: Vec<Value>,
		ordered: Vec<usize>,
	}

	impl OrderedResult {
		fn with_capacity(capacity: usize) -> Self {
			Self {
				values: vec![], // The first batch is set using `*values = batch`, so we don't need any capacity here
				ordered: Vec::with_capacity(capacity),
			}
		}
		fn into_vec(mut self) -> Vec<Value> {
			let mut vec = Vec::with_capacity(self.ordered.len());
			for idx in self.ordered {
				vec.push(mem::take(&mut self.values[idx]));
			}
			vec
		}
	}

	/// The struct MemoryOrdered represents an in-memory store that aggregates data in batches,
	/// ordering the data, and allows for pushing the data asynchronously.
	pub(in crate::dbs) struct MemoryOrdered {
		/// Sender-side of asynchronous channel to send batches
		tx: Option<Sender<Vec<Value>>>,
		/// Handle for the merge task that processes incoming batches
		rx: Option<JoinHandle<OrderedResult>>,
		/// Current batch of values to be merged once full
		batch: Vec<Value>,
		/// Vector containing ordered values
		result: Vec<Value>,
		/// The maximum size of a batch
		batch_size: usize,
	}

	impl MemoryOrdered {
		pub(in crate::dbs) fn new(ordering: &Ordering, batch_size: Option<usize>) -> Self {
			let (tx, rx) = mpsc::channel(CHANNEL_BUFFER_SIZE);
			let batch_size = batch_size.unwrap_or(DEFAULT_BATCH_MAX_SIZE);
			let result = OrderedResult::with_capacity(batch_size);
			// Spawns a merge task to process and merge incoming batches asynchronously.
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
				result: vec![],
			}
		}

		async fn merge_sort_task(
			mut result: OrderedResult,
			mut rx: Receiver<Vec<Value>>,
			orders: OrderList,
		) -> OrderedResult {
			while let Some(batch) = rx.recv().await {
				Self::incremental_sorted_insertion(&mut result, batch, |a, b| orders.compare(a, b));
			}
			result
		}

		async fn merge_random_task(
			mut result: OrderedResult,
			mut rx: Receiver<Vec<Value>>,
		) -> OrderedResult {
			while let Some(batch) = rx.recv().await {
				Self::incremental_random_insertion(&mut result, batch);
			}
			result
		}

		fn incremental_sorted_insertion<F>(
			result: &mut OrderedResult,
			mut batch: Vec<Value>,
			cmp: F,
		) where
			F: Fn(&Value, &Value) -> cmp::Ordering,
		{
			// Ensure the batch is sorted
			batch.sort_unstable();
			let batch_len = batch.len();

			// If merged is empty we just move the batch,
			if result.values.is_empty() {
				result.values = batch;
				// This is the fastest way or inserting a range inside a vector
				result.ordered = Vec::with_capacity(batch_len);
				result.ordered.extend(0..batch_len);
				return;
			}

			// Reserve capacity in the merged vector
			result.values.extend(batch);
			result.ordered.reserve(batch_len);

			let mut start_idx = 0;
			let start = result.ordered.len();
			let end = start + batch_len;

			// Iterator over the new values that must be ordered
			for idx in start..end {
				let val = &result.values[idx];
				// Perform binary search between start_idx and merged.len()
				// As the batch is sorted, when a value is inserted,
				// we know that the next value will be inserted after.
				// Therefore, we can reduce the scope of the next binary search.
				let insert_pos = result.ordered[start_idx..]
					.binary_search_by(|a| cmp(&result.values[*a], val))
					.map(|pos| start_idx + pos)
					.unwrap_or_else(|pos| start_idx + pos);

				// Insert the element at the found position
				result.ordered.insert(insert_pos, idx);

				// Update start_idx for the next iteration
				start_idx = insert_pos + 1; // +1 because we just inserted an element
			}
		}

		fn incremental_random_insertion(result: &mut OrderedResult, batch: Vec<Value>) {
			let mut rng = thread_rng();

			let batch_len = batch.len();

			if result.ordered.is_empty() {
				result.values = batch;
				// This is the fastest way or inserting a range inside a vector
				result.ordered = Vec::with_capacity(batch_len);
				result.ordered.extend(0..batch_len);
				// Then we just shuffle the order
				result.ordered.shuffle(&mut rng);
				return;
			}

			// Add the values
			result.values.extend(batch);
			// Reserve capacity in the merged vector
			result.ordered.reserve(batch_len);
			let start = result.ordered.len();
			let end = start + batch_len;

			// Fisher-Yates shuffle to shuffle the elements as they are merged
			for idx in start..end {
				result.ordered.push(idx);
				let i = result.ordered.len() - 1;
				let j = rng.gen_range(0..=i);
				result.ordered.swap(i, j);
			}
		}

		pub(in crate::dbs) async fn push(&mut self, val: Value) -> Result<(), Error> {
			self.batch.push(val);
			if self.batch.len() >= self.batch_size {
				self.send_buffer().await?;
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

		async fn send_buffer(&mut self) -> Result<(), Error> {
			let batch = mem::replace(&mut self.batch, Vec::with_capacity(self.batch_size));
			self.tx()?.send(batch).await.map_err(|e| Error::Internal(format!("{e}")))?;
			Ok(())
		}

		pub(in crate::dbs) fn len(&self) -> usize {
			self.result.len()
		}

		async fn finalize(&mut self) -> Result<(), Error> {
			if !self.batch.is_empty() {
				self.send_buffer().await?;
			}
			if let Some(tx) = self.tx.take() {
				drop(tx);
			}
			if let Some(rx) = self.rx.take() {
				let result = rx.await.map_err(|e| Error::Internal(format!("{e}")))?;
				self.result = result.into_vec();
			}
			Ok(())
		}

		pub(in crate::dbs) async fn start_limit(
			&mut self,
			start: Option<u32>,
			limit: Option<u32>,
		) -> Result<(), Error> {
			self.finalize().await?;
			MemoryCollector::vec_start_limit(start, limit, &mut self.result);
			Ok(())
		}

		pub(in crate::dbs) async fn take_vec(&mut self) -> Result<Vec<Value>, Error> {
			self.finalize().await?;
			Ok(mem::take(&mut self.result))
		}

		pub(in crate::dbs) fn explain(&self, exp: &mut Explanation) {
			exp.add_collector("MemoryOrdered", vec![]);
		}
	}

	#[cfg(test)]
	mod test {
		use crate::dbs::store::memory_ordered::{MemoryOrdered, OrderedResult};
		use crate::sql::Value;

		impl From<Vec<Value>> for OrderedResult {
			fn from(values: Vec<Value>) -> Self {
				Self {
					ordered: (0..values.len()).collect(),
					values,
				}
			}
		}
		#[test]
		fn incremental_sorted_insertion_test() {
			let test = |merged: Vec<Value>, batch: Vec<Value>, expected: Vec<Value>| {
				let mut result = merged.into();
				MemoryOrdered::incremental_sorted_insertion(&mut result, batch, Value::cmp);
				assert_eq!(result.into_vec(), expected);
			};
			// All empty
			test(vec![], vec![], vec![]);
			// Merged empty
			test(vec![], vec![2.into(), 1.into()], vec![1.into(), 2.into()]);
			// Batch empty
			test(vec![1.into(), 2.into()], vec![], vec![1.into(), 2.into()]);
			// Batch before
			test(
				vec![3.into(), 4.into()],
				vec![2.into(), 1.into()],
				vec![1.into(), 2.into(), 3.into(), 4.into()],
			);
			// Batch after
			test(
				vec![3.into(), 4.into()],
				vec![6.into(), 5.into()],
				vec![3.into(), 4.into(), 5.into(), 6.into()],
			);
			// Batch interlaced
			test(
				vec![2.into(), 4.into()],
				vec![5.into(), 1.into(), 3.into()],
				vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into()],
			);
			// Batch interlaced with duplicates
			test(
				vec![2.into(), 4.into(), 4.into()],
				vec![3.into(), 2.into(), 5.into(), 3.into(), 1.into()],
				vec![
					1.into(),
					2.into(),
					2.into(),
					3.into(),
					3.into(),
					4.into(),
					4.into(),
					5.into(),
				],
			);
		}
	}

	#[test]
	fn incremental_random_insertion_test() {
		let test = |merged: Vec<Value>, batch: Vec<Value>, expected: Vec<Value>| {
			let mut result = merged.into();
			MemoryOrdered::incremental_random_insertion(&mut result, batch);
			let result = result.into_vec();
			for v in expected {
				assert!(result.contains(&v));
			}
		};
		// All empty
		test(vec![], vec![], vec![]);
		// Merged empty
		test(vec![], vec![2.into(), 1.into()], vec![1.into(), 2.into()]);
		// Batch empty
		test(vec![1.into(), 2.into()], vec![], vec![1.into(), 2.into()]);
		// Normal batch
		test(
			vec![3.into(), 4.into()],
			vec![2.into(), 1.into()],
			vec![1.into(), 2.into(), 3.into(), 4.into()],
		);
	}

	#[test]
	fn incremental_sorted_insertions() {
		let mut result = OrderedResult::with_capacity(2);
		MemoryOrdered::incremental_sorted_insertion(
			&mut result,
			vec![2.into(), 4.into()],
			Value::cmp,
		);
		MemoryOrdered::incremental_sorted_insertion(
			&mut result,
			vec![5.into(), 3.into()],
			Value::cmp,
		);
		MemoryOrdered::incremental_sorted_insertion(
			&mut result,
			vec![6.into(), 1.into()],
			Value::cmp,
		);
		let result = result.into_vec();
		assert_eq!(result, vec![1.into(), 2.into(), 3.into(), 4.into(), 5.into(), 6.into()]);
	}
}
