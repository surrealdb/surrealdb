use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
use std::path::{Path, PathBuf};
use std::{fs, io, mem};

use anyhow::Result;
use ext_sort::{ExternalChunk, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
use rand::Rng as _;
use rand::seq::SliceRandom as _;
use revision::{DeserializeRevisioned, SerializeRevisioned};
use tempfile::{Builder, TempDir};
#[cfg(not(target_family = "wasm"))]
use tokio::task::spawn_blocking;

use crate::dbs::plan::Explanation;
use crate::err::Error;
use crate::expr::order::Ordering;
use crate::val::Value;

pub(super) struct FileCollector {
	dir: TempDir,
	len: usize,
	writer: Option<FileWriter>,
	reader: Option<FileReader>,
	orders: Option<Ordering>,
	paging: FilePaging,
	buffer_limit: usize,
}

impl FileCollector {
	const INDEX_FILE_NAME: &'static str = "ix";
	const RECORDS_FILE_NAME: &'static str = "re";

	const SORT_DIRECTORY_NAME: &'static str = "so";

	const USIZE_SIZE: usize = mem::size_of::<usize>();

	pub(super) fn new(
		temp_dir: &Path,
		orders: Option<Ordering>,
		buffer_limit: usize,
	) -> Result<Self, Error> {
		let dir = Builder::new().prefix("SURREAL").tempdir_in(temp_dir)?;
		Ok(Self {
			len: 0,
			writer: Some(FileWriter::new(&dir)?),
			reader: None,
			orders,
			paging: Default::default(),
			dir,
			buffer_limit,
		})
	}
	pub(super) async fn push(&mut self, value: Value) -> Result<(), Error> {
		if let Some(mut writer) = self.writer.take() {
			#[cfg(not(target_family = "wasm"))]
			let writer = spawn_blocking(move || {
				writer.push(&value)?;
				Ok::<FileWriter, Error>(writer)
			})
			.await
			.map_err(|e| Error::Internal(format!("{e}")))??;
			#[cfg(target_family = "wasm")]
			writer.push(&value)?;
			self.len += 1;
			self.writer = Some(writer);
			Ok(())
		} else {
			Err(Error::Internal("No FileWriter available.".to_string()))
		}
	}

	fn check_reader(&mut self) -> Result<(), Error> {
		if self.reader.is_none()
			&& let Some(writer) = self.writer.take()
		{
			writer.flush()?;
			self.reader = Some(FileReader::new(self.len, &self.dir)?);
		}
		Ok(())
	}

	pub(super) fn len(&self) -> usize {
		self.len
	}

	pub(super) fn start_limit(&mut self, start: Option<u32>, limit: Option<u32>) {
		self.paging.start = start;
		self.paging.limit = limit;
	}

	pub(super) async fn take_vec(&mut self) -> Result<Vec<Value>, Error> {
		self.check_reader()?;
		if let Some(mut reader) = self.reader.take()
			&& let Some((start, num)) = self.paging.get_start_num(reader.len as u32)
		{
			if let Some(orders) = self.orders.take() {
				return self.sort_and_take_vec(reader, orders, start, num).await;
			}
			return reader.take_vec(start, num);
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
				let f = move || {
					let mut rng = rand::rng();
					let mut iter = reader.into_iter();
					// fill initial array
					let mut res: Vec<Value> = Vec::with_capacity(num as usize);
					for r in iter.by_ref().take(num as usize) {
						res.push(r?);
					}

					// Then handle the remaining values as they might need to be part of the random
					// sampling. This is Vitter's Algorithm R for reservoir sampling — the same
					// algorithm used by `IteratorRandom::choose_multiple`.
					//
					// Correctness: let `n = num` and let `N` be the total length of the
					// stream. We use 1-based indexing for "the k-th item seen so far".
					// The loop maintains the invariant that, immediately after seeing the
					// k-th item, every item seen so far is in the reservoir with
					// probability `n/k` (with probability `1` while `k <= n`). The fill
					// loop above establishes the base case at `k = n` (probability `1`).
					//
					// In the loop below, `i` is the zero-based offset *into the remaining
					// iterator*, so when we process this item we have just seen the
					// `(i + n + 1)`-th item overall. Call that `k+1` where `k = i + n`.
					// The inductive step is:
					//
					//   1. `idx` is drawn uniformly from `{0, 1, ..., k}` — i.e. one of `k + 1` (=
					//      `i + 1 + n`) values — matching `rng.random_range(0..(i + 1 + num))`.
					//   2. The new item enters the reservoir iff `idx < n` (i.e. `res.get_mut(idx)`
					//      returns `Some`), with probability `n/(k+1)`. That is the target
					//      inclusion probability after seeing `k + 1` items.
					//   3. For each item already in the reservoir, the probability of being
					//      replaced at this step is `1/(k+1)` (one specific slot among the `k+1`
					//      equally-likely outcomes of `idx`), so the probability of survival is
					//      `k/(k+1)`. Combined with the inductive hypothesis `n/k`, its post-step
					//      inclusion probability is `n/k * k/(k+1) = n/(k+1)`, preserving the
					//      invariant.
					//
					// At termination (after seeing all `N` items) every item is therefore
					// in the reservoir with probability `n/N`, i.e. uniform.
					for (i, v) in iter.enumerate() {
						let v = v?;
						// Pick an index to insert the value in, swapping existing values
						// if it is within the range.
						let idx = rng.random_range(0..(i + 1 + num as usize));
						if let Some(slot) = res.get_mut(idx as usize) {
							*slot = v
						}
					}

					// The above code does not create a random ordering.
					// if for example only the first n values happened to be selected they are
					// still in the original ordering. So shuffle the final result.
					res.shuffle(&mut rng);
					Ok(res)
				};
				#[cfg(target_family = "wasm")]
				let res = f();
				#[cfg(not(target_family = "wasm"))]
				let res = spawn_blocking(f).await.map_err(|e| Error::OrderingError(format!("{e}")))?;
				//
				res
			}
			Ordering::Order(orders) => {
				let sort_dir = self.dir.path().join(Self::SORT_DIRECTORY_NAME);
				let buffer_limit = self.buffer_limit;

				let f = move || {
					fs::create_dir(&sort_dir)?;

					let sorter: ExternalSorter<
						Value,
						Error,
						LimitedBufferBuilder,
						ValueExternalChunk,
					> = ExternalSorterBuilder::new()
						.with_tmp_dir(&sort_dir)
						.with_buffer(LimitedBufferBuilder::new(buffer_limit, true))
						.build()?;

					let sorted = sorter.sort_by(reader, |a, b| orders.compare(a, b))?;
					let iter = sorted.map(Result::unwrap);
					let r: Vec<Value> = iter.skip(start as usize).take(num as usize).collect();
					Ok(r)
				};
				#[cfg(target_family = "wasm")]
				let res = f();
				#[cfg(not(target_family = "wasm"))]
				let res = spawn_blocking(f).await.map_err(|e| Error::OrderingError(format!("{e}")))?;
				//
				res
			}
		}
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
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

	fn write_value<W: Write>(writer: &mut W, value: &Value) -> Result<usize, Error> {
		let mut val = Vec::new();
		SerializeRevisioned::serialize_revisioned(value, &mut val)?;
		// Write the size of the buffer in the index
		Self::write_usize(writer, val.len())?;
		// Write the buffer in the records
		writer.write_all(&val)?;
		Ok(val.len())
	}

	fn push(&mut self, value: &Value) -> Result<(), Error> {
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
	/// The amount of values present in the file of this reader.
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
		let val: Value = DeserializeRevisioned::deserialize_revisioned(&mut buf.as_slice())?;
		Ok(val)
	}

	fn read_usize<R: Read>(reader: &mut R) -> Result<usize, io::Error> {
		let mut buf = vec![0u8; FileCollector::USIZE_SIZE];
		reader.read_exact(&mut buf)?;
		// Safe because we know the slice length matches the expected length
		let u = usize::from_be_bytes(buf.try_into().expect("buffer size matches usize"));
		Ok(u)
	}

	fn take_vec(&mut self, start: u32, num: u32) -> Result<Vec<Value>, Error> {
		let mut iter = FileRecordsIterator::new(self.records.clone(), self.len);
		if start > 0 {
			// Get the start offset of the first record
			let mut index = OpenOptions::new().read(true).open(&self.index)?;
			index
				.seek(SeekFrom::Start(((start as usize - 1) * FileCollector::USIZE_SIZE) as u64))?;
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

	fn size_hint(&self) -> (usize, Option<usize>) {
		(self.len - self.pos, Some(self.len - self.pos))
	}
}

impl ExactSizeIterator for FileRecordsIterator {
	fn len(&self) -> usize {
		self.len - self.pos
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
			FileWriter::write_value(chunk_writer, &item)?;
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
