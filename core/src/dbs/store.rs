use crate::dbs::plan::Explanation;
use crate::sql::value::Value;
use crate::sql::Orders;
use std::mem;

#[derive(Default)]
pub(super) struct MemoryCollector(Vec<Value>);

impl MemoryCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	pub(super) fn sort(&mut self, orders: &Orders) {
		self.0.sort_by(|a, b| orders.compare(a, b));
	}

	pub(super) fn len(&self) -> usize {
		self.0.len()
	}

	pub(super) fn start_limit(&mut self, start: Option<&usize>, limit: Option<&usize>) {
		match (start, limit) {
			(Some(&start), Some(&limit)) => {
				self.0 = mem::take(&mut self.0).into_iter().skip(start).take(limit).collect()
			}
			(Some(&start), None) => {
				self.0 = mem::take(&mut self.0).into_iter().skip(start).collect()
			}
			(None, Some(&limit)) => {
				self.0 = mem::take(&mut self.0).into_iter().take(limit).collect()
			}
			(None, None) => {}
		}
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

#[cfg(any(
	feature = "kv-mem",
	feature = "kv-surrealkv",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
pub(super) mod file_store {
	use crate::cnf::EXTERNAL_SORTING_BUFFER_LIMIT;
	use crate::dbs::plan::Explanation;
	use crate::err::Error;
	use crate::sql::{Orders, Value};
	use ext_sort::{ExternalChunk, ExternalSorter, ExternalSorterBuilder, LimitedBufferBuilder};
	use revision::Revisioned;
	use std::fs::{File, OpenOptions};
	use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Take, Write};
	use std::path::{Path, PathBuf};
	use std::{fs, io, mem};
	use tempfile::{Builder, TempDir};

	pub(in crate::dbs) struct FileCollector {
		dir: TempDir,
		len: usize,
		writer: Option<FileWriter>,
		reader: Option<FileReader>,
		orders: Option<Orders>,
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
		pub(in crate::dbs) fn push(&mut self, value: Value) -> Result<(), Error> {
			if let Some(writer) = &mut self.writer {
				writer.push(value)?;
				self.len += 1;
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
		pub(in crate::dbs) fn sort(&mut self, orders: &Orders) {
			self.orders = Some(orders.clone());
		}

		pub(in crate::dbs) fn len(&self) -> usize {
			self.len
		}

		pub(in crate::dbs) fn start_limit(&mut self, start: Option<&usize>, limit: Option<&usize>) {
			self.paging.start = start.cloned();
			self.paging.limit = limit.cloned();
		}

		pub(in crate::dbs) fn take_vec(&mut self) -> Result<Vec<Value>, Error> {
			self.check_reader()?;
			if let Some(mut reader) = self.reader.take() {
				if let Some((start, num)) = self.paging.get_start_num(reader.len) {
					if let Some(orders) = self.orders.take() {
						return self.sort_and_take_vec(reader, orders, start, num);
					}
					return reader.take_vec(start, num);
				}
			}
			Ok(vec![])
		}

		fn sort_and_take_vec(
			&mut self,
			reader: FileReader,
			orders: Orders,
			start: usize,
			num: usize,
		) -> Result<Vec<Value>, Error> {
			let sort_dir = self.dir.path().join(Self::SORT_DIRECTORY_NAME);
			fs::create_dir(&sort_dir)?;

			let sorter: ExternalSorter<Value, Error, LimitedBufferBuilder, ValueExternalChunk> =
				ExternalSorterBuilder::new()
					.with_tmp_dir(&sort_dir)
					.with_buffer(LimitedBufferBuilder::new(*EXTERNAL_SORTING_BUFFER_LIMIT, true))
					.build()?;

			let sorted = sorter.sort_by(reader, |a, b| orders.compare(a, b))?;
			let iter = sorted.map(Result::unwrap);
			let r: Vec<Value> = iter.skip(start).take(num).collect();
			Ok(r)
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
			debug!("PUSH {}", self.offset);
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

		fn take_vec(&mut self, start: usize, num: usize) -> Result<Vec<Value>, Error> {
			let mut iter = FileRecordsIterator::new(self.records.clone(), self.len);
			if start > 0 {
				// Get the start offset of the first record
				let mut index = OpenOptions::new().read(true).open(&self.index)?;
				index.seek(SeekFrom::Start(((start - 1) * FileCollector::USIZE_SIZE) as u64))?;
				let start_offset = Self::read_usize(&mut index)?;

				// Set records to the position of the first record
				iter.seek(start_offset, start)?;
			}

			// Collect the records
			let mut res = Vec::with_capacity(num);
			for _ in 0..num {
				debug!("READ");
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
				debug!("SEEK {seek_pos}");
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
		start: Option<usize>,
		limit: Option<usize>,
	}

	impl FilePaging {
		fn get_start_num(&self, len: usize) -> Option<(usize, usize)> {
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
			debug!("FilePaging - START: {start} - NUM: {num}");
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
