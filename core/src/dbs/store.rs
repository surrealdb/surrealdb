use crate::dbs::plan::Explanation;
use crate::err::Error;
use crate::sql::value::Value;
use crate::sql::Orders;
// use ext_sort::buffer::mem::MemoryLimitedBufferBuilder;
// use ext_sort::{ExternalSorter, RmpExternalChunk};
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use revision::Revisioned;
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::fs::{File, OpenOptions};
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem;
#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
use tempfile::TempDir;

#[derive(Default)]
pub(super) struct MemoryCollector(Vec<Value>);

impl MemoryCollector {
	pub(super) fn push(&mut self, val: Value) {
		self.0.push(val);
	}

	pub(super) fn sort(&mut self, orders: &Orders) -> Result<(), Error> {
		self.0.sort_by(|a, b| orders.compare(a, b));
		Ok(())
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

	pub(super) fn try_iter_mut(&mut self) -> Result<std::slice::IterMut<'_, Value>, Error> {
		Ok(self.0.iter_mut())
	}

	pub(super) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("Memory", vec![]);
	}
}

#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
pub(super) struct FileCollector {
	dir: TempDir,
	len: usize,
	writer: Option<FileWriter>,
	reader: Option<FileReader>,
	start: Option<usize>,
	limit: Option<usize>,
}

#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
impl FileCollector {
	const INDEX_FILE_NAME: &'static str = "ix";
	const RECORDS_FILE_NAME: &'static str = "re";
	const USIZE_SIZE: usize = mem::size_of::<usize>();

	pub(super) fn new() -> Result<Self, Error> {
		let dir = TempDir::new()?;
		Ok(Self {
			len: 0,
			writer: Some(FileWriter::new(&dir)?),
			reader: None,
			start: None,
			limit: None,
			dir,
		})
	}
	pub(super) fn push(&mut self, value: Value) -> Result<(), Error> {
		if let Some(writer) = &mut self.writer {
			writer.push(value)?;
			self.len += 1;
			Ok(())
		} else {
			Err(Error::Unreachable("FileCollector::push"))
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
	pub(super) fn sort(&mut self, _orders: &Orders) -> Result<(), Error> {
		self.check_reader()?;
		todo!()
	}

	pub(super) fn len(&self) -> usize {
		self.len
	}

	pub(super) fn start_limit(&mut self, start: Option<&usize>, limit: Option<&usize>) {
		self.start = start.cloned();
		self.limit = limit.cloned();
	}

	pub(super) fn try_iter_mut(&mut self) -> Result<std::slice::IterMut<'_, Value>, Error> {
		todo!()
	}

	pub(super) fn take_vec(&mut self) -> Result<Vec<Value>, Error> {
		self.check_reader()?;
		if let Some(reader) = &mut self.reader {
			reader.take_vec(self.start, self.limit)
		} else {
			Ok(vec![])
		}
	}
	pub(super) fn explain(&self, exp: &mut Explanation) {
		exp.add_collector("TempFile", vec![]);
	}
}

#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
struct FileWriter {
	index: BufWriter<File>,
	records: BufWriter<File>,
	offset: usize,
}

#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
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

	fn write_usize(writer: &mut BufWriter<File>, u: usize) -> Result<(), Error> {
		let buf = u.to_be_bytes();
		writer.write_all(&buf)?;
		Ok(())
	}

	fn push(&mut self, value: Value) -> Result<(), Error> {
		// Serialize the value in a buffer
		let mut val = Vec::new();
		value.serialize_revisioned(&mut val)?;
		// Write the size of the buffer in the index
		Self::write_usize(&mut self.records, val.len())?;
		// Write the buffer in the records
		self.records.write_all(&val)?;
		// Increment the offset of the next record
		self.offset += val.len() + FileCollector::USIZE_SIZE;
		Self::write_usize(&mut self.index, self.offset)?;
		Ok(())
	}

	fn flush(mut self) -> Result<(), Error> {
		self.records.flush()?;
		self.index.flush()?;
		Ok(())
	}
}

#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
struct FileReader {
	len: usize,
	index: BufReader<File>,
	records: BufReader<File>,
}

#[cfg(any(
	feature = "kv-surrealkv",
	feature = "kv-file",
	feature = "kv-rocksdb",
	feature = "kv-fdb",
	feature = "kv-tikv",
	feature = "kv-speedb"
))]
impl FileReader {
	fn new(len: usize, dir: &TempDir) -> Result<Self, Error> {
		let index =
			OpenOptions::new().read(true).open(dir.path().join(FileCollector::INDEX_FILE_NAME))?;
		let records = OpenOptions::new()
			.read(true)
			.open(dir.path().join(FileCollector::RECORDS_FILE_NAME))?;
		Ok(Self {
			len,
			index: BufReader::new(index),
			records: BufReader::new(records),
		})
	}

	fn read_usize(reader: &mut BufReader<File>) -> Result<usize, Error> {
		let mut buf = vec![0u8; FileCollector::USIZE_SIZE];
		reader.read_exact(&mut buf)?;
		// Safe to call unwrap because we know the slice length matches the expected length
		let u = usize::from_be_bytes(buf.try_into().unwrap());
		Ok(u)
	}

	fn take_vec(
		&mut self,
		start: Option<usize>,
		limit: Option<usize>,
	) -> Result<Vec<Value>, Error> {
		let start = start.unwrap_or(0);
		if start >= self.len {
			return Ok(vec![]);
		}

		if start > 0 {
			self.index.seek(SeekFrom::Start(((start - 1) * FileCollector::USIZE_SIZE) as u64))?;

			// Get the start offset of the first record
			let start_offset = Self::read_usize(&mut self.index)?;

			// Set records to the position of the first record
			self.records.seek(SeekFrom::Start(start_offset as u64))?;
		}

		// Compute the maximum number of record to collect
		let max = self.len - start;
		let num = if let Some(limit) = limit {
			limit.min(max)
		} else {
			max
		};

		// Collect the records
		let mut res = Vec::with_capacity(num);
		for _ in 0..num {
			let len = Self::read_usize(&mut self.records)?;
			let mut buf = vec![0u8; len];
			self.records.read_exact(&mut buf)?;
			let val = Value::deserialize_revisioned(&mut buf.as_slice())?;
			res.push(val);
		}
		Ok(res)
	}
}
