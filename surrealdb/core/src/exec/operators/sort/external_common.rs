//! Shared on-disk record format for external (disk-backed) sort operators.
//!
//! Both [`super::external::ExternalSort`] and [`super::external_by_key::ExternalSortByKey`]
//! spool input rows to temporary files with their pre-computed sort keys, then
//! external-merge-sort the result. The on-disk format and the `ext_sort` chunk
//! adapter are identical between the two — only the key-extraction policy
//! differs — so the shared bits live here.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Take, Write};
use std::mem;
use std::path::PathBuf;

use ext_sort::ExternalChunk;
use revision::{DeserializeRevisioned, SerializeRevisioned};
use tempfile::TempDir;

use crate::err::Error;
use crate::val::Value;

/// A value with pre-computed sort keys for external sorting.
#[derive(Debug, Clone)]
pub(super) struct KeyedValue {
	pub(super) keys: Vec<Value>,
	pub(super) value: Value,
}

const USIZE_SIZE: usize = mem::size_of::<usize>();

/// Writer for temporary files during external sort.
pub(super) struct TempFileWriter {
	records: BufWriter<File>,
}

impl TempFileWriter {
	const RECORDS_FILE_NAME: &'static str = "records";

	pub(super) fn new(dir: &TempDir) -> Result<Self, Error> {
		let records = OpenOptions::new()
			.create_new(true)
			.append(true)
			.open(dir.path().join(Self::RECORDS_FILE_NAME))?;
		Ok(Self {
			records: BufWriter::new(records),
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
		Self::write_usize(writer, val.len())?;
		writer.write_all(&val)?;
		Ok(val.len())
	}

	pub(super) fn push(&mut self, keyed: &KeyedValue) -> Result<(), Error> {
		Self::write_usize(&mut self.records, keyed.keys.len())?;
		for key in &keyed.keys {
			Self::write_value(&mut self.records, key)?;
		}
		Self::write_value(&mut self.records, &keyed.value)?;
		Ok(())
	}

	pub(super) fn flush(mut self) -> Result<(), Error> {
		self.records.flush()?;
		Ok(())
	}
}

/// Reader for temporary files during external sort.
pub(super) struct TempFileReader {
	len: usize,
	records_path: PathBuf,
}

impl TempFileReader {
	pub(super) fn new(len: usize, dir: &TempDir) -> Result<Self, Error> {
		Ok(Self {
			len,
			records_path: dir.path().join(TempFileWriter::RECORDS_FILE_NAME),
		})
	}
}

impl IntoIterator for TempFileReader {
	type Item = Result<KeyedValue, Error>;
	type IntoIter = TempFileIterator;

	fn into_iter(self) -> Self::IntoIter {
		TempFileIterator::new(self.records_path, self.len)
	}
}

/// Iterator over temporary file records.
pub(super) struct TempFileIterator {
	path: PathBuf,
	reader: Option<BufReader<File>>,
	len: usize,
	pos: usize,
}

impl TempFileIterator {
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

	fn read_usize<R: Read>(reader: &mut R) -> Result<usize, std::io::Error> {
		let mut buf = vec![0u8; USIZE_SIZE];
		reader.read_exact(&mut buf)?;
		Ok(usize::from_be_bytes(buf.try_into().expect("buffer size matches usize")))
	}

	fn read_value<R: Read>(reader: &mut R) -> Result<Value, Error> {
		let len = Self::read_usize(reader)?;
		let mut buf = vec![0u8; len];
		reader.read_exact(&mut buf)?;
		let val: Value = DeserializeRevisioned::deserialize_revisioned(&mut buf.as_slice())?;
		Ok(val)
	}

	fn read_keyed_value<R: Read>(reader: &mut R) -> Result<KeyedValue, Error> {
		let num_keys = Self::read_usize(reader)?;
		let mut keys = Vec::with_capacity(num_keys);
		for _ in 0..num_keys {
			keys.push(Self::read_value(reader)?);
		}
		let value = Self::read_value(reader)?;
		Ok(KeyedValue {
			keys,
			value,
		})
	}
}

impl Iterator for TempFileIterator {
	type Item = Result<KeyedValue, Error>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.pos == self.len {
			return None;
		}
		if let Err(e) = self.check_reader() {
			return Some(Err(e));
		}
		if let Some(reader) = &mut self.reader {
			match Self::read_keyed_value(reader) {
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

impl ExactSizeIterator for TempFileIterator {
	fn len(&self) -> usize {
		self.len - self.pos
	}
}

/// External chunk implementation for [`KeyedValue`], used by `ext_sort` to
/// spill intermediate sorted runs while merge-sorting.
pub(super) struct KeyedValueExternalChunk {
	reader: Take<BufReader<File>>,
}

impl ExternalChunk<KeyedValue> for KeyedValueExternalChunk {
	type SerializationError = Error;
	type DeserializationError = Error;

	fn new(reader: Take<BufReader<File>>) -> Self {
		Self {
			reader,
		}
	}

	fn dump(
		chunk_writer: &mut BufWriter<File>,
		items: impl IntoIterator<Item = KeyedValue>,
	) -> Result<(), Self::SerializationError> {
		for item in items {
			TempFileWriter::write_usize(chunk_writer, item.keys.len())?;
			for key in &item.keys {
				TempFileWriter::write_value(chunk_writer, key)?;
			}
			TempFileWriter::write_value(chunk_writer, &item.value)?;
		}
		Ok(())
	}
}

impl Iterator for KeyedValueExternalChunk {
	type Item = Result<KeyedValue, Error>;

	fn next(&mut self) -> Option<Self::Item> {
		if self.reader.limit() == 0 {
			None
		} else {
			match TempFileIterator::read_keyed_value(&mut self.reader) {
				Ok(val) => Some(Ok(val)),
				Err(err) => Some(Err(err)),
			}
		}
	}
}
