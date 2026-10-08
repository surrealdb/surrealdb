//! The encoding of a spilled run: records written into parts and read back
//! from them.
//!
//! A record is a list of keys and a row, each a value encoded as a stored
//! value is and framed by its length; the record is framed by its key count.
//! Records are appended to one part buffer and the buffer is written out each
//! time it holds a part, so a record may cross a part boundary. A reader
//! holds the part it is reading and, while a record crosses into the next,
//! that one too.
//!
//! A [`Run`] owns its run in the store: dropping it deletes the run, so a
//! query that ends or fails before merging what it spilled leaves nothing
//! behind.

use std::sync::Arc;

use anyhow::Context as _;
use revision::{DeserializeRevisioned, SerializeRevisioned};

use super::QueryMemory;
use super::store::{RunId, RunRead, RunWrite, TempStore};
use crate::val::Value;

/// A spilled record: its keys, then its row.
pub(crate) type Record = (Vec<Value>, Value);

/// A run in the store, deleted when dropped.
pub(crate) struct Run {
	id: RunId,
	bytes: u64,
	records: u64,
	store: Arc<dyn TempStore>,
	memory: Arc<QueryMemory>,
}

impl std::fmt::Debug for Run {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Run").field("id", &self.id).field("records", &self.records).finish()
	}
}

impl Run {
	/// A reader over the run's records, from the first.
	pub(crate) async fn read(&self) -> anyhow::Result<RunSource> {
		Ok(RunSource {
			reader: Some(self.store.read_run(self.id).await?),
			buf: Vec::new(),
			at: 0,
		})
	}
}

impl Drop for Run {
	fn drop(&mut self) {
		self.store.delete_run(self.id);
		self.memory.unspilled(self.bytes);
	}
}

/// A run being written: records appended, written out a part at a time.
pub(crate) struct RunSink {
	writer: Box<dyn RunWrite>,
	buf: Vec<u8>,
	part_size: usize,
	bytes: u64,
	records: u64,
	store: Arc<dyn TempStore>,
	memory: Arc<QueryMemory>,
}

impl RunSink {
	/// Begin a run in `memory`'s store.
	pub(crate) async fn open(memory: &Arc<QueryMemory>) -> anyhow::Result<Self> {
		let store = memory.store().context("A query spilled with no temporary store")?;
		let part_size = store.part_size();
		Ok(Self {
			writer: store.write_run().await?,
			buf: Vec::with_capacity(part_size),
			part_size,
			bytes: 0,
			records: 0,
			store,
			memory: Arc::clone(memory),
		})
	}

	/// Append a record, writing out the part buffer once it holds a part.
	pub(crate) async fn push(&mut self, keys: &[Value], row: &Value) -> anyhow::Result<()> {
		encode_record(&mut self.buf, keys, row)?;
		self.records += 1;
		if self.buf.len() >= self.part_size {
			self.write_part().await?;
		}
		Ok(())
	}

	async fn write_part(&mut self) -> anyhow::Result<()> {
		let len = self.buf.len() as u64;
		self.memory.spilled(len)?;
		self.bytes += len;
		let part = std::mem::replace(&mut self.buf, Vec::with_capacity(self.part_size));
		self.writer.write_part(part).await
	}

	/// Close the run.
	pub(crate) async fn finish(mut self) -> anyhow::Result<Run> {
		if !self.buf.is_empty() {
			self.write_part().await?;
		}
		let id = self.writer.finish().await?;
		Ok(Run {
			id,
			bytes: self.bytes,
			records: self.records,
			store: self.store,
			memory: self.memory,
		})
	}
}

/// A run's records, read back in the order written.
pub(crate) struct RunSource {
	reader: Option<Box<dyn RunRead>>,
	buf: Vec<u8>,
	at: usize,
}

impl RunSource {
	/// The next record, or `None` at the end of the run.
	pub(crate) async fn next(&mut self) -> anyhow::Result<Option<Record>> {
		loop {
			if let Some((record, used)) = decode_record(&self.buf[self.at..])? {
				self.at += used;
				return Ok(Some(record));
			}
			let Some(reader) = &mut self.reader else {
				anyhow::ensure!(self.at == self.buf.len(), "A spilled run ended inside a record");
				return Ok(None);
			};
			match reader.next_part().await? {
				Some(part) => {
					self.buf.drain(..self.at);
					self.at = 0;
					if self.buf.is_empty() {
						self.buf = part;
					} else {
						self.buf.extend_from_slice(&part);
					}
				}
				None => self.reader = None,
			}
		}
	}
}

fn encode_value(out: &mut Vec<u8>, value: &Value) -> anyhow::Result<()> {
	let at = out.len();
	out.extend_from_slice(&[0u8; 8]);
	SerializeRevisioned::serialize_revisioned(value, out)?;
	let len = (out.len() - at - 8) as u64;
	out[at..at + 8].copy_from_slice(&len.to_be_bytes());
	Ok(())
}

/// Append the framed encoding of a record to `out`.
pub(crate) fn encode_record(out: &mut Vec<u8>, keys: &[Value], row: &Value) -> anyhow::Result<()> {
	out.extend_from_slice(&(keys.len() as u64).to_be_bytes());
	for key in keys {
		encode_value(out, key)?;
	}
	encode_value(out, row)
}

fn read_u64(bytes: &[u8], at: usize) -> Option<u64> {
	let end = at.checked_add(8)?;
	let raw: [u8; 8] = bytes.get(at..end)?.try_into().ok()?;
	Some(u64::from_be_bytes(raw))
}

/// The value framed at `at`, and where the next frame starts, or `None` when
/// `bytes` ends before the frame does.
fn decode_value(bytes: &[u8], at: usize) -> anyhow::Result<Option<(Value, usize)>> {
	let Some(len) = read_u64(bytes, at) else {
		return Ok(None);
	};
	let start = at + 8;
	let Some(end) = start.checked_add(len as usize) else {
		anyhow::bail!("A spilled run holds a malformed record");
	};
	let Some(mut frame) = bytes.get(start..end) else {
		return Ok(None);
	};
	let value = DeserializeRevisioned::deserialize_revisioned(&mut frame)?;
	Ok(Some((value, end)))
}

/// The record at the start of `bytes`, and its length, or `None` when `bytes`
/// ends before the record does.
pub(crate) fn decode_record(bytes: &[u8]) -> anyhow::Result<Option<(Record, usize)>> {
	let Some(count) = read_u64(bytes, 0) else {
		return Ok(None);
	};
	let mut at = 8;
	let mut keys = Vec::with_capacity((count as usize).min(64));
	for _ in 0..count {
		let Some((key, next)) = decode_value(bytes, at)? else {
			return Ok(None);
		};
		keys.push(key);
		at = next;
	}
	let Some((row, next)) = decode_value(bytes, at)? else {
		return Ok(None);
	};
	Ok(Some(((keys, row), next)))
}
