//! A sort's rows held on disk once they outgrow the rows it may keep in
//! memory, for a statement that asks for `TEMPFILES`.
//!
//! The sort takes its rows in arrival order. Each time it holds its limit, the
//! rows are sorted — stably — and written out as one run. When the sort hands
//! its rows on, the runs and the rows still held are merged: at each step the
//! row that orders first is taken, and of rows whose keys are equal, the one
//! from the earlier run, so the merged order is the order sorting every row at
//! once gives.
//!
//! A run is a file of records, each its keys and its row, every value framed
//! by its length and encoded as a stored value is. Records are read back one
//! at a time, so a merge holds one record per run.

use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::Context as _;
use revision::{DeserializeRevisioned, SerializeRevisioned};

use super::interp::{SortRow, compare_keys, sort_and_keep};
use super::program::KeyOrder;
use crate::val::Value;

/// How many runs this process has written, for tests that must know a sort
/// spilled.
#[cfg(test)]
pub(super) static RUNS_WRITTEN: std::sync::atomic::AtomicUsize =
	std::sync::atomic::AtomicUsize::new(0);

/// The runs a sort has written, in the order it wrote them.
pub(crate) struct Runs {
	dir: tempfile::TempDir,
	runs: Vec<PathBuf>,
}

impl std::fmt::Debug for Runs {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Runs").field("runs", &self.runs.len()).finish()
	}
}

impl Runs {
	/// No runs yet, written beneath `parent`.
	pub(super) fn new(parent: &Path) -> anyhow::Result<Self> {
		let dir = tempfile::Builder::new()
			.prefix("SURREAL_VM_SORT")
			.tempdir_in(parent)
			.context("Failed to create a sort's temporary directory")?;
		Ok(Self {
			dir,
			runs: Vec::new(),
		})
	}

	/// Sort `rows` and write them as the next run.
	pub(super) async fn spill(
		&mut self,
		mut rows: Vec<(Vec<Value>, SortRow)>,
		keys: Vec<KeyOrder>,
	) -> anyhow::Result<()> {
		let path = self.dir.path().join(format!("run-{}", self.runs.len()));
		let written = path.clone();
		tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
			sort_and_keep(&mut rows, &keys, usize::MAX);
			let mut out = BufWriter::new(File::create(&written)?);
			for (keys, row) in rows {
				write_record(&mut out, &keys, &row.into_value()?)?;
			}
			out.flush()?;
			Ok(())
		})
		.await
		.context("A sort's spill task failed")??;
		self.runs.push(path);
		#[cfg(test)]
		RUNS_WRITTEN.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
		Ok(())
	}

	/// The merge of every run with `held`, the rows still in memory, which
	/// arrived after every run, already in order.
	pub(super) fn merge(
		self,
		held: Vec<(Vec<Value>, SortRow)>,
		keys: Vec<KeyOrder>,
	) -> anyhow::Result<Merge> {
		let mut sources = Vec::with_capacity(self.runs.len() + 1);
		for path in &self.runs {
			sources.push(Source::Run(BufReader::new(File::open(path)?)));
		}
		sources.push(Source::Held(held.into_iter()));
		let mut merge = Merge {
			_dir: self.dir,
			heads: Vec::with_capacity(sources.len()),
			sources,
			keys,
		};
		for at in 0..merge.sources.len() {
			let head = merge.sources[at].next()?;
			merge.heads.push(head);
		}
		Ok(merge)
	}
}

/// Where a merge reads rows from: a run on disk, or the rows held in memory.
enum Source {
	Run(BufReader<File>),
	Held(std::vec::IntoIter<(Vec<Value>, SortRow)>),
}

impl Source {
	fn next(&mut self) -> anyhow::Result<Option<(Vec<Value>, Value)>> {
		match self {
			Source::Run(reader) => read_record(reader),
			Source::Held(rows) => match rows.next() {
				Some((keys, row)) => Ok(Some((keys, row.into_value()?))),
				None => Ok(None),
			},
		}
	}
}

/// The merged rows of a sort's runs and the rows it still held.
pub(crate) struct Merge {
	/// Held so the runs are removed only once the merge is done with them.
	_dir: tempfile::TempDir,
	sources: Vec<Source>,
	/// Each source's next row, `None` once the source is spent.
	heads: Vec<Option<(Vec<Value>, Value)>>,
	keys: Vec<KeyOrder>,
}

impl std::fmt::Debug for Merge {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Merge").field("sources", &self.sources.len()).finish()
	}
}

impl Merge {
	/// Up to `count` further rows in order, reading the runs on a blocking
	/// thread; empty once every row is taken.
	pub(super) async fn next_chunk(
		mut self,
		count: usize,
	) -> anyhow::Result<(Self, Vec<(Vec<Value>, Value)>)> {
		tokio::task::spawn_blocking(move || -> anyhow::Result<_> {
			let mut chunk = Vec::with_capacity(count);
			while chunk.len() < count {
				// The earliest source wins a tie, which keeps the sort stable.
				let mut first: Option<usize> = None;
				for (at, head) in self.heads.iter().enumerate() {
					let Some((keys, _)) = head else {
						continue;
					};
					first = match first {
						Some(best) => {
							let best_keys =
								&self.heads[best].as_ref().expect("a chosen head is present").0;
							if compare_keys(keys, best_keys, &self.keys) == std::cmp::Ordering::Less
							{
								Some(at)
							} else {
								Some(best)
							}
						}
						None => Some(at),
					};
				}
				let Some(at) = first else {
					break;
				};
				let row = self.heads[at].take().expect("a chosen head is present");
				self.heads[at] = self.sources[at].next()?;
				chunk.push(row);
			}
			Ok((self, chunk))
		})
		.await
		.context("A sort's merge task failed")?
	}
}

fn write_value<W: Write>(out: &mut W, value: &Value) -> anyhow::Result<()> {
	let mut bytes = Vec::new();
	SerializeRevisioned::serialize_revisioned(value, &mut bytes)?;
	out.write_all(&(bytes.len() as u64).to_be_bytes())?;
	out.write_all(&bytes)?;
	Ok(())
}

fn write_record<W: Write>(out: &mut W, keys: &[Value], row: &Value) -> anyhow::Result<()> {
	out.write_all(&(keys.len() as u64).to_be_bytes())?;
	for key in keys {
		write_value(out, key)?;
	}
	write_value(out, row)
}

/// A `u64` framing length, or `None` at the end of the file.
fn read_len<R: Read>(reader: &mut R) -> anyhow::Result<Option<u64>> {
	let mut len = [0u8; 8];
	match reader.read_exact(&mut len) {
		Ok(()) => Ok(Some(u64::from_be_bytes(len))),
		Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
		Err(e) => Err(e.into()),
	}
}

fn read_value<R: Read>(reader: &mut R) -> anyhow::Result<Value> {
	let len = read_len(reader)?.context("A sort run ended inside a record")?;
	let mut bytes = vec![0u8; len as usize];
	reader.read_exact(&mut bytes)?;
	Ok(DeserializeRevisioned::deserialize_revisioned(&mut bytes.as_slice())?)
}

fn read_record<R: Read>(reader: &mut R) -> anyhow::Result<Option<(Vec<Value>, Value)>> {
	let Some(count) = read_len(reader)? else {
		return Ok(None);
	};
	let mut keys = Vec::with_capacity(count as usize);
	for _ in 0..count {
		keys.push(read_value(reader)?);
	}
	Ok(Some((keys, read_value(reader)?)))
}
