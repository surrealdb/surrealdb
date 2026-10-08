//! The host's temporary store: where a query's breakers write the runs they
//! spill.
//!
//! The store holds runs, each a sequence of parts written once, in order,
//! and read back in the same order. A run is written through one part buffer
//! and read through one, whatever its length, so spilling holds a bounded
//! amount of memory however much it spills. Every part but a run's last is at
//! least [`TempStore::part_size`] bytes, which suits object stores whose
//! multipart uploads have a minimum part size.
//!
//! The trait is the whole contract between the VM and a host's storage:
//!
//! - native hosts use [`super::FileTempStore`], files in a temporary directory;
//! - an edge host supplies its own: a local tier (a Durable Object's storage) with an
//!   object-storage overflow ([`super::TieredTempStore`]), or object storage alone for an isolate
//!   with no local store.
//!
//! A run is deleted once the breaker that wrote it has merged it, or when the
//! query ends without merging it. A store also removes, on its own, runs a
//! process that ended without deleting them left behind.

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use crate::exec::BoxFut;

/// A run's name within its store.
pub type RunId = u64;

/// Where a query's spilled runs are kept.
pub trait TempStore: Send + Sync + 'static {
	/// The size a run's parts are written in: every part but the last is at
	/// least this many bytes.
	fn part_size(&self) -> usize;

	/// The most bytes one query may spill to this store, or `None` for no
	/// limit beyond the store's own capacity.
	fn quota(&self) -> Option<u64>;

	/// Begin a new run.
	fn write_run(&self) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>>;

	/// Read run `run` from its first part.
	fn read_run(&self, run: RunId) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>>;

	/// Delete run `run`. The store may remove it later, but never hands it
	/// out again. Deleting a run that does not exist is not an error.
	fn delete_run(&self, run: RunId);
}

/// A run being written, a part at a time.
pub trait RunWrite: Send {
	/// Append `part` to the run.
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>>;

	/// Close the run, answering its name.
	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunId>>;
}

/// A run being read, a part at a time.
pub trait RunRead: Send {
	/// The run's next part, or `None` once every part has been read.
	fn next_part(&mut self) -> BoxFut<'_, anyhow::Result<Option<Vec<u8>>>>;
}

/// A store keeping its runs in memory: for tests, and as a stand-in for a
/// host's object store.
pub struct MemTempStore {
	part_size: usize,
	quota: Option<u64>,
	next: AtomicU64,
	runs: std::sync::Arc<Mutex<HashMap<RunId, Vec<Vec<u8>>>>>,
	written: std::sync::Arc<AtomicUsize>,
	/// The runs being read now, and the most read at once.
	reading: std::sync::Arc<(AtomicUsize, AtomicUsize)>,
}

impl std::fmt::Debug for MemTempStore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MemTempStore").field("part_size", &self.part_size).finish()
	}
}

impl MemTempStore {
	/// A store writing parts of `part_size` bytes, with `quota` per query.
	pub fn new(part_size: usize, quota: Option<u64>) -> Self {
		Self {
			part_size: part_size.max(1),
			quota,
			next: AtomicU64::new(1),
			runs: Default::default(),
			written: Default::default(),
			reading: Default::default(),
		}
	}

	/// The runs the store holds now.
	pub fn runs(&self) -> usize {
		self.runs.lock().unwrap_or_else(|e| e.into_inner()).len()
	}

	/// The bytes the store holds now.
	pub fn bytes(&self) -> usize {
		let runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
		runs.values().flatten().map(Vec::len).sum()
	}

	/// The most runs read at once since the store was made.
	pub fn most_read_at_once(&self) -> usize {
		self.reading.1.load(Ordering::Relaxed)
	}

	/// The runs ever written to the store.
	pub fn runs_written(&self) -> usize {
		self.written.load(Ordering::Relaxed)
	}
}

impl TempStore for MemTempStore {
	fn part_size(&self) -> usize {
		self.part_size
	}

	fn quota(&self) -> Option<u64> {
		self.quota
	}

	fn write_run(&self) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		let id = self.next.fetch_add(1, Ordering::Relaxed);
		let runs = std::sync::Arc::clone(&self.runs);
		let written = std::sync::Arc::clone(&self.written);
		Box::pin(async move {
			Ok(Box::new(MemRunWrite {
				id,
				parts: Vec::new(),
				runs,
				written,
			}) as Box<dyn RunWrite>)
		})
	}

	fn read_run(&self, run: RunId) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		Box::pin(async move {
			let parts = self
				.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.get(&run)
				.cloned()
				.ok_or_else(|| anyhow::anyhow!("A spilled run is missing from its store"))?;
			let now = self.reading.0.fetch_add(1, Ordering::Relaxed) + 1;
			self.reading.1.fetch_max(now, Ordering::Relaxed);
			Ok(Box::new(MemRunRead {
				parts: parts.into_iter(),
				reading: std::sync::Arc::clone(&self.reading),
			}) as Box<dyn RunRead>)
		})
	}

	fn delete_run(&self, run: RunId) {
		self.runs.lock().unwrap_or_else(|e| e.into_inner()).remove(&run);
	}
}

struct MemRunWrite {
	id: RunId,
	parts: Vec<Vec<u8>>,
	runs: std::sync::Arc<Mutex<HashMap<RunId, Vec<Vec<u8>>>>>,
	written: std::sync::Arc<AtomicUsize>,
}

impl RunWrite for MemRunWrite {
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>> {
		self.parts.push(part);
		Box::pin(async { Ok(()) })
	}

	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunId>> {
		Box::pin(async move {
			let this = *self;
			this.runs.lock().unwrap_or_else(|e| e.into_inner()).insert(this.id, this.parts);
			this.written.fetch_add(1, Ordering::Relaxed);
			Ok(this.id)
		})
	}
}

struct MemRunRead {
	parts: std::vec::IntoIter<Vec<u8>>,
	reading: std::sync::Arc<(AtomicUsize, AtomicUsize)>,
}

impl Drop for MemRunRead {
	fn drop(&mut self) {
		self.reading.0.fetch_sub(1, Ordering::Relaxed);
	}
}

impl RunRead for MemRunRead {
	fn next_part(&mut self) -> BoxFut<'_, anyhow::Result<Option<Vec<u8>>>> {
		let part = self.parts.next();
		Box::pin(async move { Ok(part) })
	}
}
