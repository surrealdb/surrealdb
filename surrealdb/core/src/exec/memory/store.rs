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
//! The store sees a run as opaque bytes. A run is named by a [`RunRef`]: the
//! session of the handle that wrote it, an id unique within that session, and
//! a [`Tier`]. A `Local` run is readable only through the handle that wrote
//! it; a `Shared` run is readable through any handle on the same backend,
//! provided the store [shares](TempStore::shares) runs. A `RunRef` is a name,
//! not a capability: a handle refuses to read a run it cannot reach.
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
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use crate::exec::BoxFut;

/// A run's name within its session.
pub type RunId = u64;

/// A store's name for the session that writes runs into it: unique among the
/// sessions of one backend.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SessionName(Arc<str>);

impl SessionName {
	/// A session named `name`.
	pub fn new(name: impl Into<Arc<str>>) -> Self {
		Self(name.into())
	}

	/// The name.
	pub fn as_str(&self) -> &str {
		&self.0
	}
}

/// Which processes can open a run.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Tier {
	/// Only the process that wrote it.
	Local,
	/// Any process holding a handle on the same backend.
	Shared,
}

/// How a run is to be written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Sharing {
	/// Readable only through the handle that writes it.
	Private,
	/// Readable through any handle on the same backend, which requires a store
	/// that [shares](TempStore::shares) runs.
	Shared,
}

/// A run's name across its backend.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RunRef {
	/// The session of the handle that wrote the run.
	pub session: SessionName,
	/// The run within its session.
	pub id: RunId,
	/// Which processes can open the run.
	pub tier: Tier,
	/// The bytes the run holds: the sum of the parts written to it, set by the
	/// store when the run is finished. It travels with the name, so a process
	/// holding only the `RunRef` and the run's spec can open the run. A
	/// reader refuses a run whose blocks and trailer do not fit within it, so a
	/// `RunRef` whose `bytes` understates the run makes the run unreadable
	/// rather than partly read.
	pub bytes: u64,
}

/// The error a store raises when asked to write a shared run it cannot share.
pub(crate) fn cannot_share() -> anyhow::Error {
	anyhow::anyhow!("This temporary store cannot share a run with another process")
}

/// The error a store raises when asked to read a run of another session that
/// it cannot reach.
pub(crate) fn not_readable() -> anyhow::Error {
	anyhow::anyhow!("A spilled run of another session is not readable from this store")
}

/// Where a query's spilled runs are kept.
///
/// `read_run` of a run whose session is not the handle's own fails unless the
/// run's tier is [`Tier::Shared`] and the store [shares](Self::shares) runs.
/// `delete_run` of a run the handle cannot read, or that does not exist, does
/// nothing.
pub trait TempStore: Send + Sync + 'static {
	/// The size a run's parts are written in: every part but the last is at
	/// least this many bytes.
	fn part_size(&self) -> usize;

	/// The most bytes one query may spill to this store, or `None` for no
	/// limit beyond the store's own capacity.
	fn quota(&self) -> Option<u64>;

	/// The session this handle writes runs under.
	fn session(&self) -> &SessionName;

	/// Whether a run written [`Sharing::Shared`] can be opened by another
	/// handle on the same backend.
	fn shares(&self) -> bool;

	/// Begin a new run. A store that does not [share](Self::shares) refuses
	/// [`Sharing::Shared`] with an error.
	fn write_run(&self, sharing: Sharing) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>>;

	/// Read run `run` from its first part.
	fn read_run(&self, run: &RunRef) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>>;

	/// Delete run `run`. The store may remove it later, but never hands it
	/// out again. Deleting a run that does not exist is not an error.
	fn delete_run(&self, run: &RunRef);
}

/// A run being written, a part at a time.
pub trait RunWrite: Send {
	/// Append `part` to the run.
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>>;

	/// Close the run, answering its name, whose [`RunRef::bytes`] is the sum
	/// of the parts written.
	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunRef>>;
}

/// A run being read, a part at a time.
pub trait RunRead: Send {
	/// The run's next part, or `None` once every part has been read.
	fn next_part(&mut self) -> BoxFut<'_, anyhow::Result<Option<Vec<u8>>>>;
}

/// The names of the sessions of every [`MemTempStore`] in the process.
static NEXT_MEM_SESSION: AtomicU64 = AtomicU64::new(1);

type MemRuns = Arc<Mutex<HashMap<(SessionName, RunId), Vec<Vec<u8>>>>>;

/// A store keeping its runs in memory: for tests, and as a stand-in for a
/// host's object store.
pub struct MemTempStore {
	session: SessionName,
	shares: bool,
	part_size: usize,
	quota: Option<u64>,
	next: AtomicU64,
	runs: MemRuns,
	written: Arc<AtomicUsize>,
	/// The runs being read now, and the most read at once.
	reading: Arc<(AtomicUsize, AtomicUsize)>,
	parts_read: Arc<AtomicUsize>,
	parts_written: Arc<AtomicUsize>,
}

impl std::fmt::Debug for MemTempStore {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MemTempStore")
			.field("session", &self.session)
			.field("part_size", &self.part_size)
			.finish()
	}
}

impl MemTempStore {
	fn session_name() -> SessionName {
		SessionName::new(format!("mem-{}", NEXT_MEM_SESSION.fetch_add(1, Ordering::Relaxed)))
	}

	/// A store writing parts of `part_size` bytes, with `quota` per query. Its
	/// runs are readable only through it.
	pub fn new(part_size: usize, quota: Option<u64>) -> Self {
		Self {
			session: Self::session_name(),
			shares: false,
			part_size: part_size.max(1),
			quota,
			next: AtomicU64::new(1),
			runs: Default::default(),
			written: Default::default(),
			reading: Default::default(),
			parts_read: Default::default(),
			parts_written: Default::default(),
		}
	}

	/// Two handles over one backend, each its own session, that share the runs
	/// either writes [`Sharing::Shared`]: the stand-in for a backend that
	/// several nodes reach. The counters below count across both.
	pub fn shared(part_size: usize, quota: Option<u64>) -> (Self, Self) {
		let first = Self {
			shares: true,
			..Self::new(part_size, quota)
		};
		let second = Self {
			session: Self::session_name(),
			shares: true,
			part_size: first.part_size,
			quota,
			next: AtomicU64::new(1),
			runs: Arc::clone(&first.runs),
			written: Arc::clone(&first.written),
			reading: Arc::clone(&first.reading),
			parts_read: Arc::clone(&first.parts_read),
			parts_written: Arc::clone(&first.parts_written),
		};
		(first, second)
	}

	/// The runs the backend holds now.
	pub fn runs(&self) -> usize {
		self.runs.lock().unwrap_or_else(|e| e.into_inner()).len()
	}

	/// The bytes the backend holds now.
	pub fn bytes(&self) -> usize {
		let runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
		runs.values().flatten().map(Vec::len).sum()
	}

	/// The most runs read at once since the backend was made.
	pub fn most_read_at_once(&self) -> usize {
		self.reading.1.load(Ordering::Relaxed)
	}

	/// The runs ever written to the backend.
	pub fn runs_written(&self) -> usize {
		self.written.load(Ordering::Relaxed)
	}

	/// The parts ever read from the backend.
	pub fn parts_read(&self) -> usize {
		self.parts_read.load(Ordering::Relaxed)
	}

	/// The parts ever written to the backend.
	pub fn parts_written(&self) -> usize {
		self.parts_written.load(Ordering::Relaxed)
	}

	/// Whether this handle may read or delete `run`.
	fn reaches(&self, run: &RunRef) -> bool {
		run.session == self.session || (self.shares && run.tier == Tier::Shared)
	}

	/// Join `run`'s parts into one, apply `f` to the bytes, and store them
	/// back as the run's only part.
	#[cfg(test)]
	pub(crate) fn tamper(&self, run: &RunRef, f: impl FnOnce(&mut Vec<u8>)) {
		let mut runs = self.runs.lock().unwrap_or_else(|e| e.into_inner());
		let parts = runs.get_mut(&(run.session.clone(), run.id)).expect("the run exists");
		let mut bytes = parts.concat();
		f(&mut bytes);
		*parts = vec![bytes];
	}
}

impl TempStore for MemTempStore {
	fn part_size(&self) -> usize {
		self.part_size
	}

	fn quota(&self) -> Option<u64> {
		self.quota
	}

	fn session(&self) -> &SessionName {
		&self.session
	}

	fn shares(&self) -> bool {
		self.shares
	}

	fn write_run(&self, sharing: Sharing) -> BoxFut<'_, anyhow::Result<Box<dyn RunWrite>>> {
		Box::pin(async move {
			if sharing == Sharing::Shared && !self.shares {
				return Err(cannot_share());
			}
			let tier = match sharing {
				Sharing::Shared => Tier::Shared,
				Sharing::Private => Tier::Local,
			};
			Ok(Box::new(MemRunWrite {
				run: RunRef {
					session: self.session.clone(),
					id: self.next.fetch_add(1, Ordering::Relaxed),
					tier,
					bytes: 0,
				},
				parts: Vec::new(),
				runs: Arc::clone(&self.runs),
				written: Arc::clone(&self.written),
				parts_written: Arc::clone(&self.parts_written),
			}) as Box<dyn RunWrite>)
		})
	}

	fn read_run(&self, run: &RunRef) -> BoxFut<'_, anyhow::Result<Box<dyn RunRead>>> {
		let run = run.clone();
		Box::pin(async move {
			if !self.reaches(&run) {
				return Err(not_readable());
			}
			let parts = self
				.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.get(&(run.session, run.id))
				.cloned()
				.ok_or_else(|| anyhow::anyhow!("A spilled run is missing from its store"))?;
			let now = self.reading.0.fetch_add(1, Ordering::Relaxed) + 1;
			self.reading.1.fetch_max(now, Ordering::Relaxed);
			Ok(Box::new(MemRunRead {
				parts: parts.into_iter(),
				reading: Arc::clone(&self.reading),
				parts_read: Arc::clone(&self.parts_read),
			}) as Box<dyn RunRead>)
		})
	}

	fn delete_run(&self, run: &RunRef) {
		if self.reaches(run) {
			self.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.remove(&(run.session.clone(), run.id));
		}
	}
}

struct MemRunWrite {
	run: RunRef,
	parts: Vec<Vec<u8>>,
	runs: MemRuns,
	written: Arc<AtomicUsize>,
	parts_written: Arc<AtomicUsize>,
}

impl RunWrite for MemRunWrite {
	fn write_part(&mut self, part: Vec<u8>) -> BoxFut<'_, anyhow::Result<()>> {
		self.run.bytes += part.len() as u64;
		self.parts.push(part);
		self.parts_written.fetch_add(1, Ordering::Relaxed);
		Box::pin(async { Ok(()) })
	}

	fn finish(self: Box<Self>) -> BoxFut<'static, anyhow::Result<RunRef>> {
		Box::pin(async move {
			let this = *self;
			this.runs
				.lock()
				.unwrap_or_else(|e| e.into_inner())
				.insert((this.run.session.clone(), this.run.id), this.parts);
			this.written.fetch_add(1, Ordering::Relaxed);
			Ok(this.run)
		})
	}
}

struct MemRunRead {
	parts: std::vec::IntoIter<Vec<u8>>,
	reading: Arc<(AtomicUsize, AtomicUsize)>,
	parts_read: Arc<AtomicUsize>,
}

impl Drop for MemRunRead {
	fn drop(&mut self) {
		self.reading.0.fetch_sub(1, Ordering::Relaxed);
	}
}

impl RunRead for MemRunRead {
	fn next_part(&mut self) -> BoxFut<'_, anyhow::Result<Option<Vec<u8>>>> {
		let part = self.parts.next();
		if part.is_some() {
			self.parts_read.fetch_add(1, Ordering::Relaxed);
		}
		Box::pin(async move { Ok(part) })
	}
}
