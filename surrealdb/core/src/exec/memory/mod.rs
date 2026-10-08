//! Bounded memory: the node's governor, each query's lease, and the host's
//! temporary store that breakers spill to.
//!
//! A query's breakers — a sort, an aggregate's groups, a distinct's seen
//! rows, a join's build — hold their state in memory while their query's
//! lease covers it, and spill it to the temporary store once it does not.
//! Each breaker charges what it holds to a [`Reservation`], which reserves
//! from the lease in chunks, and checks at each batch it hands on whether
//! the lease still covers it: a governor may lower a lease's grant while the
//! query runs ([`governor`]), and the breaker over its grant spills there.
//!
//! What a breaker spills is a run ([`runs`]): written through one part
//! buffer and read back through one, so the memory a spill takes is bounded
//! whatever it spills. That memory is the lease's headroom, part of the
//! minimum the lease is guaranteed: one part buffer to write a run and one
//! per run a merge reads at once ([`QueryMemory::fan_in`]). A merge of more
//! runs than that takes several passes.
//!
//! A statement fails for want of memory — with
//! [`DatastoreError::QueryBeyondMemoryThreshold`](crate::kvs::DatastoreError),
//! the error the process's memory threshold raises — only where spilling
//! cannot bound it: a breaker with nothing left to spill over a grant at its
//! minimum, as a single value larger than the grant leaves it, a query with
//! no temporary store over its grant, or a whole result a value must hold
//! (a subquery's rows) that outgrows the grant.

pub(crate) mod governor;
pub(crate) mod runs;
pub(crate) mod size;
pub(crate) mod store;
pub(crate) mod tiered;

#[cfg(all(storage, not(target_family = "wasm")))]
pub(crate) mod file_store;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

#[cfg(all(storage, not(target_family = "wasm")))]
pub use file_store::FileTempStore;
pub use governor::{FairGovernor, Lease, MemoryGovernor, Unlimited};
pub use store::{MemTempStore, RunId, RunRead, RunWrite, TempStore};
pub use tiered::TieredTempStore;

use crate::kvs::DatastoreError;

/// The settings a query's memory is held under, read from the datastore's
/// configuration.
#[derive(Debug, Clone, Copy)]
pub(crate) struct MemorySettings {
	/// The bytes every lease is guaranteed beyond its spill headroom.
	pub floor: usize,
	/// The runs a merge reads at once.
	pub fan_in: usize,
	/// The bytes a breaker reserves from its lease at a time.
	pub chunk: usize,
}

/// The node's memory governor and temporary store, which every query of a
/// datastore shares.
#[derive(Clone)]
pub(crate) struct MemoryHost {
	pub governor: Arc<dyn MemoryGovernor>,
	pub store: Option<Arc<dyn TempStore>>,
	pub settings: MemorySettings,
}

impl std::fmt::Debug for MemoryHost {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("MemoryHost")
			.field("store", &self.store.is_some())
			.field("settings", &self.settings)
			.finish()
	}
}

impl MemoryHost {
	/// A query's memory under this host, its lease not yet opened.
	pub(crate) fn query(&self) -> Arc<QueryMemory> {
		Arc::new(QueryMemory {
			host: self.clone(),
			lease: OnceLock::new(),
			spilled: AtomicU64::new(0),
			unspillable: std::sync::Mutex::new((0, 0)),
			writes: std::sync::Mutex::new((0, 0)),
		})
	}
}

/// One query's memory: its lease, opened when a breaker first needs it, and
/// what it has spilled to the store.
pub(crate) struct QueryMemory {
	host: MemoryHost,
	lease: OnceLock<Arc<Lease>>,
	spilled: AtomicU64,
	/// What the query has added to buffers it cannot spill — its
	/// transaction's change-feed and live-event buffers — and reserved from
	/// its lease for them, as `(held, reserved)`.
	unspillable: std::sync::Mutex<(usize, usize)>,
	/// The transaction whose write buffer the query last charged, by
	/// address, and the bytes it had written then.
	writes: std::sync::Mutex<(usize, u64)>,
}

impl std::fmt::Debug for QueryMemory {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("QueryMemory")
			.field("lease", &self.lease.get())
			.field("spilled", &self.spilled.load(Ordering::Relaxed))
			.finish()
	}
}

impl QueryMemory {
	/// The query's lease, opened with its minimum on first use: the floor,
	/// and the spill headroom where the query has a store to spill to.
	pub(crate) fn lease(&self) -> &Arc<Lease> {
		self.lease.get_or_init(|| {
			let headroom = self.headroom();
			Lease::open(
				&self.host.governor,
				self.host.settings.floor.saturating_add(headroom),
				headroom,
			)
		})
	}

	/// The memory spilling takes: one part buffer to write a run, and one per
	/// run a merge reads at once.
	fn headroom(&self) -> usize {
		match &self.host.store {
			Some(store) => store.part_size().saturating_mul(1 + self.fan_in()),
			None => 0,
		}
	}

	/// The store the query spills to, if the host has one.
	pub(crate) fn store(&self) -> Option<Arc<dyn TempStore>> {
		self.host.store.clone()
	}

	/// What the query's operators may hold now: its grant less its headroom.
	pub(crate) fn working(&self) -> usize {
		let lease = self.lease();
		lease.grant().saturating_sub(lease.headroom())
	}

	/// Whether the query can spill.
	pub(crate) fn can_spill(&self) -> bool {
		self.host.store.is_some()
	}

	/// The runs a merge reads at once.
	pub(crate) fn fan_in(&self) -> usize {
		self.host.settings.fan_in.max(2)
	}

	/// A breaker's account against the query's lease.
	pub(crate) fn reservation(self: &Arc<Self>) -> Reservation {
		Reservation {
			memory: Some(Arc::clone(self)),
			reserved: 0,
			used: 0,
			refused: false,
		}
	}

	/// Count `bytes` more held in a buffer the query cannot spill, such as its
	/// transaction's change-feed buffer, which holds them until it commits.
	/// Reserved from the lease in chunks; once the lease, at its minimum, no
	/// longer covers them, the query fails with the memory threshold's error
	/// rather than grow. Above its minimum, the query's breakers give memory
	/// back by spilling.
	pub(crate) fn hold_unspillable(&self, bytes: usize) -> anyhow::Result<()> {
		let mut held = self.unspillable.lock().unwrap_or_else(|e| e.into_inner());
		held.0 = held.0.saturating_add(bytes);
		if held.0 <= held.1 {
			return Ok(());
		}
		let chunk = self.host.settings.chunk.max(1);
		let want = (held.0 - held.1).div_ceil(chunk) * chunk;
		let lease = self.lease();
		if lease.reserve(want) {
			held.1 += want;
			return Ok(());
		}
		if lease.at_minimum() {
			anyhow::bail!(DatastoreError::QueryBeyondMemoryThreshold);
		}
		Ok(())
	}

	/// Charge what transaction `txn` (named by its address) has written so
	/// far, `written` bytes, which its write buffer holds until it commits
	/// and which cannot be spilled: the growth since the last charge of the
	/// same transaction counts as [`Self::hold_unspillable`] does.
	pub(crate) fn hold_writes(&self, txn: usize, written: u64) -> anyhow::Result<()> {
		let grown = {
			let mut last = self.writes.lock().unwrap_or_else(|e| e.into_inner());
			if last.0 != txn {
				*last = (txn, 0);
			}
			let grown = written.saturating_sub(last.1);
			last.1 = last.1.max(written);
			grown
		};
		match grown {
			0 => Ok(()),
			grown => self.hold_unspillable(grown as usize),
		}
	}

	/// Count `bytes` written to the store against its quota.
	pub(crate) fn spilled(&self, bytes: u64) -> anyhow::Result<()> {
		let total = self.spilled.fetch_add(bytes, Ordering::AcqRel) + bytes;
		if let Some(quota) = self.host.store.as_ref().and_then(|store| store.quota())
			&& total > quota
		{
			anyhow::bail!(
				"The query spilled more than its temporary store's quota of {quota} bytes"
			);
		}
		Ok(())
	}

	/// Count `bytes` of a deleted run as no longer spilled.
	pub(crate) fn unspilled(&self, bytes: u64) {
		self.spilled.fetch_sub(bytes, Ordering::AcqRel);
	}
}

/// What one breaker holds, reserved from its query's lease in chunks.
///
/// A breaker charges each entry it holds and clears the account when it
/// spills or finishes; the account reserves from the lease only when what it
/// holds passes what it has reserved, a chunk at a time. Without a query's
/// memory (a context no datastore query set up) every charge is admitted.
#[derive(Debug, Default)]
pub(crate) struct Reservation {
	memory: Option<Arc<QueryMemory>>,
	reserved: usize,
	used: usize,
	/// Whether a charge since the account was last cleared found the lease
	/// short.
	refused: bool,
}

/// What a breaker must do once it has charged what it holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
	/// The lease covers what it holds.
	Hold,
	/// The lease does not cover what it holds: spill.
	Spill,
}

impl Reservation {
	/// An account with no lease, admitting every charge.
	pub(crate) fn unbounded() -> Self {
		Self::default()
	}

	/// The query's memory this account draws on, if any.
	pub(crate) fn memory(&self) -> Option<&Arc<QueryMemory>> {
		self.memory.as_ref()
	}

	/// Charge `bytes` more, answering whether the lease still covers what the
	/// account holds.
	pub(crate) fn charge(&mut self, bytes: usize) -> Verdict {
		self.used = self.used.saturating_add(bytes);
		let Some(memory) = &self.memory else {
			return Verdict::Hold;
		};
		if self.used <= self.reserved {
			return Verdict::Hold;
		}
		let chunk = memory.host.settings.chunk.max(1);
		let want = (self.used - self.reserved).div_ceil(chunk) * chunk;
		if memory.lease().reserve(want) {
			self.reserved += want;
			Verdict::Hold
		} else {
			self.refused = true;
			Verdict::Spill
		}
	}

	/// Charge what the breaker holds in all, `total`, where the breaker
	/// measures its whole state rather than each entry it takes.
	pub(crate) fn hold(&mut self, total: usize) -> Verdict {
		if total > self.used {
			self.charge(total - self.used)
		} else {
			self.used = total;
			Verdict::Hold
		}
	}

	/// Whether the breaker should spill what it holds: a charge found the
	/// lease short, or a lowered grant leaves the query holding more than it
	/// and this account holds at least a chunk of that.
	pub(crate) fn wants_spill(&self) -> bool {
		match &self.memory {
			None => false,
			Some(memory) => {
				self.refused
					|| (self.used >= memory.host.settings.chunk.max(1) && memory.lease().over())
			}
		}
	}

	/// The error a breaker raises that must hold more than its lease covers
	/// and cannot spill it.
	pub(crate) fn exhausted(&self) -> anyhow::Error {
		anyhow::Error::new(DatastoreError::QueryBeyondMemoryThreshold)
	}

	/// Give back everything reserved: the breaker has spilled or finished.
	pub(crate) fn clear(&mut self) {
		if let Some(memory) = &self.memory
			&& self.reserved > 0
		{
			memory.lease().release(self.reserved);
		}
		self.reserved = 0;
		self.used = 0;
		self.refused = false;
	}
}

impl Drop for Reservation {
	fn drop(&mut self) {
		self.clear();
	}
}
