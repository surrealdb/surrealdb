//! The bytes a loop holds only because it fanned out, as a lease the coordinator can refuse.
//!
//! A query never fails because it fanned out. A worker's copy of the statement's slots is such a
//! byte: the serial run holds one copy and the fan holds one more per worker, so a ceiling the
//! serial run stays under can be passed by the copies alone. [`FanoutLease`] is where those bytes
//! are asked for. The coordinator asks for a [`Grant`] before it hands a batch or a morsel's
//! worker its copy, and a refusal is answered by running the batch on the coordinator, which
//! answers what the worker would have. A batch run on the coordinator charges nothing: the serial
//! run makes no such copy, so no byte of it may count toward a ceiling a later charge of the
//! statement is checked against.
//!
//! # What a grant admits
//!
//! A grant admits exactly what the account of a state's copied slots admits: the bytes are
//! admitted while the query's pinned total and the bytes of the live grants together stay within
//! the query's ceiling, and the lease is then made good for them, waiting on the memory governor
//! or overcommitting as it does for the statement's own slots. Only where either fails, which is
//! where a run holding the copy would raise, is the grant refused. The error is not raised: a
//! timeout or a cancellation is the coordinator's to raise at its next check, and the memory
//! threshold error is one the serial run never raises for a copy it does not make.
//!
//! The bytes of a grant are reserved from the lease and are never added to the query's counted
//! pinned total, so a charge the serial run makes is judged against the bytes the serial run holds
//! and no grant, however long it lives, can make it raise. The lease sees them, so the query's
//! breakers spill sooner under them, which does not change an answer.
//!
//! A refusal revokes the lease for the rest of the run. A revoked lease grants nothing, so no
//! batch or morsel is handed on after it; batches already on workers finish under their grants,
//! and a Range morsel parks at its next chunk boundary so that the coordinator runs what remains
//! of its range itself, in order. A run is never widened again once revoked.
//!
//! The bytes a grant covers are the sum of [`Under::kept`](super::run::Under), a worker's copy of
//! the slots. The same API reserves the bytes of a write candidate before it is offered to the
//! apply window, and the bytes of the captured view of a write. A morsel's output that waits in the
//! reorder window stays bounded by the window's own parking and is not charged here.
//!
//! # Raise sites in `morsel/**`
//!
//! Every site of the morsel code that can raise a memory error, by what it guards:
//!
//! | Site | Class | Treatment |
//! |---|---|---|
//! | `run.rs` `batch_state` → `State::keep_copied` → `Reservation::insist` | fan-out only: bytes that exist because a worker copies the slots | a state whose copy a grant covers, or that runs on the coordinator, does not charge it; the grant is asked for before the hand-out |
//! | `run.rs`, `set.rs` `ensure_below_memory_threshold` | the process allocation threshold, which the serial scan checks at the same points (`operators/scan/common.rs`) | checked as in the serial run; the lease does not cover it |
//! | `run.rs`, `merge.rs` `spill_breakers` | a breaker the lease does not cover spills, as the serial run spills the same breakers | spills as in the serial run; the lease does not cover it |
//!
//! `QueryMemory::within_ceiling` and `QueryMemory::insist` are reached in `morsel/**` only through
//! the sites above and through [`FanoutLease::grant`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crate::exec::ExecutionContext;
use crate::exec::memory::QueryMemory;

/// Bytes held only because a loop fanned out: revocable, never the query-ceiling error.
pub(super) struct FanoutLease {
	/// The query's memory the grants are drawn from; `None` where nothing is bounded.
	memory: Option<Arc<QueryMemory>>,
	/// Set by the first refusal, for the rest of the run.
	revoked: AtomicBool,
	/// The bytes the live grants hold.
	held: AtomicUsize,
}

/// Bytes one hand-out holds from the lease; released on drop.
pub(super) struct Grant {
	/// The query's memory whose lease `reserved` was taken from; `None` where nothing was.
	memory: Option<Arc<QueryMemory>>,
	/// The bytes reserved from the query's lease as bytes it cannot spill.
	reserved: usize,
	bytes: usize,
	lease: Arc<FanoutLease>,
}

impl Drop for Grant {
	fn drop(&mut self) {
		if self.reserved > 0
			&& let Some(memory) = &self.memory
		{
			memory.lease().release_pinned(self.reserved);
		}
		self.lease.held.fetch_sub(self.bytes, Ordering::AcqRel);
	}
}

impl FanoutLease {
	/// A lease over `memory`, the query's, which grants without bound when there is none.
	pub(super) fn new(memory: Option<Arc<QueryMemory>>) -> Arc<Self> {
		Arc::new(Self {
			memory,
			revoked: AtomicBool::new(false),
			held: AtomicUsize::new(0),
		})
	}

	/// A grant of `bytes`, or `None` where a state holding a copy of that size would raise: the
	/// refusal revokes the lease for the rest of the run, and a revoked lease grants nothing.
	///
	/// A grant of no bytes makes no reservation. With no query memory every grant is made.
	pub(super) async fn grant(
		self: &Arc<Self>,
		bytes: usize,
		ctx: &ExecutionContext,
	) -> Option<Grant> {
		if self.revoked() {
			return None;
		}
		#[cfg(test)]
		if super::test_hooks::grant_fails(ctx) {
			self.revoked.store(true, Ordering::Release);
			return None;
		}
		let Some(memory) = self.memory.as_ref().filter(|_| bytes > 0) else {
			return Some(self.granted(None, 0, bytes));
		};
		// Grants are asked for one at a time by the coordinator, so the bytes held now are the
		// bytes held when this one is made.
		let admitted = memory.within_ceiling(self.held().saturating_add(bytes)).is_ok()
			&& memory.insist(bytes, ctx).await.is_ok();
		if !admitted {
			self.revoked.store(true, Ordering::Release);
			return None;
		}
		Some(self.granted(Some(Arc::clone(memory)), bytes, bytes))
	}

	fn granted(
		self: &Arc<Self>,
		memory: Option<Arc<QueryMemory>>,
		reserved: usize,
		bytes: usize,
	) -> Grant {
		self.held.fetch_add(bytes, Ordering::AcqRel);
		Grant {
			memory,
			reserved,
			bytes,
			lease: Arc::clone(self),
		}
	}

	/// Whether a grant was refused: no batch or morsel is handed to a worker from now on.
	pub(super) fn revoked(&self) -> bool {
		self.revoked.load(Ordering::Acquire)
	}

	/// The bytes the live grants hold.
	pub(super) fn held(&self) -> usize {
		self.held.load(Ordering::Acquire)
	}
}

#[cfg(test)]
mod tests {
	use std::time::Duration;

	use super::*;
	use crate::exec::memory::{FairGovernor, MemoryHost, MemorySettings, Verdict};
	use crate::exec::operators::test_util::root_ctx;

	/// A query's memory that holds each query to `max` pinned bytes, with chunks of 64 bytes and no
	/// wait on the governor.
	fn memory(max: usize) -> Arc<QueryMemory> {
		let host = MemoryHost {
			governor: Arc::new(FairGovernor::new(1 << 20)),
			store: None,
			settings: MemorySettings {
				floor: 100,
				fan_in: 2,
				chunk: 64,
				wait: Duration::ZERO,
				max,
			},
		};
		host.query()
	}

	/// Grants are made while the pinned total and the live grants together stay within the ceiling
	/// and refused past it; the refusal revokes the lease for good, and what the live grants hold
	/// returns to nothing as they drop.
	#[tokio::test]
	async fn lease_grant_and_revoke() {
		let ctx = root_ctx();
		let memory = memory(1000);
		let lease = FanoutLease::new(Some(Arc::clone(&memory)));

		let first = lease.grant(600, &ctx).await.expect("within the ceiling");
		let second = lease.grant(300, &ctx).await.expect("within the ceiling");
		assert_eq!(lease.held(), 900);
		assert!(!lease.revoked());

		assert!(lease.grant(200, &ctx).await.is_none(), "past the ceiling");
		assert!(lease.revoked());
		assert_eq!(lease.held(), 900, "a refused grant held bytes");
		assert_eq!(memory.pinned(), 0, "a grant counted toward the pinned ceiling");

		drop(first);
		assert!(lease.grant(1, &ctx).await.is_none(), "a revoked lease granted");
		assert!(lease.grant(0, &ctx).await.is_none(), "a revoked lease granted nothing");

		drop(second);
		assert_eq!(lease.held(), 0);
		assert_eq!(memory.lease().pinned(), 0, "reserved bytes outlived their grants");
	}

	/// A live grant is no reason for a charge the serial run makes to raise: the pinned total a
	/// charge is checked against holds none of its bytes, however much they are.
	#[tokio::test]
	async fn a_grant_leaves_the_pinned_ceiling_to_parity_charges() {
		let ctx = root_ctx();
		let memory = memory(1000);
		let lease = FanoutLease::new(Some(Arc::clone(&memory)));
		let grant = lease.grant(900, &ctx).await.expect("within the ceiling");
		let mut account = memory.pinned_reservation();
		assert_eq!(account.settle(800), Verdict::Hold, "a charge the serial run makes");
		account.insist(&ctx).await.expect("the grant is not counted");
		assert_eq!(memory.pinned(), 800);
		drop(account);
		drop(grant);
	}

	/// A grant of no bytes makes no reservation, whatever the ceiling.
	#[tokio::test]
	async fn a_zero_byte_grant_reserves_nothing() {
		let ctx = root_ctx();
		let memory = memory(1);
		let lease = FanoutLease::new(Some(Arc::clone(&memory)));
		let grant = lease.grant(0, &ctx).await.expect("a copy of nothing");
		assert_eq!((lease.held(), memory.pinned()), (0, 0));
		assert!(!lease.revoked());
		drop(grant);
	}

	/// With no query memory nothing bounds the run, so every grant is made.
	#[tokio::test]
	async fn lease_unbounded_without_memory() {
		let ctx = root_ctx();
		let lease = FanoutLease::new(None);
		let grants = [
			lease.grant(usize::MAX / 4, &ctx).await.expect("unbounded"),
			lease.grant(usize::MAX / 4, &ctx).await.expect("unbounded"),
			lease.grant(0, &ctx).await.expect("unbounded"),
		];
		assert!(!lease.revoked());
		assert_eq!(lease.held(), usize::MAX / 4 * 2);
		drop(grants);
		assert_eq!(lease.held(), 0);
	}
}
