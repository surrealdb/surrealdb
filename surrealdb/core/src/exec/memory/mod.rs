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
//! A statement never fails for want of the governor's memory. What a breaker
//! holds spills to the temporary store when the lease does not cover it.
//! What cannot be spilled — a gathered value, a value a program binds or
//! builds, a transaction's buffers, a breaker with no store — is pinned and
//! held through [`QueryMemory::insist`]: the lease grows, or the query waits,
//! for at most the configured wait in all, for other queries to give memory
//! back, or the governor grants the bytes past its limit. Bytes held past the
//! grant are the lease's overcommit until its grant covers them or it
//! releases them.
//!
//! A query fails with the memory threshold error when its own pinned bytes
//! would pass its ceiling ([`QueryMemory::within_ceiling`], the
//! `query_memory_max` setting), and never because of another query's use: the
//! check reads only the query's own pinned bytes. Otherwise a statement fails
//! only as its context does (cancelled, timed out), or as the process memory
//! threshold or a store's quota fails it.
//!
//! The values a program holds across instructions are charged where it keeps
//! them: its bound parameters, the frame copies of them, a `FOR` loop's
//! source, the values a once-cache keeps, the arguments of a function or
//! closure call, and a region worker's copy of its slots. A value built and
//! not kept is transient within one statement step and is not charged: an
//! expression's intermediate results and a builtin's unbound result, whose
//! generators are capped per call by `generation_allocation_limit` and whose
//! combinators are bounded by a small multiple of what is charged; a
//! function's or closure's result, which is charged where its caller binds
//! it; the rows a statement streams to its client, which the transport owns,
//! and the one batch of encoded rows a streamed statement holds; the shared
//! `Arc` parameters a name resolution rebuilds its frame from; a sort's slab
//! of rows, which is spillable breaker state charged to the sort; and the
//! script runtime's heap, bounded by `scripting_max_memory_limit`.

pub(crate) mod governor;
pub(crate) mod runs;
pub(crate) mod size;
pub(crate) mod store;
pub(crate) mod tiered;

#[cfg(all(storage, not(target_family = "wasm")))]
pub(crate) mod file_store;

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

#[cfg(all(storage, not(target_family = "wasm")))]
pub use file_store::FileTempStore;
pub use governor::{Admit, BoxFut, FairGovernor, Lease, MemoryGovernor, Unlimited};
pub use store::{MemTempStore, RunId, RunRead, RunWrite, TempStore};
pub use tiered::TieredTempStore;

use crate::exec::{ExecutionContext, FlowResult};
use crate::expr::ControlFlow;
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
	/// The longest a query waits for other queries to give memory back before
	/// it is granted memory past the node's limit.
	pub wait: Duration,
	/// The bytes a single query may hold that it cannot spill before it fails
	/// with the memory threshold error. Bounds the query's own pinned bytes
	/// alone.
	pub max: usize,
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
			waited: AtomicU64::new(0),
			unspillable: std::sync::Mutex::new((0, 0)),
			writes: std::sync::Mutex::new((0, 0)),
			counted: AtomicUsize::new(0),
		})
	}
}

/// One query's memory: its lease, opened when a breaker first needs it, and
/// what it has spilled to the store.
pub(crate) struct QueryMemory {
	host: MemoryHost,
	lease: OnceLock<Arc<Lease>>,
	spilled: AtomicU64,
	/// The nanoseconds the query has spent waiting in [`QueryMemory::insist`],
	/// all its calls together.
	waited: AtomicU64,
	/// What the query has added to buffers it cannot spill — its
	/// transaction's change-feed and live-event buffers — and reserved from
	/// its lease for them, as `(held, reserved)`. Given back when the
	/// transaction ends ([`QueryMemory::transaction_ended`]).
	unspillable: std::sync::Mutex<(usize, usize)>,
	/// The transaction whose write buffer the query last charged, by
	/// address, and the bytes it had written then.
	writes: std::sync::Mutex<(usize, u64)>,
	/// The pinned bytes the query holds, counted exactly: what each pinned
	/// account has charged, and what its transaction's buffers hold. The
	/// ceiling is checked against it, so it is what the query holds and not
	/// the chunks its lease reserves for that. An account whose total fits in
	/// one chunk ([`Reservation::settle`]) is counted here and reserves
	/// nothing, so a query that holds only a few small values never opens a
	/// lease.
	counted: AtomicUsize,
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

	/// A breaker's account against the query's lease. A breaker with no store
	/// to spill to holds what it holds, so its account is pinned.
	pub(crate) fn reservation(self: &Arc<Self>) -> Reservation {
		self.account(!self.can_spill())
	}

	/// An account for bytes the query cannot spill, held through
	/// [`Reservation::insist`].
	pub(crate) fn pinned_reservation(self: &Arc<Self>) -> Reservation {
		self.account(true)
	}

	fn account(self: &Arc<Self>, pinned: bool) -> Reservation {
		Reservation {
			memory: Some(Arc::clone(self)),
			reserved: 0,
			used: 0,
			refused: false,
			exceeded: false,
			pinned,
			counted: 0,
		}
	}

	/// The pinned bytes the query holds, counted exactly. Does not open the
	/// lease.
	pub(crate) fn pinned(&self) -> usize {
		self.counted.load(Ordering::Acquire)
	}

	/// Count a pinned account's holding as `to` bytes in place of the `counted`
	/// it has added so far, answering whether the query then holds more than
	/// its ceiling after a growth.
	fn count(&self, counted: &mut usize, to: usize) -> bool {
		if to > *counted {
			let by = to - *counted;
			*counted = to;
			let now = self.counted.fetch_add(by, Ordering::AcqRel).saturating_add(by);
			now > self.host.settings.max
		} else {
			self.uncount(*counted - to);
			*counted = to;
			false
		}
	}

	/// Stop counting `bytes` of the pinned bytes the query holds.
	fn uncount(&self, bytes: usize) {
		if bytes > 0 {
			let _ = self.counted.fetch_update(Ordering::AcqRel, Ordering::Acquire, |held| {
				Some(held.saturating_sub(bytes))
			});
		}
	}

	/// Refuse `bytes` more pinned bytes when the query would then hold more than
	/// its ceiling, with the memory threshold error. Reads only this query's
	/// own pinned bytes, counted exactly, so no other query's use can raise it
	/// and the rounding of reservations to chunks does not. Tasks of one query
	/// running at once may pass the ceiling by the bytes each adds between its
	/// check and its count.
	pub(crate) fn within_ceiling(&self, bytes: usize) -> FlowResult<()> {
		if self.pinned().saturating_add(bytes) > self.host.settings.max {
			let error = anyhow::Error::new(DatastoreError::QueryBeyondMemoryThreshold);
			return Err(ControlFlow::Err(error));
		}
		Ok(())
	}

	/// Reserve `bytes` more from the lease for pinned bytes the caller has
	/// counted ([`Self::count`], [`Self::within_ceiling`]): the lease grows, or
	/// the query waits for other queries to give memory back, or the bytes are
	/// granted past the node's limit. Fails only as the query's context fails:
	/// cancelled, or past its deadline.
	///
	/// The waits of all a query's calls together last at most the configured
	/// wait, and each at most half the time the context has left, so a query
	/// that waits on something the governor cannot see still ends its wait. A
	/// wait is cancellation safe: dropping the future leaves the lease no longer
	/// waiting.
	pub(crate) async fn insist(&self, bytes: usize, ctx: &ExecutionContext) -> FlowResult<()> {
		if bytes == 0 {
			return Ok(());
		}
		let lease = self.lease();
		let governor = &self.host.governor;
		let allowed = self
			.host
			.settings
			.wait
			.saturating_sub(Duration::from_nanos(self.waited.load(Ordering::Acquire)));
		let started = web_time::Instant::now();
		let mut may_wait = !allowed.is_zero();
		loop {
			let epoch = match governor.admit(lease, bytes, may_wait) {
				Admit::Granted | Admit::Overcommit => return Ok(()),
				Admit::Wait {
					epoch,
				} => epoch,
			};
			let mut guard = WaitGuard {
				governor: governor.as_ref(),
				lease,
				armed: true,
			};
			let left = allowed.saturating_sub(started.elapsed());
			let slice = match ctx.ctx().timeout() {
				Some(remaining) => left.min(remaining / 2),
				None => left,
			};
			let waiting = web_time::Instant::now();
			let ran_out = tokio::select! {
				woken = common::time::timeout(slice, governor.changed(epoch)) => woken.is_err(),
				_ = ctx.cancellation().cancelled() => false,
			};
			let spent = u64::try_from(waiting.elapsed().as_nanos()).unwrap_or(u64::MAX);
			self.waited.fetch_add(spent, Ordering::AcqRel);
			super::vm::fuel::interrupted(ctx)?;
			// The next ask re-decides and clears the wait. A slice that ran out,
			// whether the configured wait's or the deadline's share, is the last;
			// a wake-up by the governor keeps the rest of the wait, so a lease
			// behind an older waiter stays behind it.
			guard.armed = false;
			if ran_out || started.elapsed() >= allowed {
				may_wait = false;
			}
		}
	}

	/// [`Self::insist`] without waiting, for a caller that cannot suspend: the
	/// bytes are granted, past the node's limit where the node has no memory
	/// for them.
	pub(crate) fn insist_now(&self, bytes: usize) -> FlowResult<()> {
		if bytes == 0 {
			return Ok(());
		}
		let lease = self.lease();
		let governor = &self.host.governor;
		if let Admit::Wait {
			..
		} = governor.admit(lease, bytes, false)
		{
			// A governor never waits a lease that may not wait; one that does is
			// answered by holding the bytes.
			governor.unwait(lease);
			lease.force_pinned(bytes);
		}
		Ok(())
	}

	/// Count `bytes` more held in a buffer the query cannot spill, such as its
	/// transaction's change-feed buffer, which holds them until it commits.
	/// Reserved from the lease in chunks through [`Self::insist`], so a lease
	/// short of them makes the query wait or proceed past the node's limit, and
	/// fails it only where the bytes would take its pinned bytes past its
	/// ceiling.
	pub(crate) async fn hold_unspillable(
		&self,
		bytes: usize,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
		let want = {
			let mut held = self.unspillable.lock().unwrap_or_else(|e| e.into_inner());
			self.within_ceiling(bytes)?;
			held.0 = held.0.saturating_add(bytes);
			self.counted.fetch_add(bytes, Ordering::AcqRel);
			if held.0 <= held.1 {
				return Ok(());
			}
			let chunk = self.host.settings.chunk.max(1);
			let want = (held.0 - held.1).div_ceil(chunk) * chunk;
			if self.lease().reserve_pinned(want) {
				held.1 += want;
				return Ok(());
			}
			want
		};
		self.insist(want, ctx).await?;
		self.unspillable.lock().unwrap_or_else(|e| e.into_inner()).1 += want;
		Ok(())
	}

	/// Give back what the query charged for the buffers of the transaction
	/// that has ended, committed or cancelled: its change-feed and live-event
	/// buffers and its write buffer. What a query holds for transactions is
	/// then never more than the one transaction running, however many a
	/// request runs.
	pub(crate) fn transaction_ended(&self) {
		let (held, reserved) = {
			let mut held = self.unspillable.lock().unwrap_or_else(|e| e.into_inner());
			std::mem::take(&mut *held)
		};
		self.uncount(held);
		*self.writes.lock().unwrap_or_else(|e| e.into_inner()) = (0, 0);
		if reserved > 0
			&& let Some(lease) = self.lease.get()
		{
			lease.release_pinned(reserved);
		}
	}

	/// Charge what transaction `txn` (named by its address) has written so
	/// far, `written` bytes, which its write buffer holds until it commits:
	/// the growth since the last charge of the same transaction counts as
	/// [`Self::hold_unspillable`] does: the governor sees the memory, the query
	/// waits when other queries can give memory back, and the bytes are
	/// otherwise granted past the node's limit. It fails only where the bytes
	/// would take the query's pinned bytes past its ceiling.
	pub(crate) async fn hold_writes(
		&self,
		txn: usize,
		written: u64,
		ctx: &ExecutionContext,
	) -> FlowResult<()> {
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
			grown => self.hold_unspillable(grown as usize, ctx).await,
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
	/// Whether a charge or [`Reservation::pin`] since the account was last
	/// cleared would have taken the query's pinned bytes past its ceiling. The
	/// next [`Reservation::insist`] raises the error.
	exceeded: bool,
	/// Whether what the account holds cannot be spilled, so the lease counts
	/// it as pinned and the account is made good by [`Reservation::insist`].
	pinned: bool,
	/// The bytes of `used` the account has added to the query's
	/// [`QueryMemory::counted`], which it gives back when it is cleared. Nothing
	/// for an account that is not pinned.
	counted: usize,
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
		if self.pinned && memory.count(&mut self.counted, self.used) {
			self.refused = true;
			self.exceeded = true;
			return Verdict::Spill;
		}
		if self.used <= self.reserved {
			return Verdict::Hold;
		}
		let chunk = memory.host.settings.chunk.max(1);
		let want = (self.used - self.reserved).div_ceil(chunk) * chunk;
		let lease = memory.lease();
		let reserved = if self.pinned {
			lease.reserve_pinned(want)
		} else {
			lease.reserve(want)
		};
		if reserved {
			self.reserved += want;
			Verdict::Hold
		} else {
			self.refused = true;
			Verdict::Spill
		}
	}

	/// Make good what the account holds past what it has reserved, waiting or
	/// overcommitting where the lease is short ([`QueryMemory::insist`]), and
	/// failing where a charge took the query's pinned bytes past its ceiling.
	/// The bytes are pinned, so the account is. A no-op for an account with no
	/// lease or none short.
	pub(crate) async fn insist(&mut self, ctx: &ExecutionContext) -> FlowResult<()> {
		let Some(memory) = self.memory.clone() else {
			return Ok(());
		};
		self.pin();
		if std::mem::take(&mut self.exceeded) {
			memory.within_ceiling(0)?;
		}
		if self.used > self.reserved {
			let chunk = memory.host.settings.chunk.max(1);
			let want = (self.used - self.reserved).div_ceil(chunk) * chunk;
			memory.insist(want, ctx).await?;
			self.reserved += want;
		}
		self.refused = false;
		Ok(())
	}

	/// [`insist`](Self::insist) without waiting ([`QueryMemory::insist_now`]).
	pub(crate) fn insist_now(&mut self) -> FlowResult<()> {
		let Some(memory) = self.memory.clone() else {
			return Ok(());
		};
		self.pin();
		if std::mem::take(&mut self.exceeded) {
			memory.within_ceiling(0)?;
		}
		if self.used > self.reserved {
			let chunk = memory.host.settings.chunk.max(1);
			let want = (self.used - self.reserved).div_ceil(chunk) * chunk;
			memory.insist_now(want)?;
			self.reserved += want;
		}
		self.refused = false;
		Ok(())
	}

	/// Take over what `other` has reserved, without giving it back to the lease
	/// and reserving it again. Both are accounts of the same query.
	pub(crate) fn absorb(&mut self, mut other: Reservation) {
		self.reserved += std::mem::take(&mut other.reserved);
		other.used = 0;
	}

	/// Charge what the holder keeps in all, `total`, where it measures its
	/// state afresh: growth is charged as [`hold`](Self::hold) charges it, and
	/// on shrinkage the chunks reserved are given back once they number more
	/// than one beyond what `total` needs, so the lease recovers memory as the
	/// state shrinks, and a total that moves across a chunk boundary does not
	/// release and reserve a chunk with every change.
	///
	/// A pinned account that has reserved nothing and whose total fits in one
	/// chunk reserves nothing: the bytes are counted exactly toward the query's
	/// ceiling ([`QueryMemory::counted`]) and the lease is neither opened nor
	/// locked, so holders of many small values cost the governor nothing. The
	/// governor's resolution of a pinned total is one chunk: it does not see
	/// those bytes.
	pub(crate) fn settle(&mut self, total: usize) -> Verdict {
		if self.pinned
			&& self.reserved == 0
			&& let Some(memory) = &self.memory
			&& total <= memory.host.settings.chunk.max(1)
		{
			self.used = total;
			return match memory.count(&mut self.counted, total) {
				true => {
					self.refused = true;
					self.exceeded = true;
					Verdict::Spill
				}
				false => Verdict::Hold,
			};
		}
		let verdict = if total > self.used {
			self.charge(total - self.used)
		} else {
			self.shrink(total);
			Verdict::Hold
		};
		if verdict == Verdict::Hold
			&& let Some(memory) = &self.memory
		{
			let chunk = memory.host.settings.chunk.max(1);
			let keep = total.div_ceil(chunk) * chunk;
			if self.reserved > keep.saturating_add(chunk) {
				let give = self.reserved - keep;
				match self.pinned {
					true => memory.lease().release_pinned(give),
					false => memory.lease().release(give),
				}
				self.reserved = keep;
			}
		}
		verdict
	}

	/// Hold `total` bytes, which is no more than the account held.
	fn shrink(&mut self, total: usize) {
		self.used = total;
		if self.pinned
			&& let Some(memory) = &self.memory
		{
			memory.count(&mut self.counted, total);
		}
	}

	/// Count what the account has reserved as bytes the query cannot spill,
	/// as does every byte it reserves from now on.
	pub(crate) fn pin(&mut self) {
		if let Some(memory) = &self.memory
			&& !self.pinned
		{
			self.pinned = true;
			let lease = memory.lease();
			lease.pin(self.reserved);
			if memory.count(&mut self.counted, self.used) {
				self.exceeded = true;
			}
		}
	}

	/// Charge what the breaker holds in all, `total`, where the breaker
	/// measures its whole state rather than each entry it takes.
	pub(crate) fn hold(&mut self, total: usize) -> Verdict {
		if total > self.used {
			self.charge(total - self.used)
		} else {
			self.shrink(total);
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

	/// Give back everything reserved: the breaker has spilled or finished.
	pub(crate) fn clear(&mut self) {
		if let Some(memory) = &self.memory
			&& self.reserved > 0
		{
			match self.pinned {
				true => memory.lease().release_pinned(self.reserved),
				false => memory.lease().release(self.reserved),
			}
		}
		if let Some(memory) = &self.memory {
			memory.uncount(self.counted);
		}
		self.counted = 0;
		self.reserved = 0;
		self.used = 0;
		self.refused = false;
		self.exceeded = false;
	}
}

impl Drop for Reservation {
	fn drop(&mut self) {
		self.clear();
	}
}

/// A query's wait in [`QueryMemory::insist`], which leaves the lease no
/// longer waiting if it is dropped before it is disarmed.
struct WaitGuard<'a> {
	governor: &'a dyn MemoryGovernor,
	lease: &'a Lease,
	armed: bool,
}

impl Drop for WaitGuard<'_> {
	fn drop(&mut self) {
		if self.armed {
			self.governor.unwait(self.lease);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::exec::operators::test_util::root_ctx;

	/// A host with no store, holding its node to `limit` and each query to
	/// `max` pinned bytes, with queries waiting at most `wait`.
	fn host(limit: usize, wait: Duration, max: usize) -> (Arc<FairGovernor>, MemoryHost) {
		let governor = Arc::new(FairGovernor::new(limit));
		let host = MemoryHost {
			governor: Arc::clone(&governor) as Arc<dyn MemoryGovernor>,
			store: None,
			settings: MemorySettings {
				floor: 100,
				fan_in: 2,
				chunk: 64,
				wait,
				max,
			},
		};
		(governor, host)
	}

	/// Whether `result` is the memory threshold error.
	fn is_ceiling(result: &FlowResult<()>) -> bool {
		matches!(
			result,
			Err(ControlFlow::Err(error))
				if matches!(
					error.downcast_ref::<DatastoreError>(),
					Some(DatastoreError::QueryBeyondMemoryThreshold)
				)
		)
	}

	/// What a query charged for its transaction's buffers is given back when
	/// the transaction ends.
	#[test]
	fn a_transaction_ending_gives_back_its_charge() {
		let (_governor, host) = host(1000, Duration::ZERO, usize::MAX);
		let memory = host.query();
		let lease = memory.lease();
		assert!(lease.reserve_pinned(300));
		lease.force_pinned(200);
		*memory.unspillable.lock().unwrap() = (450, 500);
		*memory.writes.lock().unwrap() = (7, 450);
		memory.transaction_ended();
		assert_eq!((lease.held(), lease.pinned(), lease.overcommit()), (0, 0, 0));
		assert_eq!(*memory.unspillable.lock().unwrap(), (0, 0));
		assert_eq!(*memory.writes.lock().unwrap(), (0, 0));
	}

	/// A query that would pin more than its ceiling fails with the memory
	/// threshold error, and another query on the same node is not affected by
	/// what it holds.
	#[tokio::test]
	async fn the_ceiling_fails_only_the_query_that_passes_it() {
		let (governor, host) = host(10_000, Duration::ZERO, 1000);
		let ctx = root_ctx();
		let hog = host.query();
		let other = host.query();
		hog.hold_unspillable(800, &ctx).await.expect("within the ceiling");
		assert!(is_ceiling(&hog.hold_unspillable(300, &ctx).await));
		assert_eq!(hog.pinned(), 800, "a refused request holds nothing");
		// The other query's own ceiling is its own: the hog's bytes do not count.
		other.hold_unspillable(900, &ctx).await.expect("the other query is within its ceiling");
		assert!(is_ceiling(&other.hold_unspillable(200, &ctx).await));
		drop((hog, other));
		assert_eq!(governor.overcommitted(), 0);
	}

	/// A pinned account that would pass the ceiling is refused its chunk and
	/// its query's next `insist` raises the error; an account switched to pinned
	/// past the ceiling raises it the same way.
	#[tokio::test]
	async fn a_pinned_account_past_the_ceiling_raises_at_its_next_insist() {
		let (_governor, host) = host(10_000, Duration::ZERO, 1000);
		let ctx = root_ctx();
		let memory = host.query();
		let mut account = memory.pinned_reservation();
		assert_eq!(account.charge(500), Verdict::Hold);
		assert_eq!(account.charge(900), Verdict::Spill);
		assert!(account.wants_spill());
		assert!(is_ceiling(&account.insist(&ctx).await));
		account.clear();
		assert_eq!(memory.lease().pinned(), 0);

		let mut spillable = memory.account(false);
		assert_eq!(spillable.charge(1500), Verdict::Hold);
		spillable.pin();
		assert!(is_ceiling(&spillable.insist(&ctx).await));
	}

	/// The ceiling counts the bytes a query holds, not the chunks its accounts
	/// reserve for them: accounts that each hold just under a whole number of
	/// chunks fit a ceiling their reservations together pass, and the byte that
	/// takes the holdings past it is refused.
	#[tokio::test]
	async fn the_ceiling_counts_bytes_held_not_chunks_reserved() {
		let (_governor, host) = host(100_000, Duration::ZERO, 1000);
		let ctx = root_ctx();
		let memory = host.query();
		let mut accounts: Vec<Reservation> = (0..5).map(|_| memory.pinned_reservation()).collect();
		// Four accounts of 240 bytes reserve 256 each, 1024 in all.
		for account in &mut accounts[..4] {
			assert_eq!(account.charge(240), Verdict::Hold);
			account.insist(&ctx).await.expect("within the ceiling");
		}
		assert_eq!(memory.lease().pinned(), 1024);
		assert_eq!(memory.pinned(), 960);
		assert_eq!(accounts[4].charge(40), Verdict::Hold);
		assert_eq!(accounts[4].charge(1), Verdict::Spill);
		assert!(is_ceiling(&accounts[4].insist(&ctx).await));
		// Holding the query at its ceiling exactly is not past it.
		accounts[4].clear();
		assert_eq!(accounts[4].charge(40), Verdict::Hold);
		assert_eq!(memory.pinned(), 1000);
		accounts.clear();
		assert_eq!(memory.pinned(), 0, "counted bytes outlived their accounts");
	}

	/// A total that moves across a chunk boundary and back reserves and releases
	/// nothing after its first reservation: the lease keeps one spare chunk, in
	/// the small path of an account that has reserved as in the general one.
	#[tokio::test]
	async fn a_total_oscillating_across_a_chunk_boundary_releases_nothing() {
		let (_governor, host) = host(100_000, Duration::ZERO, usize::MAX);
		let memory = host.query();
		let mut account = memory.pinned_reservation();
		assert_eq!(account.settle(129), Verdict::Hold);
		let reserved = memory.lease().pinned();
		assert_eq!(reserved, 192);
		for _ in 0..4 {
			assert_eq!(account.settle(127), Verdict::Hold);
			assert_eq!(memory.lease().pinned(), reserved, "a shrink across the boundary released");
			assert_eq!(account.settle(129), Verdict::Hold);
			assert_eq!(memory.lease().pinned(), reserved);
		}
		// Across the boundary of the account's one chunk, once it holds a
		// reservation.
		assert_eq!(account.settle(64), Verdict::Hold);
		assert_eq!(memory.lease().pinned(), 64, "two spare chunks were not given back");
		for _ in 0..4 {
			assert_eq!(account.settle(63), Verdict::Hold);
			assert_eq!(account.settle(65), Verdict::Hold);
			assert_eq!(memory.lease().pinned(), 128);
		}
		assert_eq!(memory.pinned(), 65);
	}

	/// A query's waits in all last at most the configured wait: a lease that
	/// waits behind an older one, which never resolves, proceeds when its budget
	/// is spent, and a later request of the same query does not wait at all.
	#[tokio::test]
	async fn insist_never_waits_past_the_query_budget() {
		let wait = Duration::from_millis(50);
		let (governor, host) = host(1000, wait, usize::MAX);
		let ctx = root_ctx();
		let node: Arc<dyn MemoryGovernor> = Arc::clone(&governor) as Arc<dyn MemoryGovernor>;
		let holder = Lease::open(&node, 100, 0);
		assert!(holder.reserve_pinned(500));
		// Older than the query's lease, and waiting for memory that the holder
		// does not give back.
		let older = Lease::open(&node, 100, 0);
		assert!(matches!(governor.admit(&older, 600, true), Admit::Wait { .. }));

		let memory = host.query();
		memory.insist(600, &ctx).await.expect("proceeds past the limit when its wait is spent");
		let waited = Duration::from_nanos(memory.waited.load(Ordering::Acquire));
		assert!(waited >= wait / 2, "it did not wait its budget: {waited:?}");
		assert!(
			waited < wait + Duration::from_millis(500),
			"it waited past its budget: {waited:?}"
		);
		assert!(!memory.lease().waiting());
		let before = memory.waited.load(Ordering::Acquire);
		memory.insist(600, &ctx).await.expect("granted without waiting");
		assert_eq!(memory.waited.load(Ordering::Acquire), before, "a second request waited");
		assert!(older.waiting());
	}

	/// A governor that records, for each lease, the `may_wait` of every request
	/// it is asked.
	struct Asked {
		inner: FairGovernor,
		asked: std::sync::Mutex<Vec<(u64, bool)>>,
	}

	impl Asked {
		fn by(&self, lease: &Lease) -> Vec<bool> {
			let asked = self.asked.lock().unwrap();
			asked.iter().filter(|(id, _)| *id == lease.id()).map(|(_, wait)| *wait).collect()
		}
	}

	impl MemoryGovernor for Asked {
		fn opened(&self, lease: &Arc<Lease>) {
			self.inner.opened(lease);
		}

		fn grow(&self, lease: &Lease, bytes: usize) -> bool {
			self.inner.grow(lease, bytes)
		}

		fn closed(&self, lease: &Lease) {
			self.inner.closed(lease);
		}

		fn admit(&self, lease: &Lease, bytes: usize, may_wait: bool) -> Admit {
			self.asked.lock().unwrap().push((lease.id(), may_wait));
			self.inner.admit(lease, bytes, may_wait)
		}

		fn changed(&self, epoch: u64) -> BoxFut<'_, ()> {
			self.inner.changed(epoch)
		}

		fn released(&self, lease: &Lease) {
			self.inner.released(lease);
		}

		fn overcommitted(&self, lease: &Lease, bytes: usize) {
			MemoryGovernor::overcommitted(&self.inner, lease, bytes);
		}

		fn unwait(&self, lease: &Lease) {
			self.inner.unwait(lease);
		}
	}

	/// A context whose deadline is `timeout` away.
	fn ctx_with_timeout(timeout: Duration) -> ExecutionContext {
		let ExecutionContext::Root(mut root) = root_ctx() else {
			unreachable!("root_ctx builds a Root context")
		};
		let mut ctx = crate::ctx::Context::new_test();
		ctx.add_timeout(timeout).expect("a timeout");
		root.ctx = ctx.freeze();
		ExecutionContext::Root(root)
	}

	/// A wait is cut short by the governor's wake-up and not by its slice: the
	/// slice here is the share of the deadline, shorter than what is left of the
	/// configured wait, and a query woken by another lease's change before it
	/// ends is still behind the older waiter and asks to wait again rather than
	/// proceeding past it.
	#[tokio::test]
	async fn a_wake_up_before_the_slice_ends_keeps_the_rest_of_the_wait() {
		let governor = Arc::new(Asked {
			inner: FairGovernor::new(1000),
			asked: std::sync::Mutex::new(Vec::new()),
		});
		let node: Arc<dyn MemoryGovernor> = Arc::clone(&governor) as Arc<dyn MemoryGovernor>;
		let host = MemoryHost {
			governor: Arc::clone(&node),
			store: None,
			settings: MemorySettings {
				floor: 100,
				fan_in: 2,
				chunk: 64,
				wait: Duration::from_secs(30),
				max: usize::MAX,
			},
		};
		let holder = Lease::open(&node, 100, 0);
		assert!(holder.reserve_pinned(500));
		let older = Lease::open(&node, 100, 0);
		assert!(matches!(governor.admit(&older, 600, true), Admit::Wait { .. }));
		let memory = host.query();
		let lease = Arc::clone(memory.lease());
		// Half the deadline is 15 s: a slice of it is shorter than the 30 s wait.
		let ctx = ctx_with_timeout(Duration::from_secs(30));
		let query = {
			let memory = Arc::clone(&memory);
			tokio::spawn(async move { memory.insist(600, &ctx).await })
		};
		let started = std::time::Instant::now();
		while !lease.waiting() {
			assert!(started.elapsed() < Duration::from_secs(10), "the query never waited");
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
		// Another lease's change wakes the query, which asks again.
		let bystander = Lease::open(&node, 100, 0);
		node.unwait(&bystander);
		while governor.by(&lease).len() < 2 {
			assert!(started.elapsed() < Duration::from_secs(10), "the query was not woken");
			tokio::time::sleep(Duration::from_millis(1)).await;
		}
		assert!(!query.is_finished(), "the query went past the older waiter");
		assert!(governor.by(&lease).iter().all(|wait| *wait), "a wake-up spent the wait");
		query.abort();
		assert!(query.await.is_err());
		assert!(!lease.waiting(), "a cancelled wait left the lease waiting");
		drop((holder, older, bystander));
	}

	/// An account settled to a smaller total gives back the whole chunks it
	/// reserved beyond it, so a holder that shrinks returns memory to its lease
	/// without a release for every byte, and one that grows is charged by chunk.
	#[tokio::test]
	async fn settling_gives_back_whole_chunks() {
		let (_governor, host) = host(100_000, Duration::ZERO, usize::MAX);
		let memory = host.query();
		let mut account = memory.pinned_reservation();
		assert_eq!(account.settle(1000), Verdict::Hold);
		assert_eq!(memory.lease().pinned(), 1024, "1000 bytes reserve 16 chunks of 64");
		assert_eq!(account.settle(4000), Verdict::Hold);
		assert_eq!(memory.lease().pinned(), 4032);
		assert_eq!(account.settle(100), Verdict::Hold);
		assert_eq!(memory.lease().pinned(), 128, "100 bytes keep 2 chunks");
		assert_eq!(account.settle(0), Verdict::Hold);
		assert_eq!(memory.lease().pinned(), 0);
	}

	/// Accounts whose totals fit in one chunk reserve nothing and open no lease:
	/// their bytes count exactly toward the query's ceiling, and the account
	/// that takes the query past it raises the error at its next `insist`.
	#[tokio::test]
	async fn small_totals_reserve_nothing_and_count_toward_the_ceiling() {
		let (governor, host) = host(10_000, Duration::ZERO, 200);
		let ctx = root_ctx();
		let memory = host.query();
		let mut accounts: Vec<Reservation> = (0..4).map(|_| memory.pinned_reservation()).collect();
		for account in &mut accounts[..3] {
			assert_eq!(account.settle(64), Verdict::Hold);
		}
		assert!(memory.lease.get().is_none(), "a small total opened the lease");
		assert_eq!(governor.granted(), 0);
		assert_eq!(memory.pinned(), 192, "the small totals are counted exactly");
		assert_eq!(accounts[3].settle(64), Verdict::Spill);
		assert!(is_ceiling(&accounts[3].insist(&ctx).await));
		accounts.clear();
		assert_eq!(memory.pinned(), 0, "small bytes outlived their accounts");
	}

	/// An account takes over what another reserved without giving it back to the
	/// lease, which neither sees the bytes released nor reserved again.
	#[tokio::test]
	async fn absorbing_keeps_the_bytes_reserved() {
		let (governor, host) = host(100_000, Duration::ZERO, usize::MAX);
		let memory = host.query();
		let mut handed = memory.pinned_reservation();
		assert_eq!(handed.charge(1000), Verdict::Hold);
		let reserved = memory.lease().pinned();
		let mut keeper = memory.pinned_reservation();
		keeper.absorb(handed);
		assert_eq!(memory.lease().pinned(), reserved, "the bytes were given back");
		assert_eq!(keeper.settle(1000), Verdict::Hold);
		assert_eq!(memory.lease().pinned(), reserved, "the bytes were reserved again");
		drop(keeper);
		assert_eq!(memory.lease().pinned(), 0);
		assert_eq!(governor.overcommitted(), 0);
	}
}
