//! The node's memory governor and the lease each query holds from it.
//!
//! One [`MemoryGovernor`] serves a process (on an edge host, an isolate) and
//! is sized by the host. Each query that holds state across its input — a
//! sort, a group, a distinct, a join's build — opens one [`Lease`]: a
//! guaranteed minimum, which the governor never takes back, and a grant above
//! it that the query borrows from the node's free memory. Operators reserve
//! and release memory against their query's lease in chunks
//! ([`super::Reservation`]), never a row at a time.
//!
//! The governor may lower a grant while the query runs. The query observes
//! its grant where it hands rows on (a batch flush), and a breaker holding
//! more than its grant shrinks there by spilling, so the memory a lowered
//! grant revokes is given back within one batch. A lease never falls below
//! its minimum, and the minimum includes the spill headroom
//! ([`Lease::headroom`]): the part buffer a run is written through and the
//! read buffers a merge holds.
//!
//! Spillable growth never creates overcommit. A lease that grows a breaker is
//! refused when the node has no free memory that no waiting lease has claimed,
//! and the breaker spills; reclaiming for it lowers no lease below the bytes it
//! cannot spill plus its headroom.
//!
//! A lease that must hold bytes it cannot spill asks
//! [`MemoryGovernor::admit`], which never refuses. In order, the governor
//! covers the bytes from the grant; grows the grant to at most the lease's
//! fair share, lowering every lease above its own fair share to it where the
//! node has too little free, pinned bytes included; grows it past the fair
//! share from free memory that no waiting lease has claimed; displaces the
//! bytes onto what the lease itself can spill; answers that the query should
//! wait for other queries to give memory back; or grants the bytes past the
//! node's limit. Growth within the fair share never waits.
//!
//! The pinned bytes a lease holds above what its grant covers are its
//! overcommit ([`Lease::overcommit`]), derived from the lease's state and never
//! stored. A lease's fair share and a new lease's minimum are honoured by
//! lowering a borrower's grant, so a borrower's pinned bytes above its new grant
//! become its own overcommit and its further borrowing waits behind every claim.
//!
//! A lease that waits records what it is short of as its claim and is marked
//! waiting. Leases are ordered by age, their id. A lease waits while an older
//! lease waits, or while the leases that are not waiting can give back memory
//! that covers its shortfall; free memory claimed by an older waiter is not
//! free to a younger lease past its fair share. A waiting lease is waited on
//! only by younger leases, and a lease that is not waiting waits on none, so the
//! wait-for relation is acyclic: every edge points from a younger waiter to an
//! older one, or to a lease that is running. The oldest waiter, with nothing
//! that is running left to give memory back, is granted its bytes past the
//! limit. Every wait is also bounded by the caller, so a query blocked on
//! something the governor cannot see still ends its wait. Decisions are atomic
//! under the governor's lock.
//!
//! The governor does not bound what a single query pins: that is the query's
//! own ceiling ([`super::QueryMemory`]). Memory a borrower holds past its
//! grant adds to what the process holds, so where the process has a memory
//! threshold the threshold's guard, which fails whichever query checks it
//! next, is the only node-wide bound on overcommit.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use tokio::sync::Notify;

/// A boxed future, as a [`MemoryGovernor`] returns where it must be object
/// safe.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// How a governor answers a lease that must hold bytes it cannot spill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admit {
	/// The bytes are reserved on the lease.
	Granted,
	/// Other leases will give memory back, enough that the lease's grant can
	/// then grow to hold the bytes: wait until the governor's epoch
	/// moves past `epoch` ([`MemoryGovernor::changed`]), then ask again. The
	/// lease is marked waiting.
	Wait {
		epoch: u64,
	},
	/// The bytes are reserved past the governor's limit; those its grant does
	/// not cover are the lease's overcommit.
	Overcommit,
}

/// A node's policy for how much memory each query may hold.
///
/// The governor owns the grants; a [`Lease`] owns what its query holds. A
/// governor changes a grant with [`Lease::set_grant`], which never lowers it
/// below the lease's minimum.
pub trait MemoryGovernor: Send + Sync + 'static {
	/// `lease` has opened, granted its minimum.
	fn opened(&self, lease: &Arc<Lease>);

	/// `lease` asks for its grant to rise by at least `bytes`. Answers whether
	/// it rose; a refusal tells the query to spill.
	fn grow(&self, lease: &Lease, bytes: usize) -> bool;

	/// `lease` has closed: what it was granted returns to the node.
	fn closed(&self, lease: &Lease);

	/// `lease` must hold `bytes` more that it cannot spill. Never refuses:
	/// the answer reserves the bytes on the lease (`Granted`, `Overcommit`) or
	/// tells the query to wait. `may_wait` is false once the caller's bounded
	/// wait has run out, and the answer is then never `Wait`.
	///
	/// The default grows the lease and otherwise grants the bytes past the
	/// governor's limit. A governor that never answers `Wait` needs no
	/// [`changed`](Self::changed).
	fn admit(&self, lease: &Lease, bytes: usize, _may_wait: bool) -> Admit {
		if lease.reserve_pinned(bytes) {
			Admit::Granted
		} else {
			lease.force_pinned(bytes);
			self.overcommitted(lease, lease.overcommit());
			Admit::Overcommit
		}
	}

	/// Resolves once memory may have been given back since `epoch`, an epoch
	/// a `Wait` answer named. The default resolves at once.
	fn changed(&self, _epoch: u64) -> BoxFut<'_, ()> {
		Box::pin(std::future::ready(()))
	}

	/// `lease` gave memory back ([`Lease::release`]). The default does
	/// nothing.
	fn released(&self, _lease: &Lease) {}

	/// `lease` came to hold `bytes` of pinned memory its grant does not cover,
	/// having held none: once for each run of overcommit, however many requests
	/// extend it. The default does nothing.
	fn overcommitted(&self, _lease: &Lease, _bytes: usize) {}

	/// `lease`'s query stopped waiting without asking again: it was cancelled
	/// or its wait ran out. The default does nothing.
	fn unwait(&self, _lease: &Lease) {}

	/// The node limit the governor holds its leases to, if it has one.
	fn limit(&self) -> Option<usize> {
		None
	}
}

static NEXT_LEASE: AtomicU64 = AtomicU64::new(1);

/// What one query may hold, and what it holds, under its node's governor.
pub struct Lease {
	id: u64,
	minimum: usize,
	headroom: usize,
	grant: AtomicUsize,
	held: AtomicUsize,
	/// The bytes of `held` that cannot be spilled: gathered values,
	/// transaction buffers, breakers with no store. Never more than `held`.
	pinned: AtomicUsize,
	/// Set under the governor's lock while the lease's query waits for other
	/// queries to give memory back.
	waiting: AtomicBool,
	/// The bytes the lease is short of while it waits, which younger leases
	/// leave free for it. Set under the governor's lock with `waiting`.
	want: AtomicUsize,
	governor: Arc<dyn MemoryGovernor>,
}

impl std::fmt::Debug for Lease {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Lease")
			.field("id", &self.id)
			.field("minimum", &self.minimum)
			.field("headroom", &self.headroom)
			.field("grant", &self.grant())
			.field("held", &self.held())
			.field("pinned", &self.pinned())
			.field("overcommit", &self.overcommit())
			.finish()
	}
}

impl Lease {
	/// Open a lease under `governor` guaranteeing `minimum` bytes, of which
	/// `headroom` is kept for spilling and is never reserved by an operator.
	pub fn open(governor: &Arc<dyn MemoryGovernor>, minimum: usize, headroom: usize) -> Arc<Self> {
		let minimum = minimum.max(headroom);
		let lease = Arc::new(Self {
			id: NEXT_LEASE.fetch_add(1, Ordering::Relaxed),
			minimum,
			headroom,
			grant: AtomicUsize::new(minimum),
			held: AtomicUsize::new(0),
			pinned: AtomicUsize::new(0),
			waiting: AtomicBool::new(false),
			want: AtomicUsize::new(0),
			governor: Arc::clone(governor),
		});
		governor.opened(&lease);
		lease
	}

	/// The lease's identity, unique in the process. A lower id is an older
	/// lease.
	pub fn id(&self) -> u64 {
		self.id
	}

	/// The bytes the lease is guaranteed, headroom included.
	pub fn minimum(&self) -> usize {
		self.minimum
	}

	/// The part of the minimum kept for spill buffers: one run's write
	/// buffer and the read buffers of a merge.
	pub fn headroom(&self) -> usize {
		self.headroom
	}

	/// The bytes the governor grants the lease now, headroom included.
	pub fn grant(&self) -> usize {
		self.grant.load(Ordering::Acquire)
	}

	/// The bytes the query's operators hold.
	pub fn held(&self) -> usize {
		self.held.load(Ordering::Acquire)
	}

	/// The bytes of [`held`](Self::held) that cannot be spilled.
	pub fn pinned(&self) -> usize {
		self.pinned.load(Ordering::Acquire)
	}

	/// The bytes of [`held`](Self::held) that spilling gives back.
	pub fn spillable(&self) -> usize {
		self.held().saturating_sub(self.pinned())
	}

	/// The pinned bytes the lease's grant does not cover: memory held past the
	/// node limit.
	pub fn overcommit(&self) -> usize {
		self.pinned().saturating_sub(self.usable())
	}

	/// Whether the lease's query waits for other queries to give memory back.
	pub fn waiting(&self) -> bool {
		self.waiting.load(Ordering::Acquire)
	}

	/// Set the grant to `bytes`, or to the minimum when `bytes` is below it.
	/// A grant lowered below what the query holds is observed at its next
	/// flush, where its breakers spill.
	pub fn set_grant(&self, bytes: usize) {
		self.grant.store(bytes.max(self.minimum), Ordering::Release);
	}

	/// What operators may hold in all: the grant less the headroom.
	pub(crate) fn usable(&self) -> usize {
		self.grant().saturating_sub(self.headroom)
	}

	/// The grant no spillable growth lowers the lease below: the minimum, and
	/// room for what the lease holds and cannot spill.
	pub(crate) fn floor(&self) -> usize {
		self.minimum.max(self.pinned().saturating_add(self.headroom))
	}

	/// Whether the lease holds spillable bytes its grant does not cover; its
	/// breakers spill at their next flush. Pinned bytes past the grant do not
	/// count: they do not come back by spilling.
	pub fn over(&self) -> bool {
		self.held() > self.usable().max(self.pinned())
	}

	/// Reserve `bytes` more if the grant covers them, without asking the
	/// governor.
	fn hold(&self, bytes: usize) -> bool {
		loop {
			let held = self.held();
			let want = held.saturating_add(bytes);
			if want > self.usable() {
				return false;
			}
			if self
				.held
				.compare_exchange_weak(held, want, Ordering::AcqRel, Ordering::Acquire)
				.is_ok()
			{
				return true;
			}
		}
	}

	/// Reserve `bytes` more, asking the governor to grow the grant when it
	/// does not cover them. Answers whether the bytes are reserved.
	pub fn reserve(&self, bytes: usize) -> bool {
		let mut asked = false;
		loop {
			if self.hold(bytes) {
				return true;
			}
			let want = self.held().saturating_add(bytes);
			if want <= self.usable() {
				continue;
			}
			if asked || !self.governor.grow(self, want - self.usable()) {
				return false;
			}
			asked = true;
		}
	}

	/// [`reserve`](Self::reserve) bytes the lease cannot spill.
	pub(crate) fn reserve_pinned(&self, bytes: usize) -> bool {
		let reserved = self.reserve(bytes);
		if reserved {
			self.pinned.fetch_add(bytes, Ordering::AcqRel);
		}
		reserved
	}

	/// [`hold`](Self::hold) bytes the lease cannot spill, without asking the
	/// governor.
	fn hold_pinned(&self, bytes: usize) -> bool {
		let held = self.hold(bytes);
		if held {
			self.pinned.fetch_add(bytes, Ordering::AcqRel);
		}
		held
	}

	/// Count `bytes` of what the lease holds as bytes it cannot spill. Pinned
	/// bytes the grant does not cover will not come back by spilling, so they
	/// are the lease's overcommit; the governor is told when that begins.
	pub(crate) fn pin(&self, bytes: usize) {
		let before = self.overcommit();
		self.pinned.fetch_add(bytes, Ordering::AcqRel);
		let after = self.overcommit();
		if before == 0 && after > 0 {
			self.governor.overcommitted(self, after);
		}
	}

	/// Hold `bytes` more that the lease cannot spill, whatever its grant: the
	/// lease then holds more than it is granted, and its breakers spill. What
	/// the grant does not cover is the lease's overcommit.
	pub(crate) fn force_pinned(&self, bytes: usize) {
		self.held.fetch_add(bytes, Ordering::AcqRel);
		self.pinned.fetch_add(bytes, Ordering::AcqRel);
	}

	/// Give back `bytes` reserved earlier.
	pub fn release(&self, bytes: usize) {
		let mut held = self.held();
		let gave = loop {
			let next = held.saturating_sub(bytes);
			match self.held.compare_exchange_weak(held, next, Ordering::AcqRel, Ordering::Acquire) {
				Ok(_) => break held - next,
				Err(now) => held = now,
			}
		};
		if gave == 0 {
			return;
		}
		self.governor.released(self);
	}

	/// Give back `bytes` reserved earlier as bytes the lease cannot spill.
	pub(crate) fn release_pinned(&self, bytes: usize) {
		Self::lower(&self.pinned, bytes);
		self.release(bytes);
	}

	/// Lower `counter` by `bytes`, to no less than zero.
	fn lower(counter: &AtomicUsize, bytes: usize) {
		let mut now = counter.load(Ordering::Acquire);
		loop {
			let next = now.saturating_sub(bytes);
			match counter.compare_exchange_weak(now, next, Ordering::AcqRel, Ordering::Acquire) {
				Ok(_) => return,
				Err(seen) => now = seen,
			}
		}
	}
}

impl Drop for Lease {
	fn drop(&mut self) {
		self.governor.closed(self);
	}
}

/// A governor with no limit: every grant grows on request. The default where
/// a host neither sets a limit nor supplies a store to spill to.
#[derive(Debug, Default)]
pub struct Unlimited;

impl MemoryGovernor for Unlimited {
	fn opened(&self, _lease: &Arc<Lease>) {}

	fn grow(&self, lease: &Lease, bytes: usize) -> bool {
		lease.set_grant(lease.grant().saturating_add(bytes));
		true
	}

	fn closed(&self, _lease: &Lease) {}
}

/// How far [`FairGovernor::reclaim`] lowers a lease's grant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reclaim {
	/// For spillable growth: no lower than the bytes a lease cannot spill plus
	/// its headroom, so reclaim creates no overcommit.
	Spillable,
	/// For a minimum, or pinned growth within the asker's fair share: a
	/// borrower above its fair share gives back what it borrowed, pinned bytes
	/// included, and what it then holds past its grant is its overcommit.
	Guaranteed,
}

/// A governor holding the node to `limit` bytes, shared fairly.
///
/// A lease grows from the node's free memory while there is any, so a query
/// alone on an idle node may borrow the whole limit. When a lease asks for
/// more than is free, every other lease above its fair share — the limit
/// divided among the open leases, and never below a lease's minimum — is
/// lowered to it, and the asker is granted up to its own fair share. Spillable
/// growth lowers a lease no further than the bytes it cannot spill, so it
/// creates no overcommit; a minimum, and pinned growth within the asker's fair
/// share, lower a borrower to its fair share whatever it has pinned, and the
/// bytes it holds past its new grant are its overcommit. Memory a closing lease
/// held is free for the next to ask. The grants' sum stays within the greater
/// of the limit and the sum of the minimums, which are guaranteed so that
/// admission never waits.
///
/// The memory a lowered lease holds is given back when it next flushes, so
/// what the node holds may pass the limit by what its queries hold in one
/// batch. Pinned bytes a lease holds past its grant stay held until the lease
/// releases them; the bytes all leases hold so are counted by
/// [`FairGovernor::overcommitted`].
pub struct FairGovernor {
	limit: usize,
	leases: Mutex<Vec<Weak<Lease>>>,
	/// Moves on a release or close while a lease waits, and on every change in
	/// who waits or what they want; a waiter whose epoch is behind it decides
	/// again.
	epoch: AtomicU64,
	changed: Notify,
	/// The leases marked waiting.
	waiters: AtomicUsize,
	waits: AtomicU64,
	overcommits: AtomicU64,
}

impl std::fmt::Debug for FairGovernor {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("FairGovernor").field("limit", &self.limit).finish()
	}
}

impl FairGovernor {
	/// A governor holding its node to `limit` bytes.
	pub fn new(limit: usize) -> Self {
		Self {
			limit,
			leases: Mutex::new(Vec::new()),
			epoch: AtomicU64::new(0),
			changed: Notify::new(),
			waiters: AtomicUsize::new(0),
			waits: AtomicU64::new(0),
			overcommits: AtomicU64::new(0),
		}
	}

	/// The node's limit, in bytes.
	pub fn limit(&self) -> usize {
		self.limit
	}

	/// The sum of the open leases' grants.
	pub fn granted(&self) -> usize {
		let leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		// Summed under the lock, which every change of a grant holds.
		let granted = open.iter().map(|lease| lease.grant()).sum();
		drop(leases);
		drop(open);
		granted
	}

	/// How many times a lease has been told to wait, counting each wait once
	/// however often its query asks again.
	pub fn waits(&self) -> u64 {
		self.waits.load(Ordering::Relaxed)
	}

	/// How many times a lease came to hold memory past its grant, counting
	/// each run of it once however many requests extend it.
	pub fn overcommits(&self) -> u64 {
		self.overcommits.load(Ordering::Relaxed)
	}

	/// The bytes the open leases hold past their grants now: the sum, over
	/// leases whose pinned bytes pass their usable grant, of the difference.
	pub fn overcommitted(&self) -> usize {
		let leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		let overcommitted = Self::overcommit_of(&open);
		drop(leases);
		drop(open);
		overcommitted
	}

	/// The sum of the `open` leases' overcommit.
	fn overcommit_of(open: &[Arc<Lease>]) -> usize {
		open.iter().map(|lease| lease.overcommit()).sum()
	}

	/// Move the epoch and wake the leases waiting on it.
	fn bump(&self) {
		self.epoch.fetch_add(1, Ordering::AcqRel);
		if self.waiters.load(Ordering::Acquire) > 0 {
			self.changed.notify_waiters();
		}
	}

	/// Count and log that `lease` has come to hold memory past its grant,
	/// having held none. Logged once for the run, however many requests extend
	/// it; the sum is read over the `open` leases.
	fn episode(&self, open: &[Arc<Lease>], lease: &Lease) {
		self.overcommits.fetch_add(1, Ordering::Relaxed);
		tracing::warn!(
			target: "surrealdb::core::exec::memory",
			lease = lease.id,
			bytes = lease.overcommit(),
			overcommitted = Self::overcommit_of(open),
			limit = self.limit,
			"the node holds memory past its limit"
		);
	}

	/// Record an episode if `lease` held no overcommit `before` and holds some
	/// now.
	fn note(&self, open: &[Arc<Lease>], lease: &Lease, before: usize) {
		if before == 0 && lease.overcommit() > 0 {
			self.episode(open, lease);
		}
	}

	/// The bytes the waiting leases of `open` have claimed that `lease` must
	/// leave them: those of every waiter when `lease` is not waiting, and of
	/// the older waiters when it is.
	fn claims(open: &[Arc<Lease>], lease: &Lease) -> usize {
		let waiting = lease.waiting();
		open.iter()
			.filter(|other| other.id != lease.id && other.waiting())
			.filter(|other| !waiting || other.id < lease.id)
			.map(|other| other.want.load(Ordering::Acquire))
			.sum()
	}
}

impl FairGovernor {
	/// Grow `lease` by `bytes` for spillable growth among the `open` leases,
	/// lowering those above their fair share when the node has too little free.
	/// Free memory the waiting leases have claimed is not free past the lease's
	/// fair share.
	fn decide(&self, open: &[Arc<Lease>], lease: &Lease, bytes: usize) -> bool {
		let mut granted: usize = open.iter().map(|other| other.grant()).sum();
		let free = self.limit.saturating_sub(granted).saturating_sub(Self::claims(open, lease));
		if free >= bytes {
			lease.set_grant(lease.grant() + bytes);
			return true;
		}
		let fair = self.limit / open.len().max(1);
		granted = self.reclaim(open, lease.id, fair, granted, Reclaim::Spillable);
		let free = self.limit.saturating_sub(granted);
		let room = fair.max(lease.minimum).saturating_sub(lease.grant()).min(free);
		if room >= bytes {
			lease.set_grant(lease.grant() + bytes);
			return true;
		}
		false
	}
}

impl FairGovernor {
	/// Lower every lease of `open` but `except` that is above `fair` to it, or
	/// to the floor `mode` names where that is higher, answering the grants'
	/// sum. A lease the lowering leaves holding pinned bytes its grant does not
	/// cover has begun an overcommit.
	fn reclaim(
		&self,
		open: &[Arc<Lease>],
		except: u64,
		fair: usize,
		mut granted: usize,
		mode: Reclaim,
	) -> usize {
		for other in open {
			if other.id == except {
				continue;
			}
			let floor = match mode {
				Reclaim::Spillable => fair.max(other.floor()),
				Reclaim::Guaranteed => fair.max(other.minimum),
			};
			let grant = other.grant();
			if grant > floor {
				let before = other.overcommit();
				other.set_grant(floor);
				granted -= grant - floor;
				self.note(open, other, before);
			}
		}
		granted
	}

	/// The decision of [`MemoryGovernor::admit`], made under the lock over the
	/// `open` leases.
	///
	/// Each lease's holding is read once where it is weighed: it changes without
	/// the lock, and a lease is weighed against its own reading throughout.
	fn decide_admit(
		&self,
		open: &[Arc<Lease>],
		lease: &Lease,
		bytes: usize,
		may_wait: bool,
	) -> Admit {
		// Read before anything is weighed: a release after this moves the epoch
		// past it, so a waiter told to wait on it is woken at once.
		let epoch = self.epoch.load(Ordering::Acquire);
		let before = lease.overcommit();
		let fair = self.limit / open.len().max(1);
		let granted: usize = open.iter().map(|other| other.grant()).sum();
		let free = self.limit.saturating_sub(granted).saturating_sub(Self::claims(open, lease));
		let shortfall = lease.held().saturating_add(bytes).saturating_sub(lease.usable());
		let reach = fair.max(lease.minimum).saturating_sub(lease.grant());

		// Covered by the grant.
		if shortfall == 0 && lease.hold_pinned(bytes) {
			return Admit::Granted;
		}

		// Within the fair share: always granted, never waits. The grant grows
		// from free memory, or from what borrowers above their fair share give
		// back. Where the minimums alone pass the limit there is nothing to give
		// and the excess is the lease's own overcommit.
		if shortfall <= reach {
			let mut take = shortfall;
			if free < shortfall {
				let granted = self.reclaim(open, lease.id, fair, granted, Reclaim::Guaranteed);
				take = shortfall.min(self.limit.saturating_sub(granted));
			}
			lease.set_grant(lease.grant() + take);
			if lease.hold_pinned(bytes) {
				return Admit::Granted;
			}
			lease.force_pinned(bytes);
			self.note(open, lease, before);
			return Admit::Overcommit;
		}

		// Borrow from free memory that no waiting lease has claimed. Others do
		// not give back for a borrower.
		if shortfall <= free {
			lease.set_grant(lease.grant() + shortfall);
			if lease.hold_pinned(bytes) {
				return Admit::Granted;
			}
		}

		// Displace the bytes onto what the lease itself can spill: it then holds
		// more than its grant, and its breakers spill at their next flush.
		if lease.spillable() >= bytes {
			lease.force_pinned(bytes);
			self.note(open, lease, before);
			return Admit::Granted;
		}

		// Wait behind older waiters, or for the leases that are not waiting to
		// give back what they hold above their guarantee, by spilling or when
		// their statement or query ends.
		let older_waiting = open.iter().any(|other| other.id < lease.id && other.waiting());
		let waitable: Vec<(&Arc<Lease>, usize)> = open
			.iter()
			.filter(|other| other.id != lease.id && !other.waiting())
			.filter_map(|other| {
				let held = other.held();
				let above = held.saturating_sub(other.minimum.saturating_sub(other.headroom));
				(above > 0).then_some((other, above))
			})
			.collect();
		let returnable: usize = waitable.iter().map(|(_, above)| *above).sum();
		let can_return = returnable.saturating_add(free) >= shortfall;
		if may_wait && (older_waiting || can_return) {
			if can_return {
				for (other, _) in &waitable {
					if other.spillable() > 0 {
						other.set_grant(other.grant().min(fair.max(other.floor())));
					}
				}
			}
			// A lease that starts waiting changes who is waitable and what is
			// claimed for every other waiter, as does a changed claim; a lease
			// that waits again for the same bytes changes nothing.
			let first = !lease.waiting.swap(true, Ordering::AcqRel);
			let changed = lease.want.swap(shortfall, Ordering::AcqRel) != shortfall;
			if first {
				self.waiters.fetch_add(1, Ordering::SeqCst);
				self.waits.fetch_add(1, Ordering::Relaxed);
			}
			if first || changed {
				self.bump();
			}
			return Admit::Wait {
				epoch,
			};
		}

		// The wait has run out, or this is the oldest lease asking and nothing
		// that is running can give back what it needs: the bytes are granted
		// past the limit.
		lease.force_pinned(bytes);
		self.note(open, lease, before);
		Admit::Overcommit
	}
}

impl MemoryGovernor for FairGovernor {
	fn opened(&self, lease: &Arc<Lease>) {
		let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		leases.retain(|weak| weak.strong_count() > 0);
		leases.push(Arc::downgrade(lease));
		// A minimum is granted at once: leases borrowing past their fair
		// share make room for it, pinned bytes included.
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		let granted: usize = open.iter().map(|other| other.grant()).sum();
		if granted > self.limit {
			let fair = self.limit / open.len().max(1);
			self.reclaim(&open, lease.id, fair, granted, Reclaim::Guaranteed);
		}
		// The fair share has changed for every waiter.
		if self.waiters.load(Ordering::SeqCst) > 0 {
			self.bump();
		}
		drop(leases);
		drop(open);
	}

	fn grow(&self, lease: &Lease, bytes: usize) -> bool {
		let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		leases.retain(|weak| weak.strong_count() > 0);
		// The lock is held across the decision, so two leases growing at once
		// cannot both be granted the same free memory. The leases are released
		// only after it, since dropping the last handle on one closes it, which
		// takes the lock.
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		let grown = self.decide(&open, lease, bytes);
		drop(leases);
		drop(open);
		grown
	}

	fn closed(&self, lease: &Lease) {
		let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		leases.retain(|weak| weak.strong_count() > 0 && !std::ptr::eq(weak.as_ptr(), lease));
		if lease.waiting.swap(false, Ordering::AcqRel) {
			lease.want.store(0, Ordering::Release);
			self.waiters.fetch_sub(1, Ordering::AcqRel);
		}
		if self.waiters.load(Ordering::SeqCst) > 0 {
			self.bump();
		}
	}

	fn admit(&self, lease: &Lease, bytes: usize, may_wait: bool) -> Admit {
		let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		leases.retain(|weak| weak.strong_count() > 0);
		// Held across the decision, as in `grow`, and the leases dropped only
		// after it.
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		let was_waiting = lease.waiting();
		let admitted = self.decide_admit(&open, lease, bytes, may_wait);
		if was_waiting && !matches!(admitted, Admit::Wait { .. }) {
			// The set of leases that are not waiting grew, and its claim is gone.
			lease.waiting.store(false, Ordering::Release);
			lease.want.store(0, Ordering::Release);
			self.waiters.fetch_sub(1, Ordering::AcqRel);
			self.bump();
		}
		drop(leases);
		drop(open);
		admitted
	}

	fn changed(&self, epoch: u64) -> BoxFut<'_, ()> {
		Box::pin(async move {
			let notified = self.changed.notified();
			let mut notified = std::pin::pin!(notified);
			// Registered before the epoch is read, so a change between the two
			// is not lost.
			notified.as_mut().enable();
			if self.epoch.load(Ordering::Acquire) != epoch {
				return;
			}
			notified.await;
		})
	}

	fn released(&self, _lease: &Lease) {
		// With no lease waiting this is the whole cost of a release. A lease
		// that starts waiting moves the epoch itself, so its first `changed`
		// returns at once and it decides again on what the release gave back.
		if self.waiters.load(Ordering::SeqCst) > 0 {
			self.bump();
		}
	}

	fn overcommitted(&self, lease: &Lease, _bytes: usize) {
		let leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		self.episode(&open, lease);
		drop(leases);
		drop(open);
	}

	fn unwait(&self, lease: &Lease) {
		let leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		if lease.waiting.swap(false, Ordering::AcqRel) {
			lease.want.store(0, Ordering::Release);
			self.waiters.fetch_sub(1, Ordering::AcqRel);
		}
		self.bump();
		drop(leases);
	}

	fn limit(&self) -> Option<usize> {
		Some(self.limit)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn dynamic(governor: &Arc<FairGovernor>) -> Arc<dyn MemoryGovernor> {
		Arc::clone(governor) as Arc<dyn MemoryGovernor>
	}

	/// Assert what every state of the governor upholds: its overcommit is the
	/// sum of its leases', and its grants stay within the limit unless the
	/// minimums alone pass it.
	fn check(governor: &FairGovernor, open: &[&Arc<Lease>]) {
		let overcommit: usize =
			open.iter().map(|lease| lease.pinned().saturating_sub(lease.usable())).sum();
		assert_eq!(governor.overcommitted(), overcommit, "the overcommit is not the leases'");
		let minimums: usize = open.iter().map(|lease| lease.minimum()).sum();
		assert!(
			governor.granted() <= governor.limit().max(minimums),
			"the grants pass the limit and the minimums"
		);
	}

	/// A lease alone on an idle node borrows up to the whole limit.
	#[test]
	fn a_lease_alone_borrows_the_whole_limit() {
		let governor = Arc::new(FairGovernor::new(1000));
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		assert!(lease.reserve(900));
		assert!(lease.reserve(100));
		assert!(!lease.reserve(1));
		assert_eq!(lease.held(), 1000);
		assert!(governor.granted() <= 1000);
	}

	/// A second lease asking for memory lowers the first to its fair share,
	/// which then holds more than its grant until it spills.
	#[test]
	fn an_asking_lease_lowers_a_borrower_to_its_fair_share() {
		let governor = Arc::new(FairGovernor::new(1000));
		let first = Lease::open(&dynamic(&governor), 100, 0);
		assert!(first.reserve(900));
		let second = Lease::open(&dynamic(&governor), 100, 0);
		assert!(second.reserve(300));
		assert_eq!(first.grant(), 500);
		assert!(first.over());
		assert!(governor.granted() <= 1000);
		first.release(900);
		assert!(!first.over());
	}

	/// A grant is never lowered below the minimum, and the headroom inside
	/// the minimum is never reserved by an operator.
	#[test]
	fn the_minimum_holds_and_the_headroom_is_kept() {
		let governor = Arc::new(FairGovernor::new(100));
		let lease = Lease::open(&dynamic(&governor), 80, 50);
		lease.set_grant(0);
		assert_eq!(lease.grant(), 80);
		assert!(lease.reserve(30));
		assert!(!lease.reserve(30));
	}

	/// A closed lease's memory is free for the next lease to ask.
	#[test]
	fn a_closed_lease_returns_its_memory() {
		let governor = Arc::new(FairGovernor::new(1000));
		let first = Lease::open(&dynamic(&governor), 0, 0);
		assert!(first.reserve(1000));
		drop(first);
		let second = Lease::open(&dynamic(&governor), 0, 0);
		assert!(second.reserve(1000));
	}

	/// Leases opened and grown from many threads keep the grants' sum within
	/// the limit.
	#[test]
	fn concurrent_leases_stay_within_the_limit() {
		let governor = Arc::new(FairGovernor::new(64 * 1024));
		let threads: Vec<_> = (0..8)
			.map(|_| {
				let governor = Arc::clone(&governor);
				std::thread::spawn(move || {
					let lease = Lease::open(&dynamic(&governor), 1024, 0);
					for _ in 0..1000 {
						if !lease.reserve(512) || lease.over() {
							lease.release(lease.held());
						}
						assert!(governor.granted() <= governor.limit());
					}
				})
			})
			.collect();
		for thread in threads {
			thread.join().expect("a lease thread");
		}
	}

	/// Each branch of the decision, in order: the grant covers the bytes, grows
	/// from free memory, the lease displaces onto what it can spill, waits for
	/// others to give memory back, and is granted past the limit once its wait
	/// has run out.
	#[test]
	fn admit_covers_grows_displaces_waits_then_overcommits() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert_eq!(governor.admit(&a, 50, true), Admit::Granted);
		assert_eq!((a.grant(), a.held(), a.pinned()), (100, 50, 50));
		assert_eq!(governor.admit(&a, 150, true), Admit::Granted);
		assert_eq!((a.grant(), a.held(), a.pinned()), (200, 200, 200));
		assert!(!a.over());

		// `b` holds spillable memory the node cannot grow past: the bytes it
		// must hold are displaced onto it.
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(b.reserve(600));
		assert_eq!(governor.admit(&b, 300, true), Admit::Granted);
		assert_eq!((b.held(), b.pinned(), b.spillable()), (900, 300, 600));
		assert!(b.over());
		assert_eq!(governor.overcommits(), 0);
		drop((a, b));

		// `c` asks for more than its fair share and the node's free memory
		// covers: `d` can give memory back, so it waits.
		let d = Lease::open(&dynamic(&governor), 100, 0);
		assert!(d.reserve_pinned(600));
		let c = Lease::open(&dynamic(&governor), 100, 0);
		let Admit::Wait {
			epoch,
		} = governor.admit(&c, 600, true)
		else {
			panic!("`c` did not wait");
		};
		assert!(c.waiting());
		assert_eq!(c.want.load(Ordering::Relaxed), 500);
		assert_eq!((governor.waits(), governor.overcommits()), (1, 0));
		// Asking again keeps it waiting for the same bytes: no second wait, and
		// the epoch it was told stays current.
		assert!(matches!(governor.admit(&c, 600, true), Admit::Wait { .. }));
		assert_eq!(governor.waits(), 1);
		assert!(governor.epoch.load(Ordering::Relaxed) > epoch);

		// Its wait has run out: the bytes are granted past the limit.
		assert_eq!(governor.admit(&c, 600, false), Admit::Overcommit);
		assert!(!c.waiting());
		assert_eq!(c.want.load(Ordering::Relaxed), 0);
		assert_eq!(governor.waiters.load(Ordering::Relaxed), 0);
		assert_eq!((c.overcommit(), c.pinned()), (500, 600));
		assert_eq!((governor.overcommits(), governor.overcommitted()), (1, 500));
		check(&governor, &[&c, &d]);
	}

	/// A request past the asker's fair share does not take free memory an older
	/// waiting lease has claimed: it waits behind the claim.
	#[test]
	fn a_request_past_the_fair_share_waits_behind_claims() {
		let governor = Arc::new(FairGovernor::new(1000));
		let holder = Lease::open(&dynamic(&governor), 100, 0);
		assert!(holder.reserve_pinned(500));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(matches!(governor.admit(&a, 600, true), Admit::Wait { .. }));
		assert_eq!(a.want.load(Ordering::Relaxed), 500);
		// 300 are free, short of what `a` claimed and `b` asks for: `b` waits and
		// takes none of it.
		assert!(matches!(governor.admit(&b, 600, true), Admit::Wait { .. }));
		assert!(b.waiting());
		assert_eq!((a.grant(), b.grant()), (100, 100));
		assert_eq!(governor.granted(), 700);
	}

	/// A spillable grower does not take free memory an older waiter has claimed
	/// past its fair share, and a lease within its fair share still grows.
	#[test]
	fn spillable_growth_leaves_a_waiters_claim() {
		let governor = Arc::new(FairGovernor::new(1000));
		let holder = Lease::open(&dynamic(&governor), 100, 0);
		assert!(holder.reserve_pinned(500));
		let waiter = Lease::open(&dynamic(&governor), 100, 0);
		let grower = Lease::open(&dynamic(&governor), 100, 0);
		assert!(matches!(governor.admit(&waiter, 600, true), Admit::Wait { .. }));
		// 300 free, all claimed: a spillable grower may not borrow beyond its
		// fair share (333) out of them.
		assert!(!grower.reserve(400));
		// Within its fair share it grows.
		assert!(grower.reserve(200));
	}

	/// A lease that waits is not one another lease waits on: with nothing else
	/// to give memory back, the older lease is granted past the limit and the
	/// younger waits behind it.
	#[test]
	fn the_oldest_waiter_proceeds_when_nothing_can_return() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(a.reserve_pinned(500));
		assert!(b.reserve_pinned(500));
		assert_eq!(governor.granted(), 1000);

		// `a` is the older and asks first: `b` holds memory above its minimum.
		assert!(matches!(governor.admit(&a, 100, true), Admit::Wait { .. }));
		assert!(a.waiting());
		// `b` asks behind it.
		assert!(matches!(governor.admit(&b, 100, true), Admit::Wait { .. }));
		assert_eq!(b.want.load(Ordering::Relaxed), 100);
		assert_eq!(governor.waits(), 2);
		// `a` decides again: `b` waits, so nothing that is running can give
		// memory back.
		assert_eq!(governor.admit(&a, 100, true), Admit::Overcommit);
		assert!(!a.waiting());
		assert_eq!((governor.overcommits(), governor.overcommitted()), (1, 100));
		check(&governor, &[&a, &b]);

		// `a` ends: `b` borrows from the memory it gave back.
		drop(a);
		assert_eq!(governor.admit(&b, 100, true), Admit::Granted);
		assert!(!b.waiting());
		assert_eq!(governor.waiters.load(Ordering::Relaxed), 0);
		check(&governor, &[&b]);
		drop(b);
		assert_eq!(governor.overcommitted(), 0);
	}

	/// An older lease's decision ignores a younger one that waits: the younger
	/// lease holds memory above its minimum, and it is not given back to the
	/// older.
	#[test]
	fn a_younger_lease_is_never_waited_on_by_an_older() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(a.reserve_pinned(500));
		assert!(b.reserve_pinned(500));
		assert!(matches!(governor.admit(&a, 100, true), Admit::Wait { .. }));
		assert!(matches!(governor.admit(&b, 100, true), Admit::Wait { .. }));
		assert!(b.held() > b.minimum());
		assert!(b.waiting());
		assert_eq!(governor.admit(&a, 100, true), Admit::Overcommit);
		// The younger one still waits, on the older's claim having gone.
		assert!(b.waiting());
	}

	/// Pinned growth within the asker's fair share never waits: a borrower
	/// above its fair share gives back.
	#[test]
	fn pinned_growth_within_the_fair_share_never_waits() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert!(a.reserve_pinned(900));
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert_eq!(governor.admit(&b, 300, true), Admit::Granted);
		assert_eq!(a.grant(), 500);
		assert_eq!((governor.waits(), governor.overcommits()), (0, 1));
		assert_eq!(a.overcommit(), a.pinned() - a.usable());
		check(&governor, &[&a, &b]);
	}

	/// A new lease's minimum is granted at once: a borrower holding pinned
	/// bytes gives back to its fair share and holds the rest as overcommit.
	#[test]
	fn a_new_minimum_shrinks_a_pinned_borrower() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert!(a.reserve_pinned(900));
		let b = Lease::open(&dynamic(&governor), 200, 0);
		assert!(b.grant() >= b.minimum());
		assert_eq!(a.grant(), 500.max(a.minimum()));
		assert_eq!(a.overcommit(), a.pinned() - a.usable());
		assert!(a.overcommit() > 0);
		assert!(governor.granted() <= governor.limit());
		check(&governor, &[&a, &b]);
	}

	/// Overcommit is exactly what the leases hold pinned past their grants, after
	/// every step of a sequence, and each run of it is counted once.
	#[test]
	fn overcommit_accounting_is_exact() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert!(a.reserve_pinned(950));
		check(&governor, &[&a]);
		assert_eq!(governor.overcommits(), 0);

		// `b`'s minimum takes the grants past the limit: `a` gives back to its
		// fair share, and holds what is left as overcommit.
		let b = Lease::open(&dynamic(&governor), 100, 0);
		check(&governor, &[&a, &b]);
		assert_eq!((governor.overcommits(), governor.overcommitted()), (1, 450));

		// `b` borrows from the memory `a` gave back; `a` stays shrunk.
		assert_eq!(governor.admit(&b, 300, true), Admit::Granted);
		check(&governor, &[&a, &b]);
		assert_eq!(governor.overcommits(), 1);

		// `a` pins more with nothing to wait on: the same run of overcommit
		// goes on, and is not counted again.
		assert_eq!(governor.admit(&a, 100, false), Admit::Overcommit);
		check(&governor, &[&a, &b]);
		assert_eq!((governor.overcommits(), governor.overcommitted()), (1, 550));

		// `a` releases down below its grant: no overcommit.
		a.release_pinned(650);
		check(&governor, &[&a, &b]);
		assert_eq!(governor.overcommitted(), 0);

		// Past its grant again is a second run.
		assert_eq!(governor.admit(&a, 500, false), Admit::Overcommit);
		check(&governor, &[&a, &b]);
		assert_eq!((governor.overcommits(), governor.overcommitted()), (2, 400));

		drop(a);
		check(&governor, &[&b]);
		assert_eq!(governor.overcommitted(), 0);
	}

	/// A release moves the epoch, so a waiter that registered before it is
	/// woken, and one that registers after it is not.
	#[tokio::test]
	async fn releasing_moves_the_epoch_and_wakes_a_waiter() {
		use std::time::Duration;
		let governor = Arc::new(FairGovernor::new(1000));
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(b.reserve_pinned(800));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let Admit::Wait {
			epoch,
		} = governor.admit(&a, 600, true)
		else {
			panic!("`a` did not wait");
		};
		// Its own registration moved the epoch, so it decides again at once.
		governor.changed(epoch).await;
		let Admit::Wait {
			epoch,
		} = governor.admit(&a, 600, true)
		else {
			panic!("`a` did not keep waiting");
		};
		let quiet = tokio::time::timeout(Duration::from_millis(20), governor.changed(epoch)).await;
		assert!(quiet.is_err(), "the wait ended with nothing released");
		let waiting = {
			let governor = Arc::clone(&governor);
			tokio::spawn(async move { governor.changed(epoch).await })
		};
		tokio::task::yield_now().await;
		b.release_pinned(500);
		tokio::time::timeout(Duration::from_secs(5), waiting)
			.await
			.expect("the release woke the waiter")
			.expect("the waiter ran");
		governor.changed(epoch).await;
	}

	/// Dropping a wait leaves the lease no longer waiting, with no claim.
	#[test]
	fn unwait_clears_waiting() {
		let governor = Arc::new(FairGovernor::new(1000));
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(b.reserve_pinned(800));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert!(matches!(governor.admit(&a, 600, true), Admit::Wait { .. }));
		assert_eq!(governor.waiters.load(Ordering::Relaxed), 1);
		governor.unwait(&a);
		assert!(!a.waiting());
		assert_eq!(a.want.load(Ordering::Relaxed), 0);
		assert_eq!(governor.waiters.load(Ordering::Relaxed), 0);
	}

	/// Overcommit is derived from the pinned bytes the grant does not cover, so
	/// it falls as the lease releases pinned bytes and is never part of the
	/// grants' sum.
	#[test]
	fn overcommit_is_derived_and_given_back_with_pinned_bytes() {
		let governor = Arc::new(FairGovernor::new(100));
		let lease = Lease::open(&dynamic(&governor), 50, 0);
		assert_eq!(governor.admit(&lease, 500, false), Admit::Overcommit);
		assert_eq!(lease.overcommit(), 450);
		assert!(governor.granted() <= governor.limit());
		assert!(!lease.over());
		lease.release_pinned(200);
		assert_eq!((lease.held(), lease.overcommit(), lease.pinned()), (300, 250, 300));
		assert_eq!(governor.overcommitted(), 250);
		lease.release_pinned(300);
		assert_eq!((lease.held(), lease.overcommit()), (0, 0));
		assert_eq!(governor.overcommitted(), 0);
	}

	/// Spillable bytes released while pinned bytes are held past the grant leave
	/// the overcommit where it is, so the lease is not over its grant.
	#[test]
	fn releasing_spillable_bytes_leaves_the_overcommit() {
		let governor = Arc::new(FairGovernor::new(100));
		let lease = Lease::open(&dynamic(&governor), 50, 0);
		assert_eq!(governor.admit(&lease, 500, false), Admit::Overcommit);
		// Spillable bytes the lease holds on top of the pinned ones.
		lease.held.fetch_add(300, Ordering::AcqRel);
		assert!(lease.over());
		lease.release(300);
		assert_eq!((lease.held(), lease.pinned(), lease.overcommit()), (500, 500, 450));
		assert_eq!(governor.overcommitted(), 450);
		assert!(!lease.over());
	}

	/// Spillable growth never lowers a grant below the bytes a lease cannot
	/// spill, so it creates no overcommit.
	#[test]
	fn a_grant_is_not_lowered_below_what_a_lease_cannot_spill() {
		let governor = Arc::new(FairGovernor::new(1000));
		let first = Lease::open(&dynamic(&governor), 100, 0);
		assert!(first.reserve_pinned(900));
		let second = Lease::open(&dynamic(&governor), 100, 0);
		assert!(!second.reserve(300));
		assert_eq!(first.grant(), 900);
		assert!(governor.granted() <= governor.limit());
		assert_eq!(governor.overcommitted(), 0);
	}

	/// Pinning bytes a lowered grant no longer covers is an overcommit, since
	/// spilling will not give them back, and the governor is told once.
	#[test]
	fn pinning_past_the_grant_is_recorded_as_overcommit() {
		let governor = Arc::new(FairGovernor::new(1000));
		let first = Lease::open(&dynamic(&governor), 100, 0);
		assert!(first.reserve(800));
		let second = Lease::open(&dynamic(&governor), 100, 0);
		assert!(second.reserve(300));
		assert_eq!(first.grant(), 500);
		first.pin(800);
		assert_eq!(first.overcommit(), 300);
		assert!(!first.over());
		assert_eq!(governor.overcommitted(), 300);
		assert_eq!(governor.overcommits(), 1);
		// More pinned bytes extend the run without starting another.
		first.pin(0);
		assert_eq!(governor.overcommits(), 1);
	}

	/// A release with no lease waiting leaves the epoch where it is.
	#[test]
	fn a_release_with_no_waiter_does_not_move_the_epoch() {
		let governor = Arc::new(FairGovernor::new(1000));
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		assert!(lease.reserve(400));
		let before = governor.epoch.load(Ordering::Relaxed);
		lease.release(400);
		assert_eq!(governor.epoch.load(Ordering::Relaxed), before);
	}

	/// An open moves the epoch while a lease waits, since the fair share has
	/// changed for it, and otherwise leaves it where it is.
	#[test]
	fn an_open_moves_the_epoch_when_a_lease_waits() {
		let governor = Arc::new(FairGovernor::new(1000));
		let holder = Lease::open(&dynamic(&governor), 100, 0);
		assert!(holder.reserve_pinned(800));
		let before = governor.epoch.load(Ordering::Relaxed);
		let _quiet = Lease::open(&dynamic(&governor), 100, 0);
		assert_eq!(governor.epoch.load(Ordering::Relaxed), before, "an open with no waiter");

		let asker = Lease::open(&dynamic(&governor), 100, 0);
		assert!(matches!(governor.admit(&asker, 800, true), Admit::Wait { .. }));
		let before = governor.epoch.load(Ordering::Relaxed);
		let _opened = Lease::open(&dynamic(&governor), 100, 0);
		assert!(governor.epoch.load(Ordering::Relaxed) > before, "an open while a lease waits");
	}
}
