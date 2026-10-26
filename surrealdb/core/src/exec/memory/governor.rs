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
//! something the governor cannot see still ends its wait. Decisions that weigh
//! the leases together are atomic under the governor's lock; growth that free
//! memory covers while no lease waits is one atomic step on the grants' sum.
//!
//! The governor does not bound what a single query pins: that is the query's
//! own ceiling ([`super::QueryMemory`]). Memory a borrower holds past its
//! grant adds to what the process holds, so where the process has a memory
//! threshold the threshold's guard, which fails whichever query checks it
//! next, is the only node-wide bound on overcommit, unless the governor holds a
//! hard limit.
//!
//! A governor may hold a hard limit ([`MemoryGovernor::hard_limit`]): a bound
//! on the pinned bytes of all its leases together, which a host with no
//! process threshold of its own sets to keep the sum of what its queries
//! cannot spill within its memory. A request that would take the sum past it
//! is answered [`Admit::Refuse`], at once and whatever the lease's age, so the
//! oldest waiter is no exception; the query fails with the memory threshold
//! error. With no hard limit, no request is refused.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use dashmap::DashMap;
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
	/// Nothing is reserved: the bytes would take the pinned bytes of all the
	/// governor's leases past its hard limit. Only a governor with a hard
	/// limit answers it, whether or not the request may wait.
	Refuse,
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

	/// `lease` must hold `bytes` more that it cannot spill. The answer reserves
	/// the bytes on the lease (`Granted`, `Overcommit`), tells the query to
	/// wait, or, only for a governor with a [hard limit](Self::hard_limit),
	/// refuses them (`Refuse`) and reserves nothing. `may_wait` is false once
	/// the caller's bounded wait has run out, and the answer is then never
	/// `Wait`.
	///
	/// The default grows the lease and otherwise grants the bytes past the
	/// governor's limit. A governor that never answers `Wait` needs no
	/// [`changed`](Self::changed). A governor with a hard limit overrides this
	/// to enforce it: [`Lease::reserve_pinned`] asks it in place of reserving.
	fn admit(&self, lease: &Lease, bytes: usize, _may_wait: bool) -> Admit {
		if lease.reserve_pinned_unchecked(bytes) {
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

	/// Set `lease`'s grant to `bytes`, or to its minimum where that is higher,
	/// for [`Lease::set_grant`]. A governor that keeps the grants' sum changes
	/// it with the grant ([`Lease::store_grant`]), in one step no decision of
	/// its own interleaves with; a governor that delegates to another forwards
	/// it. Required, so a governor that wraps one that keeps the sum cannot
	/// leave it uncounted.
	fn regrant(&self, lease: &Lease, bytes: usize);

	/// The node limit the governor holds its leases to, if it has one.
	fn limit(&self) -> Option<usize> {
		None
	}

	/// The most pinned bytes all the governor's leases may hold together, if it
	/// bounds them. A governor that answers `Some` refuses a request past it
	/// with [`Admit::Refuse`] from [`admit`](Self::admit), through which every
	/// reservation of pinned bytes then passes.
	fn hard_limit(&self) -> Option<usize> {
		None
	}

	/// `lease` asks to count `bytes` it already holds as pinned ([`Lease::pin`]),
	/// which no [`admit`](Self::admit) request covers. A governor with a hard
	/// limit counts them only if the pinned bytes of all its leases then stay
	/// within it, deciding atomically with the other leases; otherwise it counts
	/// nothing and answers `false`. The default counts them.
	fn pin(&self, lease: &Lease, bytes: usize) -> bool {
		lease.pin_counted(bytes);
		true
	}
}

/// How many times [`Lease::reserve`] asks the governor to grow the grant for one reservation.
const MOST_GROWTHS: u32 = 16;

/// The bits of [`FairGovernor`]'s 64-bit grants' word that hold the sum: up to
/// 256 TiB, on every target. Every change to the sum stays within these bits:
/// what a sum no node reaches holds past them is kept beside the word
/// ([`FairGovernor::overflow`]), never spilled into the bits above.
const SUM: u64 = (1 << 48) - 1;

/// One lock-free grant counted in the word but not yet published on its lease.
/// The bits above [`SUM`] and below [`CLOSED`] count them.
const IN_FLIGHT: u64 = 1 << 48;

/// The bits that count lock-free grants in flight.
const IN_FLIGHTS: u64 = ((1 << 15) - 1) << 48;

/// The bit of the grants' word set while lock-free grants are closed: while
/// the governor decides under its lock, and while a lease waits.
const CLOSED: u64 = 1 << 63;

/// A growth from free memory raises a grant by at least the grant divided by
/// this.
const GROWTH_SHARE: usize = 8;

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
		self.governor.regrant(self, bytes);
	}

	/// Set the grant to `bytes`, or to the minimum when `bytes` is below it,
	/// answering the grant it replaced. The step of
	/// [`MemoryGovernor::regrant`] that changes the lease; a governor that
	/// keeps the grants' sum changes it with this. Called anywhere else, it
	/// changes the grant without the lease's governor, whose sums then no longer
	/// match its leases: use [`Lease::set_grant`].
	pub fn store_grant(&self, bytes: usize) -> usize {
		self.grant.swap(bytes.max(self.minimum), Ordering::AcqRel)
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
	///
	/// Other holders of the lease reserve against the same grant, so the grant
	/// the governor raised may be used up before these bytes are held. The
	/// governor is asked again for what is then short, as long as it answers
	/// that it grew, up to [`MOST_GROWTHS`] times: an answer of `false` is the
	/// governor's refusal, which tells the query to spill, and never the
	/// outcome of a race between holders.
	pub fn reserve(&self, bytes: usize) -> bool {
		let mut asked = 0;
		loop {
			if self.hold(bytes) {
				return true;
			}
			let want = self.held().saturating_add(bytes);
			// One read of the usable grant: another holder of the lease can
			// shrink it between two reads.
			let usable = self.usable();
			if want <= usable {
				continue;
			}
			if asked == MOST_GROWTHS || !self.governor.grow(self, want - usable) {
				return false;
			}
			asked += 1;
		}
	}

	/// [`reserve`](Self::reserve) bytes the lease cannot spill. Where the
	/// governor has a hard limit the reservation is the governor's decision
	/// ([`MemoryGovernor::admit`], never waiting), made atomically with the
	/// pinned bytes of every other lease; an answer of `false` is then its
	/// refusal and reserves nothing.
	pub(crate) fn reserve_pinned(&self, bytes: usize) -> bool {
		if self.governor.hard_limit().is_some() {
			return !matches!(self.governor.admit(self, bytes, false), Admit::Refuse);
		}
		self.reserve_pinned_unchecked(bytes)
	}

	/// [`reserve_pinned`](Self::reserve_pinned) without the governor's hard
	/// limit.
	fn reserve_pinned_unchecked(&self, bytes: usize) -> bool {
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
	///
	/// Where the governor has a hard limit, the governor decides
	/// ([`MemoryGovernor::pin`]): answers `false` and counts nothing if the
	/// bytes would take the pinned bytes of all its leases past it, and the
	/// caller fails its query. The lease is then unchanged and still holds the
	/// bytes as spillable.
	pub(crate) fn pin(&self, bytes: usize) -> bool {
		if bytes > 0 && self.governor.hard_limit().is_some() {
			return self.governor.pin(self, bytes);
		}
		self.pin_counted(bytes);
		true
	}

	/// [`pin`](Self::pin) without the governor's hard limit: the bytes are
	/// counted whatever the pinned bytes of all leases.
	pub(crate) fn pin_counted(&self, bytes: usize) {
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

	fn regrant(&self, lease: &Lease, bytes: usize) {
		lease.store_grant(bytes);
	}
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

/// How a lock-free open counted its lease's minimum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OpenedFree {
	/// Within the limit.
	WithinLimit,
	/// Past the limit, on a reading that no lease borrowed.
	PastLimit,
}

/// The governor's lock, held. Reopens lock-free grants when it is released
/// with no lease waiting.
struct Deciding<'a> {
	governor: &'a FairGovernor,
	_guard: MutexGuard<'a, ()>,
}

impl Drop for Deciding<'_> {
	fn drop(&mut self) {
		// Runs before the guard is released: waiters change only under the
		// lock, so none starts waiting between this read and the release.
		if self.governor.waiters.load(Ordering::SeqCst) == 0 {
			self.governor.granted.fetch_and(!CLOSED, Ordering::AcqRel);
		}
	}
}

/// `bytes` as a term of the governor's 64-bit sums.
fn wide(bytes: usize) -> u64 {
	u64::try_from(bytes).unwrap_or(u64::MAX)
}

/// A 64-bit sum as bytes, saturating where `usize` is narrower.
fn narrow(sum: u64) -> usize {
	usize::try_from(sum).unwrap_or(usize::MAX)
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
/// held is free for the next to ask. Minimums are guaranteed so that admission
/// never waits: a minimum counted where the limit has no room for it may take
/// the grants' sum past the limit, by the minimums and by what borrowers keep
/// at their fair share.
///
/// A growth from free memory raises a grant by at least an eighth of itself
/// (`GROWTH_SHARE`), more than it asked for where the free memory covers it,
/// so that the holders of one lease that reserve at once do not each ask for a
/// growth of their own.
///
/// The memory a lowered lease holds is given back when it next flushes, so
/// what the node holds may pass the limit by what its queries hold in one
/// batch. Pinned bytes a lease holds past its grant stay held until the lease
/// releases them; the bytes all leases hold so are counted by
/// [`FairGovernor::overcommitted`].
///
/// Every query opens a lease, so opening, closing and growing from free
/// memory are the governor's hot path: while no lease waits, each takes no
/// lock. Each is a compare-and-swap on the grants' sum
/// ([`FairGovernor::granted`]) and a few atomic updates beside it (what is
/// borrowed, a grant, the count of grants in flight), and an open or a close
/// one entry in a sharded map of the leases. An open past the limit is as
/// cheap while no lease borrows above its minimum, since no reclaim would give
/// anything back. The governor takes its lock and weighs the leases together
/// only where a lease waits, or free memory does not cover a request and a
/// lease may have memory to give back.
pub struct FairGovernor {
	limit: usize,
	/// The open leases, by id.
	leases: DashMap<u64, Weak<Lease>>,
	/// The sum of the open leases' grants, with [`CLOSED`] set while the
	/// governor decides under its lock or a lease waits. Raised before a grant
	/// rises and lowered after a grant falls, so it never reads below the
	/// grants it sums; raised past the limit by a minimum counted past it, and by
	/// what a lowered borrower keeps at its fair share. A grant made without the lock checks the
	/// flag in the same step as it raises the sum, so it never takes memory a locked decision is
	/// weighing or a waiting lease was left.
	granted: AtomicU64,
	/// What the open leases are granted above their minimums. Raised before a
	/// lock-free grant is counted in `granted` and lowered after a grant falls,
	/// so it never reads below what the counted leases borrow; zero means no
	/// reclaim would give anything back.
	borrowed: AtomicU64,
	/// Set, and never cleared, once what is borrowed has passed what
	/// `borrowed` can count: from then on no open trusts it to read nothing
	/// borrowed, and every open past the limit takes the lock.
	borrowed_lost: AtomicBool,
	/// What the grants sum to past [`SUM`], which the grants' word cannot hold.
	/// Raised only under the lock, where a minimum or a regrant takes the sum
	/// past it, and drawn on first as grants are uncounted, so the grants'
	/// sum is the word's sum and this together.
	overflow: Mutex<u128>,
	/// Whether `overflow` may hold anything: set under its lock before the
	/// word's sum is held at its bound, and cleared there once it is empty.
	/// A close reads it to know whether to draw on `overflow`.
	overflowing: AtomicBool,
	/// Held across every decision that weighs the leases together, so two
	/// leases cannot both be granted what one lowered lease gave back.
	deciding: Mutex<()>,
	/// Moves on a release or close while a lease waits, and on every change in
	/// who waits or what they want; a waiter whose epoch is behind it decides
	/// again.
	epoch: AtomicU64,
	changed: Notify,
	/// The leases marked waiting. Changed only under the lock, which leaves
	/// [`CLOSED`] set in `granted` while any lease waits.
	waiters: AtomicUsize,
	waits: AtomicU64,
	overcommits: AtomicU64,
	refusals: AtomicU64,
	/// The most pinned bytes all leases may hold together, if bounded.
	hard_limit: Option<usize>,
}

impl std::fmt::Debug for FairGovernor {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("FairGovernor")
			.field("limit", &self.limit)
			.field("hard_limit", &self.hard_limit)
			.finish()
	}
}

impl FairGovernor {
	/// A governor holding its node to `limit` bytes.
	pub fn new(limit: usize) -> Self {
		Self {
			limit: narrow(wide(limit).min(SUM)),
			leases: DashMap::new(),
			granted: AtomicU64::new(0),
			borrowed: AtomicU64::new(0),
			borrowed_lost: AtomicBool::new(false),
			overflow: Mutex::new(0),
			overflowing: AtomicBool::new(false),
			deciding: Mutex::new(()),
			epoch: AtomicU64::new(0),
			changed: Notify::new(),
			waiters: AtomicUsize::new(0),
			waits: AtomicU64::new(0),
			overcommits: AtomicU64::new(0),
			refusals: AtomicU64::new(0),
			hard_limit: None,
		}
	}

	/// Bound the pinned bytes of all leases together to `bytes`: a request that
	/// would take their sum past it is refused ([`Admit::Refuse`]) whatever the
	/// lease's age, where without one no request is. Pinned bytes a lease holds
	/// past its grant (its overcommit) count toward the sum like any others.
	pub fn with_hard_limit(mut self, bytes: usize) -> Self {
		self.hard_limit = Some(bytes);
		self
	}

	/// The node's limit, in bytes.
	pub fn limit(&self) -> usize {
		self.limit
	}

	/// The sum of the open leases' grants.
	pub fn granted(&self) -> usize {
		let sum = self.granted.load(Ordering::Acquire) & SUM;
		let overflow = if self.overflowing.load(Ordering::Acquire) {
			*self.overflow.lock().unwrap_or_else(|e| e.into_inner())
		} else {
			0
		};
		usize::try_from(u128::from(sum) + overflow).unwrap_or(usize::MAX)
	}

	/// How many times a lease has been told to wait, counting each wait once
	/// however often its query asks again.
	pub fn waits(&self) -> u64 {
		self.waits.load(Ordering::Relaxed)
	}

	/// How many requests the hard limit has refused.
	pub fn refusals(&self) -> u64 {
		self.refusals.load(Ordering::Relaxed)
	}

	/// How many times a lease came to hold memory past its grant, counting
	/// each run of it once however many requests extend it.
	pub fn overcommits(&self) -> u64 {
		self.overcommits.load(Ordering::Relaxed)
	}

	/// The bytes the open leases hold past their grants now: the sum, over
	/// leases whose pinned bytes pass their usable grant, of the difference.
	pub fn overcommitted(&self) -> usize {
		let open = self.live();
		let overcommitted = Self::overcommit_of(&open);
		drop(open);
		overcommitted
	}

	/// The open leases. Dropping the last handle on one closes it, so the
	/// caller drops them only once it no longer holds the governor's lock.
	fn live(&self) -> Vec<Arc<Lease>> {
		self.leases.iter().filter_map(|lease| lease.value().upgrade()).collect()
	}

	/// The open leases, under the lock, once no lease is part way through
	/// closing: one whose last handle has dropped is no longer found, but its
	/// grant is counted until [`MemoryGovernor::closed`] uncounts it and drops
	/// its record, which it does without the lock. Waiting it out keeps a
	/// decision from weighing a sum above the grants it can find.
	///
	/// Only the leases found part way through closing are waited for: a lease
	/// that starts closing later is uncounted by its own close. Every close
	/// finishes without the lock, so the wait ends once their threads run; a
	/// governor that forwards [`MemoryGovernor::opened`] to this one must
	/// forward [`MemoryGovernor::closed`] too.
	fn open(&self) -> Vec<Arc<Lease>> {
		// Held while the rest are waited out, so none of these starts closing.
		let mut open = Vec::new();
		let mut closing = Vec::new();
		for lease in self.leases.iter() {
			match lease.value().upgrade() {
				Some(lease) => open.push(lease),
				None => closing.push(*lease.key()),
			}
		}
		let mut spins = 0u32;
		closing.retain(|id| self.leases.get(id).is_some_and(|lease| lease.strong_count() == 0));
		while !closing.is_empty() {
			if spins < 64 {
				std::hint::spin_loop();
			} else {
				std::thread::yield_now();
			}
			spins = spins.saturating_add(1);
			closing.retain(|id| self.leases.get(id).is_some_and(|lease| lease.strong_count() == 0));
		}
		open
	}

	/// The governor's lock, taken across a decision that weighs the leases
	/// together. Lock-free grants are closed until it is released, and stay
	/// closed then while a lease waits.
	fn lock(&self) -> Deciding<'_> {
		let guard = self.deciding.lock().unwrap_or_else(|e| e.into_inner());
		// Closed first, so no lock-free grant starts; then the ones already
		// counted are waited out, so every grant the sum counts is on its
		// lease before the decision reads either.
		let mut word = self.granted.fetch_or(CLOSED, Ordering::AcqRel);
		let mut spins = 0u32;
		while word & IN_FLIGHTS != 0 {
			if spins < 64 {
				std::hint::spin_loop();
			} else {
				std::thread::yield_now();
			}
			spins = spins.saturating_add(1);
			word = self.granted.load(Ordering::Acquire);
		}
		Deciding {
			governor: self,
			_guard: guard,
		}
	}

	/// Lower the grants' sum by the `lowered` bytes a decision took from other
	/// leases, and raise it by `want` where the sum then leaves `want` and
	/// `reserve` more within the limit, in one step: no other request is
	/// granted memory between the two. Answers whether `want` was added.
	fn settle(&self, lowered: u128, want: usize, reserve: usize) -> bool {
		let mut added = false;
		let lowered = self.draw_overflow(lowered);
		let (want, reserve, limit) = (wide(want), wide(reserve), wide(self.limit));
		let beside = self.overflowed();
		let _ = self.granted.fetch_update(Ordering::AcqRel, Ordering::Acquire, |granted| {
			let base = (granted & SUM).saturating_sub(lowered);
			added =
				base.saturating_add(beside).saturating_add(want).saturating_add(reserve) <= limit;
			let sum = if added {
				base + want
			} else {
				base
			};
			Some(sum | (granted & !SUM))
		});
		added
	}

	/// [`settle`](Self::settle), adding as much of `want` as the limit leaves
	/// room for, and answering how much that was.
	fn settle_upto(&self, lowered: u128, want: usize) -> usize {
		let mut added = 0;
		let lowered = self.draw_overflow(lowered);
		let limit = wide(self.limit);
		let beside = self.overflowed();
		let _ = self.granted.fetch_update(Ordering::AcqRel, Ordering::Acquire, |granted| {
			let base = (granted & SUM).saturating_sub(lowered);
			added = want.min(narrow(limit.saturating_sub(base.saturating_add(beside))));
			Some((base + wide(added)) | (granted & !SUM))
		});
		added
	}

	/// Grant `lease` `bytes` more without the lock, where lock-free grants are
	/// open and the limit leaves room for them. Answers whether it was granted.
	///
	/// The sum rises, and the grant is counted in flight, in one step; the
	/// grant is then raised on the lease and the count dropped, and the lock
	/// waits for that before any decision reads the leases.
	fn grant_free(&self, lease: &Lease, bytes: usize) -> bool {
		// Counted as borrowed first, so no open that weighs the sum after this
		// grant reads that nothing is borrowed.
		if bytes > self.limit {
			// Never granted without the lock, and never counted as borrowed:
			// what is counted here stays within the limit, so it cannot wrap.
			return false;
		}
		self.borrow(wide(bytes));
		let taken = self
			.granted
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |granted| {
				let sum = (granted & SUM).saturating_add(wide(bytes));
				// A full count of grants in flight sends this one to the lock
				// rather than carry into the flag.
				let room = granted & IN_FLIGHTS != IN_FLIGHTS;
				// Read after the flag: a count past the word is made under the
				// lock, before the lock clears the flag.
				let whole = !self.overflowing.load(Ordering::Acquire);
				(granted & CLOSED == 0 && room && whole && sum <= wide(self.limit))
					.then_some((sum | (granted & !SUM)) + IN_FLIGHT)
			})
			.is_ok();
		if taken {
			lease.grant.fetch_add(bytes, Ordering::AcqRel);
			self.granted.fetch_sub(IN_FLIGHT, Ordering::AcqRel);
		} else {
			self.borrowed.fetch_sub(wide(bytes), Ordering::AcqRel);
		}
		taken
	}

	/// Count a new lease's `minimum` without the lock, where no lease waits and
	/// either the limit leaves room for it or no lease is granted above its
	/// minimum, so that no reclaim would give anything back. Answers whether it
	/// was counted.
	///
	/// What is borrowed is read inside the swap, but is not part of what it
	/// compares: a growth and a close of the same size between the read and the
	/// swap leave the word as it was. An open counted past the limit therefore
	/// reads what is borrowed again once its swap has landed, and reclaims under
	/// the lock if anything is (see [`MemoryGovernor::opened`]); a growth whose
	/// swap lands after it finds the sum past the limit and is refused.
	fn open_free(&self, minimum: usize) -> Option<OpenedFree> {
		self.granted
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |granted| {
				if granted & CLOSED != 0 || self.overflowing.load(Ordering::Acquire) {
					return None;
				}
				let counted = granted & SUM;
				let sum = counted.saturating_add(wide(minimum));
				let room = sum <= wide(self.limit)
					|| self.borrowed.load(Ordering::Acquire) == 0
						&& !self.borrowed_lost.load(Ordering::Acquire)
						&& sum <= SUM;
				room.then_some(sum | (granted & !SUM))
			})
			.ok()
			.map(|granted| {
				if (granted & SUM).saturating_add(wide(minimum)) <= wide(self.limit) {
					OpenedFree::WithinLimit
				} else {
					OpenedFree::PastLimit
				}
			})
	}

	/// Whether any lease may be granted above its minimum.
	fn may_borrow(&self) -> bool {
		self.borrowed.load(Ordering::Acquire) != 0 || self.borrowed_lost.load(Ordering::Acquire)
	}

	/// Lower the grants' sum by `bytes`, to no less than zero, leaving the
	/// flag and the grants in flight as they are.
	fn uncount(&self, bytes: u128) {
		let rest = self.draw_overflow(bytes);
		self.lower_word(rest);
	}

	/// Take as much of `bytes` being uncounted as [`FairGovernor::overflow`]
	/// holds from it, answering what the word's sum must still be lowered by.
	fn draw_overflow(&self, bytes: u128) -> u64 {
		if !self.overflowing.load(Ordering::Acquire) {
			// Nothing past the bound: every grant still counted is in the word.
			return u64::try_from(bytes).unwrap_or(u64::MAX);
		}
		let mut overflow = self.overflow.lock().unwrap_or_else(|e| e.into_inner());
		let drawn = (*overflow).min(bytes);
		*overflow -= drawn;
		if *overflow == 0 {
			self.overflowing.store(false, Ordering::Release);
		}
		u64::try_from(bytes - drawn).unwrap_or(u64::MAX)
	}

	/// What the grants sum to past the word's bound, held at what a `u64` holds.
	fn overflowed(&self) -> u64 {
		if !self.overflowing.load(Ordering::Acquire) {
			return 0;
		}
		let overflow = *self.overflow.lock().unwrap_or_else(|e| e.into_inner());
		u64::try_from(overflow).unwrap_or(u64::MAX)
	}

	/// Lower the word's sum by `bytes`, to no less than zero, leaving the flag
	/// and the grants in flight as they are.
	fn lower_word(&self, bytes: u64) {
		let _ = self.granted.fetch_update(Ordering::AcqRel, Ordering::Acquire, |granted| {
			Some((granted & SUM).saturating_sub(bytes) | (granted & !SUM))
		});
	}

	/// Raise the grants' sum by `bytes` whatever the limit, keeping what the
	/// word cannot hold in [`FairGovernor::overflow`], which no sum of grants
	/// passes. Under the lock.
	fn count(&self, bytes: usize) {
		let mut overflow = self.overflow.lock().unwrap_or_else(|e| e.into_inner());
		// Marked first, so a close that finds the word held at its bound draws
		// on what is kept beside it.
		self.overflowing.store(true, Ordering::Release);
		let mut over = 0u128;
		let _ = self.granted.fetch_update(Ordering::AcqRel, Ordering::Acquire, |granted| {
			let sum = u128::from(granted & SUM) + u128::from(wide(bytes));
			over = sum.saturating_sub(u128::from(SUM));
			Some(u64::try_from(sum.min(u128::from(SUM))).unwrap_or(SUM) | (granted & !SUM))
		});
		*overflow += over;
		if *overflow == 0 {
			self.overflowing.store(false, Ordering::Release);
		}
	}

	/// Count `bytes` more borrowed, holding the count at its bound rather than
	/// wrapping it to read as nothing borrowed; past the bound the count is
	/// lost for good, and no open trusts it again.
	fn borrow(&self, bytes: u64) {
		let _ = self.borrowed.fetch_update(Ordering::AcqRel, Ordering::Acquire, |borrowed| {
			if borrowed.checked_add(bytes).is_none() {
				self.borrowed_lost.store(true, Ordering::Release);
			}
			Some(borrowed.saturating_add(bytes))
		});
	}

	/// Mark one more lease waiting. Under the lock.
	fn add_waiter(&self) {
		self.waiters.fetch_add(1, Ordering::SeqCst);
	}

	/// Mark one lease fewer waiting. Under the lock.
	fn drop_waiter(&self) {
		self.waiters.fetch_sub(1, Ordering::SeqCst);
	}

	/// Raise `lease`'s grant by `bytes` the grants' sum already counts, under
	/// the lock.
	fn raise(&self, lease: &Lease, bytes: usize) {
		self.borrow(wide(bytes));
		lease.grant.fetch_add(bytes, Ordering::AcqRel);
	}

	/// Lower `lease`'s grant to `to`, or to its minimum where that is higher,
	/// answering by how much it fell. A grant already at or below it is left
	/// where it is, so a grant lowered at the same time through
	/// [`Lease::set_grant`] is never raised here. The caller lowers the grants'
	/// sum.
	fn lower_grant(&self, lease: &Lease, to: usize) -> usize {
		let to = to.max(lease.minimum);
		match lease
			.grant
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |grant| (grant > to).then_some(to))
		{
			Ok(grant) => {
				self.borrowed.fetch_sub(wide(grant - to), Ordering::AcqRel);
				grant - to
			}
			Err(_) => 0,
		}
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
		if self.settle(0, bytes, Self::claims(open, lease)) {
			self.raise(lease, bytes);
			return true;
		}
		let fair = self.limit / open.len().max(1);
		let lowered = self.reclaim(open, lease.id, fair, Reclaim::Spillable);
		let room = fair.max(lease.minimum).saturating_sub(lease.grant());
		let want = if room >= bytes {
			bytes
		} else {
			0
		};
		let grown = self.settle(lowered, want, 0) && want > 0;
		if grown {
			self.raise(lease, bytes);
		}
		grown
	}
}

impl FairGovernor {
	/// Lower every lease of `open` but `except` that is above `fair` to it, or
	/// to the floor `mode` names where that is higher, answering by how much
	/// the grants fell; the caller lowers the grants' sum by it. A lease the
	/// lowering leaves holding pinned bytes its grant does not cover has begun
	/// an overcommit.
	fn reclaim(&self, open: &[Arc<Lease>], except: u64, fair: usize, mode: Reclaim) -> u128 {
		let mut lowered = 0u128;
		for other in open {
			if other.id == except {
				continue;
			}
			let floor = match mode {
				Reclaim::Spillable => fair.max(other.floor()),
				Reclaim::Guaranteed => fair.max(other.minimum),
			};
			if other.grant() > floor {
				let before = other.overcommit();
				lowered += u128::from(wide(self.lower_grant(other, floor)));
				self.note(open, other, before);
			}
		}
		lowered
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
		// The hard limit is judged first and for every request, so no branch
		// below reserves bytes past it.
		if let Some(hard) = self.hard_limit {
			let pinned: usize = open.iter().map(|other| other.pinned()).sum();
			if pinned.saturating_add(bytes) > hard {
				self.refusals.fetch_add(1, Ordering::Relaxed);
				return Admit::Refuse;
			}
		}
		let before = lease.overcommit();
		let fair = self.limit / open.len().max(1);
		let claims = Self::claims(open, lease);
		let free = self.limit.saturating_sub(self.granted()).saturating_sub(claims);
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
			let lowered = if free < shortfall {
				self.reclaim(open, lease.id, fair, Reclaim::Guaranteed)
			} else {
				0
			};
			let take = self.settle_upto(lowered, shortfall);
			self.raise(lease, take);
			if lease.hold_pinned(bytes) {
				return Admit::Granted;
			}
			lease.force_pinned(bytes);
			self.note(open, lease, before);
			return Admit::Overcommit;
		}

		// Borrow from free memory that no waiting lease has claimed. Others do
		// not give back for a borrower.
		if shortfall <= free && self.settle(0, shortfall, claims) {
			self.raise(lease, shortfall);
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
			// Marked waiting before anything is given back: the lock already
			// closes lock-free grants, and a lease marked waiting keeps them
			// closed once the lock is released, so none takes what the reclaim
			// frees for it.
			let first = !lease.waiting.swap(true, Ordering::AcqRel);
			let changed = lease.want.swap(shortfall, Ordering::AcqRel) != shortfall;
			if first {
				self.add_waiter();
				self.waits.fetch_add(1, Ordering::Relaxed);
			}
			if can_return {
				let mut lowered = 0u128;
				for (other, _) in &waitable {
					if other.spillable() > 0 {
						lowered += u128::from(wide(
							self.lower_grant(other, other.grant().min(fair.max(other.floor()))),
						));
					}
				}
				self.settle(lowered, 0, 0);
			}
			// A lease that starts waiting changes who is waitable and what is
			// claimed for every other waiter, as does a changed claim; a lease
			// that waits again for the same bytes changes nothing.
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

	/// Lower the leases borrowing past their fair share, as a minimum counted
	/// past the limit lowers them, for `lease`, whose minimum is counted.
	fn reclaim_for(&self, lease: &Arc<Lease>) {
		let guard = self.lock();
		let open = self.open();
		let fair = self.limit / open.len().max(1);
		let lowered = self.reclaim(&open, lease.id, fair, Reclaim::Guaranteed);
		self.settle(lowered, 0, 0);
		drop(guard);
		drop(open);
		if self.waiters.load(Ordering::SeqCst) > 0 {
			self.bump();
		}
	}

	/// [`MemoryGovernor::opened`] where free memory does not cover the minimum
	/// or a lease waits: under the lock, leases borrowing past their fair share
	/// make room for the minimum, pinned bytes included.
	fn opened_slowly(&self, lease: &Arc<Lease>) {
		let guard = self.lock();
		if !self.settle(0, lease.minimum, 0) {
			let open = self.open();
			let fair = self.limit / open.len().max(1);
			let lowered = self.reclaim(&open, lease.id, fair, Reclaim::Guaranteed);
			// A minimum is granted whatever the reclaim gave back, so the sum may
			// pass the limit by the minimums and by what borrowers keep at
			// their fair share.
			// Two steps, under the lock and with lock-free grants closed, so no
			// grant falls between them.
			self.uncount(lowered);
			self.count(lease.minimum);
			drop(guard);
			drop(open);
		} else {
			drop(guard);
		}
		// The fair share has changed for every waiter.
		if self.waiters.load(Ordering::SeqCst) > 0 {
			self.bump();
		}
	}
}

impl MemoryGovernor for FairGovernor {
	fn opened(&self, lease: &Arc<Lease>) {
		// With no lease waiting, an open weighs no other lease where the minimum
		// is free, or where no lease borrows above its own minimum, so that no
		// reclaim would give anything back: the minimums alone then pass the
		// limit, which they are guaranteed to. It counts the minimum and records
		// the lease.
		// Recorded before it is counted, so a scan under the lock never weighs
		// a sum that counts a lease it cannot find.
		self.leases.insert(lease.id, Arc::downgrade(lease));
		match self.open_free(lease.minimum) {
			Some(OpenedFree::WithinLimit) => {}
			// Counted past the limit on a reading that nothing was borrowed: any
			// growth whose swap came before this open's is seen now, and is
			// reclaimed as the open would have reclaimed it under the lock.
			Some(OpenedFree::PastLimit) if self.may_borrow() => self.reclaim_for(lease),
			Some(OpenedFree::PastLimit) => {}
			None => self.opened_slowly(lease),
		}
	}

	fn grow(&self, lease: &Lease, bytes: usize) -> bool {
		// Free memory no waiting lease has claimed covers the growth: no other
		// lease is weighed. Where it covers more, the grant rises by an eighth
		// of itself at least, so the holders of one lease that reserve at once
		// do not each ask for, and race over, a growth of their own.
		let ample = bytes.max(lease.grant() / GROWTH_SHARE);
		if ample > bytes && self.grant_free(lease, ample) {
			return true;
		}
		if self.grant_free(lease, bytes) {
			return true;
		}
		// The lock is held across the decision, so two leases growing at once
		// cannot both be granted what one lowered lease gave back. The leases
		// are released only after it, since dropping the last handle on one
		// closes it, which may take the lock.
		let guard = self.lock();
		let open = self.open();
		let grown = self.decide(&open, lease, bytes);
		drop(guard);
		drop(open);
		grown
	}

	fn closed(&self, lease: &Lease) {
		let grant = lease.grant();
		self.uncount(u128::from(wide(grant)));
		if grant > lease.minimum {
			self.borrowed.fetch_sub(wide(grant - lease.minimum), Ordering::AcqRel);
		}
		self.leases.remove(&lease.id);
		if lease.waiting() {
			let _guard = self.lock();
			if lease.waiting.swap(false, Ordering::AcqRel) {
				lease.want.store(0, Ordering::Release);
				self.drop_waiter();
			}
		}
		if self.waiters.load(Ordering::SeqCst) > 0 {
			self.bump();
		}
	}

	fn admit(&self, lease: &Lease, bytes: usize, may_wait: bool) -> Admit {
		// A lease that does not wait and whose grant, or the node's free memory
		// while no lease waits, covers the bytes is granted them without the
		// lock, as the decision would grant them. A governor with a hard limit
		// decides every request under the lock, which is the only place the
		// pinned bytes of all leases are read together.
		if self.hard_limit.is_none() && !lease.waiting() {
			if lease.hold_pinned(bytes) {
				return Admit::Granted;
			}
			let shortfall = lease.held().saturating_add(bytes).saturating_sub(lease.usable());
			if self.grant_free(lease, shortfall) && lease.hold_pinned(bytes) {
				return Admit::Granted;
			}
		}
		// Held across the decision, as in `grow`, and the leases dropped only
		// after it.
		let guard = self.lock();
		let open = self.open();
		let was_waiting = lease.waiting();
		let admitted = self.decide_admit(&open, lease, bytes, may_wait);
		if was_waiting && !matches!(admitted, Admit::Wait { .. }) {
			// The set of leases that are not waiting grew, and its claim is gone.
			lease.waiting.store(false, Ordering::Release);
			lease.want.store(0, Ordering::Release);
			self.drop_waiter();
			self.bump();
		}
		drop(guard);
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
		let open = self.live();
		self.episode(&open, lease);
		drop(open);
	}

	fn unwait(&self, lease: &Lease) {
		let _guard = self.lock();
		if lease.waiting.swap(false, Ordering::AcqRel) {
			lease.want.store(0, Ordering::Release);
			self.drop_waiter();
		}
		self.bump();
	}

	fn regrant(&self, lease: &Lease, bytes: usize) {
		// Under the lock, so no decision weighs the lease between the grant
		// and the sum.
		let _deciding = self.lock();
		// No grant is set past what the grants' word can sum: no node holds
		// that much, and what is borrowed above it could not be counted.
		let from = lease.store_grant(bytes.min(narrow(SUM)));
		let to = lease.grant();
		self.count(to);
		self.uncount(u128::from(wide(from)));
		let (from, to) = (wide(from), wide(to));
		if to > from {
			self.borrow(to - from);
		} else {
			self.borrowed.fetch_sub(from - to, Ordering::AcqRel);
		}
	}

	fn limit(&self) -> Option<usize> {
		Some(self.limit)
	}

	fn hard_limit(&self) -> Option<usize> {
		self.hard_limit
	}

	fn pin(&self, lease: &Lease, bytes: usize) -> bool {
		let Some(hard) = self.hard_limit else {
			lease.pin_counted(bytes);
			return true;
		};
		let guard = self.lock();
		let open = self.open();
		let pinned: usize = open.iter().map(|other| other.pinned()).sum();
		let counted = pinned.saturating_add(bytes) <= hard;
		if counted {
			let before = lease.overcommit();
			lease.pinned.fetch_add(bytes, Ordering::AcqRel);
			self.note(&open, lease, before);
		} else {
			self.refusals.fetch_add(1, Ordering::Relaxed);
		}
		drop(guard);
		drop(open);
		counted
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

	/// Leases opened, grown, lowered by one another and closed from many threads
	/// at once leave the grants' sum exact: nothing is counted once every lease
	/// has closed.
	#[test]
	fn the_grants_sum_is_exact_under_concurrent_opens_and_closes() {
		let governor = Arc::new(FairGovernor::new(256 * 1024));
		let threads: Vec<_> = (0..16)
			.map(|_| {
				let governor = Arc::clone(&governor);
				std::thread::spawn(move || {
					for round in 0..2000 {
						let lease = Lease::open(&dynamic(&governor), 1024, 0);
						if !lease.reserve(512 * (round % 64)) || lease.over() {
							lease.release(lease.held());
						}
						if round % 7 == 0 {
							let admitted = governor.admit(&lease, 256, false);
							assert!(!matches!(admitted, Admit::Wait { .. }));
						}
						assert!(governor.granted() <= governor.limit());
					}
				})
			})
			.collect();
		for thread in threads {
			thread.join().expect("a lease thread");
		}
		assert_eq!(governor.granted(), 0, "a closed lease's grant is still counted");
		assert!(governor.leases.is_empty(), "a closed lease is still recorded");
		assert_eq!(governor.borrowed.load(Ordering::Relaxed), 0, "a closed lease still borrows");
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		assert!(lease.reserve(1000));
		assert_eq!(governor.granted(), lease.grant());
	}

	/// Leases whose minimums alone pass the limit are each granted their
	/// minimum, and a borrower among them is still lowered when the next lease
	/// opens.
	#[test]
	fn minimums_past_the_limit_are_granted_and_a_borrower_still_gives_back() {
		let governor = Arc::new(FairGovernor::new(1000));
		let leases: Vec<_> = (0..20).map(|_| Lease::open(&dynamic(&governor), 100, 0)).collect();
		assert_eq!(governor.granted(), 2000);
		assert_eq!(governor.borrowed.load(Ordering::Relaxed), 0);
		drop(leases);
		assert_eq!(governor.granted(), 0);

		let borrower = Lease::open(&dynamic(&governor), 100, 0);
		assert!(borrower.reserve(900));
		assert_eq!(governor.borrowed.load(Ordering::Relaxed), 800);
		let others: Vec<_> = (0..9).map(|_| Lease::open(&dynamic(&governor), 100, 0)).collect();
		assert_eq!(borrower.grant(), 100, "the borrower was not lowered to its fair share");
		assert_eq!(governor.granted(), 1000);
		assert_eq!(governor.borrowed.load(Ordering::Relaxed), 0);
		drop(others);
	}

	/// A reclaim never raises a grant that was lowered below its floor at the
	/// same time: the lease keeps the lower grant and nothing is counted as
	/// given back.
	#[test]
	fn a_reclaim_never_raises_a_grant() {
		let governor = Arc::new(FairGovernor::new(1000));
		let lease = Lease::open(&dynamic(&governor), 10, 0);
		assert!(lease.reserve(90));
		lease.set_grant(30);
		assert_eq!(governor.granted(), 30);
		assert_eq!(governor.lower_grant(&lease, 50), 0);
		assert_eq!(lease.grant(), 30);
		assert_eq!(governor.lower_grant(&lease, 20), 10);
		assert_eq!(lease.grant(), 20);
	}

	/// While a lease waits no grant is made without the lock, so no growth that
	/// read the governor before the lease began waiting takes what its reclaim
	/// freed; once nothing waits, free memory is granted without it again.
	#[test]
	fn no_lock_free_grant_while_a_lease_waits() {
		let governor = Arc::new(FairGovernor::new(1000));
		let holder = Lease::open(&dynamic(&governor), 100, 0);
		assert!(holder.reserve_pinned(500));
		let waiter = Lease::open(&dynamic(&governor), 100, 0);
		let grower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(governor.grant_free(&grower, 1));
		grower.set_grant(0);
		assert!(matches!(governor.admit(&waiter, 900, true), Admit::Wait { .. }));
		assert_eq!(governor.granted(), 600);
		assert!(!governor.grant_free(&grower, 1), "a lock-free grant while a lease waits");
		governor.unwait(&waiter);
		assert!(governor.grant_free(&grower, 1));
		assert_eq!(governor.granted(), 601);
	}

	/// A lease part way through closing never lets an open past the limit
	/// skip the reclaim of a lease that borrows: what is borrowed is counted
	/// lease by lease, not inferred from the sums.
	#[test]
	fn a_closing_lease_does_not_hide_a_borrower() {
		let governor = Arc::new(FairGovernor::new(1000));
		let borrower = Lease::open(&dynamic(&governor), 100, 0);
		assert!(borrower.reserve(500));
		let closing = Lease::open(&dynamic(&governor), 300, 0);
		assert_eq!(governor.granted(), borrower.grant() + 300);
		// `closing` has begun to close: its grant is uncounted.
		governor.granted.fetch_sub(300, Ordering::AcqRel);
		assert!(governor.open_free(600).is_none(), "an open past the limit skipped the reclaim");
		governor.granted.fetch_add(300, Ordering::AcqRel);
		drop(closing);
		let opened = Lease::open(&dynamic(&governor), 600, 0);
		assert_eq!(borrower.grant(), 500, "the borrower was not lowered to its fair share");
		assert_eq!(governor.granted(), borrower.grant() + opened.grant());
		drop((borrower, opened));
		assert_eq!(governor.granted(), 0);
		assert_eq!(governor.borrowed.load(Ordering::Relaxed), 0);
	}

	/// A grants' sum of 2 GiB, the top bit of a 32-bit `usize`, is an ordinary
	/// sum: it reads back whole and marks no lease waiting.
	#[test]
	fn a_two_gib_sum_is_not_a_waiter() {
		let governor = Arc::new(FairGovernor::new(1 << 20));
		let leases: Vec<_> =
			(0..512).map(|_| Lease::open(&dynamic(&governor), 4 << 20, 0)).collect();
		assert_eq!(governor.granted(), 2 << 30);
		assert_eq!(governor.waiters.load(Ordering::Relaxed), 0);
		assert!(governor.open_free(4 << 20).is_some(), "a phantom waiter sent an open to the lock");
		governor.settle(4 << 20, 0, 0);
		drop(leases);
		assert_eq!(governor.granted(), 0);
	}

	/// No grant is made without the lock while the governor decides under it,
	/// so a forced minimum or a reclaim is weighed against a sum no lock-free
	/// grant moves; the grants reopen with the lock once no lease waits.
	#[test]
	fn lock_free_grants_are_closed_while_the_governor_decides() {
		let governor = Arc::new(FairGovernor::new(1000));
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		let deciding = governor.lock();
		assert!(!governor.grant_free(&lease, 1), "a lock-free growth during a decision");
		assert!(governor.open_free(1).is_none(), "a lock-free open during a decision");
		assert_eq!(governor.granted(), 100);
		drop(deciding);
		assert!(governor.grant_free(&lease, 1));
		lease.set_grant(0);
		assert_eq!(governor.granted(), 100);
		// A minimum that does not fit is forced under the lock: the total is
		// the leases' minimums, whatever grew while it was weighed.
		let wide = Lease::open(&dynamic(&governor), 950, 0);
		assert_eq!(governor.granted(), lease.grant() + wide.grant());
		assert_eq!(governor.leases.len(), 2);
	}

	/// The lock waits out a lock-free grant the sum counts and its lease does
	/// not yet hold, so no decision weighs a sum above the grants it can find.
	#[test]
	fn the_lock_waits_out_a_grant_in_flight() {
		let governor = Arc::new(FairGovernor::new(1000));
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		// A lock-free growth of 900 counted in the sum, not yet on the lease.
		governor.borrowed.fetch_add(900, Ordering::AcqRel);
		governor.granted.fetch_add(900 + IN_FLIGHT, Ordering::AcqRel);
		let locked = Arc::new(AtomicBool::new(false));
		let deciding = {
			let (governor, locked) = (Arc::clone(&governor), Arc::clone(&locked));
			std::thread::spawn(move || {
				let _deciding = governor.lock();
				locked.store(true, Ordering::Release);
			})
		};
		std::thread::sleep(std::time::Duration::from_millis(50));
		assert!(!locked.load(Ordering::Acquire), "the lock was taken with a grant in flight");
		lease.grant.fetch_add(900, Ordering::AcqRel);
		governor.granted.fetch_sub(IN_FLIGHT, Ordering::AcqRel);
		deciding.join().expect("the deciding thread");
		assert!(locked.load(Ordering::Acquire));
		assert_eq!(governor.granted(), lease.grant());
	}

	/// A grant set through [`Lease::set_grant`] moves the grants' sum with it.
	#[test]
	fn set_grant_moves_the_sum_with_the_grant() {
		let governor = Arc::new(FairGovernor::new(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		a.set_grant(900);
		assert_eq!(governor.granted(), 1000);
		assert!(b.reserve_pinned(400));
		assert_eq!(governor.granted(), a.grant() + b.grant());
		a.set_grant(0);
		assert_eq!(governor.granted(), a.grant() + b.grant());
		drop((a, b));
		assert_eq!(governor.granted(), 0);
	}

	/// A decision waits out a lease part way through closing, whose grant is
	/// still counted though the lease can no longer be found, so it weighs
	/// what the close gives back rather than overcommitting.
	#[test]
	fn a_decision_waits_out_a_closing_lease() {
		let governor = Arc::new(FairGovernor::new(1000));
		let asker = Lease::open(&dynamic(&governor), 100, 0);
		// A lease of 900 whose last handle has dropped and whose close has not
		// yet uncounted it.
		let closing = Lease::open(&dynamic(&governor), 0, 0);
		let id = closing.id();
		governor.leases.insert(id, Arc::downgrade(&closing));
		drop(closing);
		governor.leases.insert(id, Weak::new());
		governor.granted.fetch_add(900, Ordering::AcqRel);
		let closer = {
			let governor = Arc::clone(&governor);
			std::thread::spawn(move || {
				std::thread::sleep(std::time::Duration::from_millis(50));
				governor.granted.fetch_sub(900, Ordering::AcqRel);
				governor.leases.remove(&id);
			})
		};
		assert_eq!(governor.admit(&asker, 500, true), Admit::Granted);
		closer.join().expect("the closing thread");
		assert_eq!((governor.overcommits(), governor.granted()), (0, asker.grant()));
	}

	/// Minimums past what the grants' word can sum are counted beside it, so a
	/// close uncounts only its own grant and leaves the flag and the grants in
	/// flight untouched: the governor goes on deciding.
	#[cfg(target_pointer_width = "64")]
	#[test]
	fn a_minimum_past_the_sums_bound_saturates() {
		let governor = Arc::new(FairGovernor::new(1000));
		let huge = Lease::open(&dynamic(&governor), narrow(SUM) + 1, 0);
		let other = Lease::open(&dynamic(&governor), narrow(SUM), 0);
		assert_eq!(governor.granted(), 2 * narrow(SUM) + 1);
		drop(huge);
		assert_eq!(governor.granted(), narrow(SUM), "a close lost the grants still open");
		let grower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(!governor.grant_free(&grower, 1), "a growth past the limit");
		drop((other, grower));
		assert_eq!(governor.granted.load(Ordering::Acquire), 0);
		assert_eq!(*governor.overflow.lock().unwrap(), 0);
		assert!(!governor.overflowing.load(Ordering::Acquire));
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		drop(governor.lock());
		assert!(lease.reserve(500));
		assert_eq!(governor.granted(), lease.grant());
	}

	/// A minimum of every byte a `usize` can name, beside a lease already
	/// open, is counted whole: closing it leaves the other lease's grant.
	#[cfg(target_pointer_width = "64")]
	#[test]
	fn a_largest_minimum_beside_another_lease_is_counted_whole() {
		let governor = Arc::new(FairGovernor::new(1000));
		let small = Lease::open(&dynamic(&governor), 100, 0);
		let huge = Lease::open(&dynamic(&governor), usize::MAX, 0);
		drop(huge);
		assert_eq!(governor.granted(), small.grant(), "a close took the other lease's grant");
		let grower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(governor.grant_free(&grower, 900 - grower.grant()));
		assert!(!governor.grant_free(&grower, 1), "a growth past the limit");
		drop((small, grower));
		assert_eq!(governor.granted(), 0);
	}

	/// A reclaim of a grant counted past the word's bound draws on what is kept
	/// beside it: once the leases close, nothing is counted.
	#[cfg(target_pointer_width = "64")]
	#[test]
	fn a_reclaim_past_the_bound_draws_on_the_overflow() {
		let governor = Arc::new(FairGovernor::new(1000));
		let first = Lease::open(&dynamic(&governor), 100, 0);
		first.set_grant(usize::MAX);
		let second = Lease::open(&dynamic(&governor), 100, 0);
		assert!(second.reserve(300));
		assert!(first.grant() <= 500, "the first lease was not lowered to its fair share");
		assert_eq!(governor.granted(), first.grant() + second.grant());
		drop((first, second));
		assert_eq!(governor.granted(), 0);
		assert!(!governor.overflowing.load(Ordering::Acquire));
	}

	/// A lock-free grant is refused while the count of grants in flight is
	/// full, so the count never carries into the flag.
	#[test]
	fn a_full_count_of_grants_in_flight_refuses_another() {
		let governor = Arc::new(FairGovernor::new(1000));
		let lease = Lease::open(&dynamic(&governor), 100, 0);
		governor.granted.fetch_add(IN_FLIGHTS, Ordering::AcqRel);
		let word = governor.granted.load(Ordering::Acquire);
		assert!(!governor.grant_free(&lease, 1), "a grant past a full count in flight");
		assert_eq!(governor.granted.load(Ordering::Acquire), word);
		assert_eq!(word & CLOSED, 0);
		governor.granted.fetch_sub(IN_FLIGHTS, Ordering::AcqRel);
		assert!(governor.grant_free(&lease, 1));
	}

	/// A growth past the limit is refused before it is counted as borrowed, so
	/// what is borrowed never reads as nothing while a lease borrows.
	#[test]
	fn a_growth_past_the_limit_is_never_counted_borrowed() {
		let governor = Arc::new(FairGovernor::new(1000));
		let borrower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(governor.grant_free(&borrower, 100));
		let grower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(!governor.grant_free(&grower, usize::MAX - 99));
		assert_eq!(governor.borrowed.load(Ordering::Acquire), 100);
		assert!(governor.open_free(1000).is_none(), "an open past the limit skipped the reclaim");
	}

	/// A grant set past what the grants' word can sum is held there, so what
	/// is borrowed stays exact: closing one lease leaves another's borrowing.
	#[cfg(target_pointer_width = "64")]
	#[test]
	fn a_regrant_is_held_within_the_sums_bound() {
		let governor = Arc::new(FairGovernor::new(1000));
		let huge = Lease::open(&dynamic(&governor), 0, 0);
		let small = Lease::open(&dynamic(&governor), 0, 0);
		huge.set_grant(usize::MAX);
		small.set_grant(1);
		assert_eq!(huge.grant(), narrow(SUM));
		drop(huge);
		assert_eq!(governor.borrowed.load(Ordering::Acquire), 1);
		assert!(governor.open_free(1000).is_none(), "an open past the limit skipped the reclaim");
	}

	/// Once what is borrowed has passed what the governor can count, no open
	/// past the limit trusts the count: each takes the lock and reclaims.
	#[test]
	fn a_lost_borrowed_count_sends_every_open_past_the_limit_to_the_lock() {
		let governor = Arc::new(FairGovernor::new(1000));
		let borrower = Lease::open(&dynamic(&governor), 0, 0);
		borrower.set_grant(1000);
		{
			let _deciding = governor.lock();
			governor.borrow(u64::MAX);
		}
		assert!(governor.borrowed_lost.load(Ordering::Acquire));
		// The count reads nothing borrowed although `borrower` borrows.
		governor.borrowed.store(0, Ordering::Release);
		assert!(governor.open_free(1000).is_none(), "an open past the limit trusted a lost count");
		let opened = Lease::open(&dynamic(&governor), 1000, 0);
		assert!(borrower.grant() < 1000, "the borrower was not lowered");
		drop((borrower, opened));
	}

	/// An open counted past the limit that then finds a lease borrowing lowers
	/// it to its fair share, as an open under the lock would have.
	#[test]
	fn an_open_past_the_limit_reclaims_a_borrower_it_finds_after() {
		let governor = Arc::new(FairGovernor::new(1000));
		let borrower = Lease::open(&dynamic(&governor), 100, 0);
		assert!(borrower.reserve(800));
		let opened = Lease::open(&dynamic(&governor), 100, 0);
		// As if `opened` had been counted lock-free on a stale reading.
		borrower.grant.store(900, Ordering::Release);
		governor.granted.store(1000, Ordering::Release);
		governor.borrowed.store(800, Ordering::Release);
		governor.reclaim_for(&opened);
		assert_eq!(borrower.grant(), 500, "the borrower was not lowered to its fair share");
		assert_eq!(governor.granted(), borrower.grant() + opened.grant());
		assert_eq!(governor.borrowed.load(Ordering::Acquire), 400);
	}

	/// While grants are counted past the word's bound no grant is made without
	/// the lock, and a locked decision weighs them against the limit: a close
	/// that lowered only the word leaves no room that is not there.
	#[test]
	fn grants_past_the_word_are_weighed_against_the_limit() {
		let governor = Arc::new(FairGovernor::new(narrow(SUM)));
		let lease = Lease::open(&dynamic(&governor), 0, 0);
		// The word held at its bound less 50, with 50 counted beside it.
		governor.granted.store(SUM - 50, Ordering::Release);
		*governor.overflow.lock().unwrap() = 50;
		governor.overflowing.store(true, Ordering::Release);
		assert!(!governor.grant_free(&lease, 50), "a lock-free grant past the limit");
		assert!(governor.open_free(50).is_none(), "a lock-free open past the limit");
		assert!(!governor.settle(0, 50, 0), "a locked grant past the limit");
		assert_eq!(governor.settle_upto(0, 50), 0);
	}

	/// What a reclaim lowers is summed wide, so a total past what a `usize`
	/// holds is drawn from the overflow whole.
	#[cfg(target_pointer_width = "64")]
	#[test]
	fn a_reclaim_past_usize_is_drawn_whole() {
		let governor = Arc::new(FairGovernor::new(1000));
		// 65,537 grants at the word's bound sum past what a `usize` holds.
		let count = 65_537;
		let leases: Vec<_> = (0..count).map(|_| Lease::open(&dynamic(&governor), 0, 0)).collect();
		for lease in &leases {
			lease.set_grant(narrow(SUM));
		}
		let lowered = count as u128 * u128::from(SUM);
		assert!(lowered > u128::from(u64::MAX));
		let deciding = governor.lock();
		let open = governor.open();
		let back = governor.reclaim(&open, u64::MAX, 0, Reclaim::Guaranteed);
		governor.settle(back, 0, 0);
		drop(deciding);
		drop(open);
		assert_eq!(back, lowered);
		assert_eq!(governor.granted(), 0);
		assert!(!governor.overflowing.load(Ordering::Acquire));
	}

	/// Tentative growths that together pass what the count of what is borrowed
	/// can hold lose the count for good rather than wrap it to nothing.
	#[test]
	fn tentative_growths_past_the_borrowed_bound_lose_the_count() {
		let governor = Arc::new(FairGovernor::new(1000));
		let borrower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(governor.grant_free(&borrower, 100));
		// What tentative growths in flight have counted, all but filling it.
		governor.borrowed.store(u64::MAX - 50, Ordering::Release);
		let grower = Lease::open(&dynamic(&governor), 0, 0);
		assert!(!governor.grant_free(&grower, 1000));
		assert!(governor.borrowed_lost.load(Ordering::Acquire));
		assert!(governor.open_free(1000).is_none(), "an open past the limit trusted a lost count");
	}

	/// Reservations on one lease whose grant another holder keeps lowering never
	/// ask the governor to grow it by a negative amount.
	#[test]
	fn reserving_while_the_grant_shrinks_does_not_underflow() {
		let governor = Arc::new(FairGovernor::new(64 * 1024));
		let lease = Lease::open(&dynamic(&governor), 0, 0);
		let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let shrinker = {
			let (lease, stop) = (Arc::clone(&lease), Arc::clone(&stop));
			std::thread::spawn(move || {
				while !stop.load(Ordering::Acquire) {
					lease.set_grant(0);
					lease.set_grant(4096);
				}
			})
		};
		let reservers: Vec<_> = (0..4)
			.map(|_| {
				let lease = Arc::clone(&lease);
				std::thread::spawn(move || {
					for _ in 0..20_000 {
						if lease.reserve(512) {
							lease.release(512);
						}
					}
				})
			})
			.collect();
		for thread in reservers {
			thread.join().expect("a reserving thread");
		}
		stop.store(true, Ordering::Release);
		shrinker.join().expect("the shrinking thread");
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

	/// With a hard limit, a request that would take the pinned bytes of all
	/// leases past it is refused and reserves nothing, whoever asks and however
	/// the request is made; what a lease gives back is available again.
	#[test]
	fn a_hard_limit_refuses_pinned_bytes_past_the_sum() {
		let governor = Arc::new(FairGovernor::new(10_000).with_hard_limit(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert_eq!(governor.admit(&a, 600, true), Admit::Granted);
		assert_eq!(governor.admit(&b, 401, true), Admit::Refuse);
		assert_eq!(governor.admit(&b, 401, false), Admit::Refuse);
		assert_eq!((b.held(), b.pinned(), b.grant()), (0, 0, 100));
		assert_eq!(governor.refusals(), 2);
		assert!(!b.waiting());
		assert_eq!(governor.admit(&b, 400, true), Admit::Granted);
		// The reservation of pinned bytes without a wait is bound by it too.
		assert!(!a.reserve_pinned(1));
		assert_eq!(a.pinned(), 600);
		a.release_pinned(100);
		assert!(a.reserve_pinned(100));
		assert!(!b.reserve_pinned(1));
		// Spillable bytes are not pinned bytes.
		assert!(a.reserve(2000));
		assert_eq!(governor.overcommits(), 0);
		check(&governor, &[&a, &b]);
		drop(a);
		assert_eq!(governor.admit(&b, 600, true), Admit::Granted);
	}

	/// Counting bytes a lease already holds as pinned is decided with the pinned
	/// bytes of all leases: the conversion that would pass the hard limit counts
	/// nothing and is refused, one that fits is counted, and a governor with no
	/// hard limit counts every conversion.
	#[test]
	fn pinning_held_bytes_is_decided_against_the_hard_limit() {
		let governor = Arc::new(FairGovernor::new(10_000).with_hard_limit(1000));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert!(a.reserve(900));
		assert!(a.pin(900));
		assert_eq!(a.pinned(), 900);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(b.reserve(200));
		assert!(!b.pin(200));
		assert_eq!((b.pinned(), b.held()), (0, 200));
		assert_eq!(governor.refusals(), 1);
		assert!(b.pin(100));
		let unbounded = Arc::new(FairGovernor::new(10_000));
		let c = Lease::open(&dynamic(&unbounded), 100, 0);
		assert!(c.reserve(5000));
		assert!(c.pin(5000));
	}

	/// The oldest waiting lease is refused like any other under a hard limit,
	/// and a refusal ends its wait.
	#[test]
	fn a_hard_limit_refuses_the_oldest_waiter_and_ends_its_wait() {
		let governor = Arc::new(FairGovernor::new(1000).with_hard_limit(1100));
		let a = Lease::open(&dynamic(&governor), 100, 0);
		let b = Lease::open(&dynamic(&governor), 100, 0);
		assert!(b.reserve_pinned(500));
		assert!(matches!(governor.admit(&a, 600, true), Admit::Wait { .. }));
		assert!(a.waiting());
		assert!(b.reserve_pinned(1));
		assert_eq!(governor.admit(&a, 600, true), Admit::Refuse);
		assert!(!a.waiting());
		assert_eq!(governor.waiters.load(Ordering::Relaxed), 0);
		assert_eq!(a.want.load(Ordering::Relaxed), 0);
	}

	/// With no hard limit the governor never refuses, whatever is pinned.
	#[test]
	fn no_hard_limit_never_refuses() {
		let governor = Arc::new(FairGovernor::new(1000));
		assert_eq!(governor.hard_limit(), None);
		let a = Lease::open(&dynamic(&governor), 100, 0);
		assert_eq!(governor.admit(&a, 5000, false), Admit::Overcommit);
		assert_eq!(governor.admit(&a, usize::MAX / 4, false), Admit::Overcommit);
		assert_eq!(governor.refusals(), 0);
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

	/// A governor that raises a lease's grant by what was asked, while another holder of the lease
	/// takes what it raised before the asker can: the first `raced` growths are used up so. One
	/// that `refuses` raises nothing.
	struct Raced {
		raced: usize,
		refuses: bool,
		asked: AtomicUsize,
	}

	impl Raced {
		fn new(raced: usize) -> Arc<Self> {
			Arc::new(Self {
				raced,
				refuses: false,
				asked: AtomicUsize::new(0),
			})
		}

		fn refusing() -> Arc<Self> {
			Arc::new(Self {
				raced: 0,
				refuses: true,
				asked: AtomicUsize::new(0),
			})
		}

		fn asked(&self) -> usize {
			self.asked.load(Ordering::Relaxed)
		}
	}

	impl MemoryGovernor for Raced {
		fn opened(&self, _lease: &Arc<Lease>) {}

		fn grow(&self, lease: &Lease, bytes: usize) -> bool {
			let asked = self.asked.fetch_add(1, Ordering::Relaxed);
			if self.refuses {
				return false;
			}
			lease.set_grant(lease.grant() + bytes);
			if asked < self.raced {
				assert!(lease.hold(bytes), "the other holder finds the raised grant");
			}
			true
		}

		fn closed(&self, _lease: &Lease) {}

		fn regrant(&self, lease: &Lease, bytes: usize) {
			lease.store_grant(bytes);
		}
	}

	/// A reservation whose raised grant another holder uses up asks the governor again for what is
	/// then short, and holds its bytes once a growth is not raced.
	#[test]
	fn a_grant_another_holder_used_up_is_asked_for_again() {
		let governor = Raced::new(3);
		let lease = Lease::open(&(Arc::clone(&governor) as Arc<dyn MemoryGovernor>), 100, 0);
		assert!(lease.reserve(500));
		assert_eq!(governor.asked(), 4, "three raced growths and the one that held");
		// The bytes the other holder took in three growths, and the bytes reserved, are held.
		assert_eq!(lease.held(), 3 * 400 + 500);
	}

	/// A governor that answers that it grew, however often another holder takes what it raised, is
	/// asked at most `MOST_GROWTHS` times, and the reservation is then refused.
	#[test]
	fn a_reservation_asks_a_raced_governor_a_bounded_number_of_times() {
		let governor = Raced::new(usize::MAX);
		let lease = Lease::open(&(Arc::clone(&governor) as Arc<dyn MemoryGovernor>), 100, 0);
		assert!(!lease.reserve(500));
		assert_eq!(governor.asked(), MOST_GROWTHS as usize);
	}

	/// A governor's refusal is the answer at once: it is not asked again.
	#[test]
	fn a_refusal_is_not_asked_again() {
		let governor = Raced::refusing();
		let lease = Lease::open(&(Arc::clone(&governor) as Arc<dyn MemoryGovernor>), 100, 0);
		assert!(!lease.reserve(500));
		assert_eq!(governor.asked(), 1);
		assert_eq!(lease.held(), 0);
	}

	/// Workers of one query reserve against its one lease at once. While the node has free memory
	/// no reservation is refused: a refusal tells the query to spill, and a grant that another
	/// worker used up before the asker held its bytes is not one.
	#[test]
	fn concurrent_reservations_on_one_lease_are_not_refused_while_the_node_has_memory() {
		let governor = Arc::new(FairGovernor::new(1 << 32));
		let lease = Lease::open(&dynamic(&governor), 0, 0);
		let workers: Vec<_> = (0..8)
			.map(|_| {
				let lease = Arc::clone(&lease);
				std::thread::spawn(move || (0..20_000).filter(|_| !lease.reserve(512)).count())
			})
			.collect();
		let refused: usize = workers.into_iter().map(|w| w.join().expect("a worker")).sum();
		assert_eq!(refused, 0, "a reservation was refused with memory free on the node");
		assert_eq!(lease.held(), 8 * 20_000 * 512);
	}

	/// A governor of one lease that delegates to a [`FairGovernor`] and, for the first `raced`
	/// growths, has another holder of the asking lease take the bytes the growth raised.
	struct Racing {
		inner: Arc<FairGovernor>,
		raced: usize,
		asked: AtomicUsize,
	}

	impl MemoryGovernor for Racing {
		fn opened(&self, lease: &Arc<Lease>) {
			self.inner.opened(lease);
		}

		fn grow(&self, lease: &Lease, bytes: usize) -> bool {
			let asked = self.asked.fetch_add(1, Ordering::Relaxed);
			let grown = self.inner.grow(lease, bytes);
			if grown && asked < self.raced {
				assert!(lease.hold(bytes), "the other holder finds the raised grant");
			}
			grown
		}

		fn closed(&self, lease: &Lease) {
			self.inner.closed(lease);
		}

		fn regrant(&self, lease: &Lease, bytes: usize) {
			self.inner.regrant(lease, bytes);
		}
	}

	/// What another lease holds and is granted after the lease it shares a governor with reserves
	/// `bytes`, the first `raced` of that lease's growths used up by another of its holders.
	fn neighbour_after(raced: usize) -> (usize, usize, usize, usize) {
		let inner = Arc::new(FairGovernor::new(1000));
		let neighbour = Lease::open(&dynamic(&inner), 0, 0);
		assert!(neighbour.reserve(900));
		let racing = Arc::new(Racing {
			inner: Arc::clone(&inner),
			raced,
			asked: AtomicUsize::new(0),
		});
		let lease = Lease::open(&(Arc::clone(&racing) as Arc<dyn MemoryGovernor>), 100, 0);
		assert!(lease.reserve(150), "the lease holds its bytes");
		check(&inner, &[&neighbour, &lease]);
		(neighbour.grant(), neighbour.held(), inner.granted(), racing.asked.load(Ordering::Relaxed))
	}

	/// Asking again for a grant another holder used up lowers no other lease further than the
	/// first ask does: the neighbour that borrowed past its fair share is brought back to it once,
	/// holds what it held, and the node's grants are what they are after one clean ask plus what
	/// the raced growths added to the asking lease alone.
	#[test]
	fn asking_again_leaves_the_leases_beside_it_as_one_ask_does() {
		let (grant, held, granted, asked) = neighbour_after(0);
		assert_eq!(asked, 1);
		assert_eq!(grant, 500, "brought back to its fair share");
		assert_eq!(held, 900, "its bytes untouched");
		let (raced_grant, raced_held, raced_granted, raced_asked) = neighbour_after(3);
		assert_eq!(raced_asked, 4);
		assert_eq!(raced_grant, grant, "no further lowered");
		assert_eq!(raced_held, held);
		// The lease's own grant rose by the three growths another holder used.
		assert!(raced_granted > granted && raced_granted <= 1000);
	}
}
