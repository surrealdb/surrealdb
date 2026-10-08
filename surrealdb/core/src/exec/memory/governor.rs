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
//! read buffers a merge holds. A query can therefore always spill, and fails
//! for want of memory only when it holds no more than its minimum and has
//! nothing left to spill.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

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
}

static NEXT_LEASE: AtomicU64 = AtomicU64::new(1);

/// What one query may hold, and what it holds, under its node's governor.
pub struct Lease {
	id: u64,
	minimum: usize,
	headroom: usize,
	grant: AtomicUsize,
	held: AtomicUsize,
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
			governor: Arc::clone(governor),
		});
		governor.opened(&lease);
		lease
	}

	/// The lease's identity, unique in the process.
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

	/// Set the grant to `bytes`, or to the minimum when `bytes` is below it.
	/// A grant lowered below what the query holds is observed at its next
	/// flush, where its breakers spill.
	pub fn set_grant(&self, bytes: usize) {
		self.grant.store(bytes.max(self.minimum), Ordering::Release);
	}

	/// What operators may hold in all: the grant less the headroom.
	fn usable(&self) -> usize {
		self.grant().saturating_sub(self.headroom)
	}

	/// Whether the query's operators hold more than its grant allows, as a
	/// lowered grant leaves them.
	pub fn over(&self) -> bool {
		self.held() > self.usable()
	}

	/// Whether the grant stands at the minimum, which no governor lowers.
	pub fn at_minimum(&self) -> bool {
		self.grant() <= self.minimum
	}

	/// Reserve `bytes` more, asking the governor to grow the grant when it
	/// does not cover them. Answers whether the bytes are reserved.
	pub fn reserve(&self, bytes: usize) -> bool {
		let mut asked = false;
		loop {
			let held = self.held();
			let want = held.saturating_add(bytes);
			if want <= self.usable() {
				if self
					.held
					.compare_exchange_weak(held, want, Ordering::AcqRel, Ordering::Acquire)
					.is_ok()
				{
					return true;
				}
				continue;
			}
			if asked || !self.governor.grow(self, want - self.usable()) {
				return false;
			}
			asked = true;
		}
	}

	/// Give back `bytes` reserved earlier.
	pub fn release(&self, bytes: usize) {
		let mut held = self.held();
		loop {
			let next = held.saturating_sub(bytes);
			match self.held.compare_exchange_weak(held, next, Ordering::AcqRel, Ordering::Acquire) {
				Ok(_) => return,
				Err(now) => held = now,
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

/// A governor holding the node to `limit` bytes, shared fairly.
///
/// A lease grows from the node's free memory while there is any, so a query
/// alone on an idle node may borrow the whole limit. When a lease asks for
/// more than is free, every other lease above its fair share — the limit
/// divided among the open leases, and never below a lease's minimum — is
/// lowered to it, and the asker is granted up to its own fair share. Memory a
/// closing lease held is free for the next to ask. The grants' sum stays
/// within the limit, except where the minimums alone exceed it: a minimum is
/// guaranteed, so admission never waits.
///
/// The memory a lowered lease holds is given back when it next flushes, so
/// what the node holds may pass the limit by what its queries hold in one
/// batch.
pub struct FairGovernor {
	limit: usize,
	leases: Mutex<Vec<Weak<Lease>>>,
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
}

impl FairGovernor {
	/// Grow `lease` by `bytes` among the `open` leases, lowering those above
	/// their fair share when the node has too little free.
	fn decide(&self, open: &[Arc<Lease>], lease: &Lease, bytes: usize) -> bool {
		let mut granted: usize = open.iter().map(|other| other.grant()).sum();
		let free = self.limit.saturating_sub(granted);
		if free >= bytes {
			lease.set_grant(lease.grant() + bytes);
			return true;
		}
		let fair = self.limit / open.len().max(1);
		granted = Self::reclaim(open, lease.id, fair, granted);
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
	/// Lower every lease of `open` but `except` that is above `fair` to it,
	/// or to its minimum where that is higher, answering the grants' sum.
	fn reclaim(open: &[Arc<Lease>], except: u64, fair: usize, mut granted: usize) -> usize {
		for other in open {
			if other.id == except {
				continue;
			}
			let floor = fair.max(other.minimum);
			let grant = other.grant();
			if grant > floor {
				other.set_grant(floor);
				granted -= grant - floor;
			}
		}
		granted
	}
}

impl MemoryGovernor for FairGovernor {
	fn opened(&self, lease: &Arc<Lease>) {
		let mut leases = self.leases.lock().unwrap_or_else(|e| e.into_inner());
		leases.retain(|weak| weak.strong_count() > 0);
		leases.push(Arc::downgrade(lease));
		// A minimum is granted at once: leases borrowing past their fair
		// share make room for it.
		let open: Vec<Arc<Lease>> = leases.iter().filter_map(Weak::upgrade).collect();
		let granted: usize = open.iter().map(|other| other.grant()).sum();
		if granted > self.limit {
			let fair = self.limit / open.len().max(1);
			Self::reclaim(&open, lease.id, fair, granted);
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
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn dynamic(governor: &Arc<FairGovernor>) -> Arc<dyn MemoryGovernor> {
		Arc::clone(governor) as Arc<dyn MemoryGovernor>
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
}
