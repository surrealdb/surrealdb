//! The node's places morsel workers run in, and who gets one when it is free.
//!
//! One place per core beyond the one a statement's own task runs on, shared by every statement
//! of the process. A worker holds a place from when it starts until it ends, so a statement widens
//! while the node has idle cores and runs on its own task while other statements hold them. How
//! many places a statement may hold, and how many the pool keeps free for other work, are the
//! [`Policy`]'s to say; this module keeps the count and the order in which statements are served.
//!
//! Every statement that runs morsels registers with the pool ([`Statement`]) and asks it for
//! places. A statement that asks while no place is free, or while a place is owed to another, is
//! marked waiting until it takes one or stops asking. A free place is owed to the waiting statement
//! that holds the fewest, the one that has waited longest among equals, so a statement that holds
//! many yields to one that holds none, and a place is never handed over by a callback: the
//! statement it is owed to takes it the next time it asks, which it does at every merge step.

use super::super::interp::State;
use crate::exec::planner::placement::Policy;

#[cfg(not(target_family = "wasm"))]
mod pool {
	use std::collections::BTreeMap;
	use std::sync::{Arc, Mutex};

	use super::Policy;

	/// What the pool knows of one registered statement.
	#[derive(Debug, Default)]
	struct Entry {
		/// The places the statement holds.
		held: usize,
		/// Whether the statement asked for a place it was not given, and has not stopped asking.
		waiting: bool,
	}

	/// The pool's books: the places free and the statements registered, by arrival.
	#[derive(Debug)]
	struct Books {
		free: usize,
		next: u64,
		statements: BTreeMap<u64, Entry>,
	}

	/// A pool of places and the statements served from it.
	#[derive(Debug)]
	pub(crate) struct Pool {
		books: Mutex<Books>,
	}

	impl Pool {
		/// A pool of `places` places.
		pub(crate) fn new(places: usize) -> Arc<Self> {
			Arc::new(Self {
				books: Mutex::new(Books {
					free: places,
					next: 0,
					statements: BTreeMap::new(),
				}),
			})
		}

		fn books(&self) -> std::sync::MutexGuard<'_, Books> {
			self.books.lock().unwrap_or_else(|e| e.into_inner())
		}

		/// Register a statement.
		pub(crate) fn register(self: &Arc<Self>) -> Statement {
			let mut books = self.books();
			let id = books.next;
			books.next += 1;
			books.statements.insert(id, Entry::default());
			Statement(Arc::new(Registration {
				pool: Arc::clone(self),
				id,
			}))
		}
	}

	/// A statement's registration, which ends when the statement and every place it holds are
	/// gone.
	#[derive(Debug)]
	struct Registration {
		pool: Arc<Pool>,
		id: u64,
	}

	impl Drop for Registration {
		fn drop(&mut self) {
			self.pool.books().statements.remove(&self.id);
		}
	}

	/// A statement registered with a pool.
	#[derive(Clone, Debug)]
	pub(crate) struct Statement(Arc<Registration>);

	/// A place in the pool, held by a worker until it ends.
	#[derive(Debug)]
	pub(crate) struct Place(Arc<Registration>);

	impl Drop for Place {
		fn drop(&mut self) {
			let mut books = self.0.pool.books();
			books.free += 1;
			if let Some(entry) = books.statements.get_mut(&self.0.id) {
				entry.held = entry.held.saturating_sub(1);
			}
		}
	}

	impl Statement {
		/// Takes a place for this statement, or returns `None`. At its cap under `policy` the
		/// statement gets `None` at once and is not marked waiting. Otherwise a place is granted
		/// only when the free places beyond the ones `policy` keeps back outnumber the waiting
		/// statements owed a place before this one. If not, the statement is marked waiting.
		pub(crate) fn acquire(&self, policy: Policy) -> Option<Place> {
			self.acquire_keeping_waiting(policy, false)
		}

		/// [`Self::acquire`], except that a grant leaves the statement marked waiting when
		/// `keep_waiting` asks it to and it is still below its cap, under the same lock as the
		/// grant, so no other statement sees it unmarked in between. The morsel runs never keep
		/// waiting; the index engines do, for a registration one of whose users still waits
		/// after another was granted a place.
		pub(crate) fn acquire_keeping_waiting(
			&self,
			policy: Policy,
			keep_waiting: bool,
		) -> Option<Place> {
			let mut books = self.0.pool.books();
			let me = self.0.id;
			let held = books.statements.get(&me).map_or(0, |entry| entry.held);
			if held >= policy.place_cap() {
				return None;
			}
			let owed_before = books
				.statements
				.iter()
				.filter(|(id, entry)| {
					**id != me
						&& entry.waiting && (entry.held < held || (entry.held == held && **id < me))
				})
				.count();
			let usable = books.free.saturating_sub(policy.oltp_reserve());
			let entry = books.statements.entry(me).or_default();
			if usable <= owed_before {
				entry.waiting = true;
				return None;
			}
			entry.held += 1;
			entry.waiting = keep_waiting && entry.held < policy.place_cap();
			books.free -= 1;
			Some(Place(Arc::clone(&self.0)))
		}

		/// Clears this statement's waiting mark.
		pub(crate) fn stop_waiting(&self) {
			if let Some(entry) = self.0.pool.books().statements.get_mut(&self.0.id) {
				entry.waiting = false;
			}
		}

		/// Whether a statement other than this one waits for a place, and holds no more than this
		/// one does.
		pub(crate) fn others_wait(&self) -> bool {
			let books = self.0.pool.books();
			let held = books.statements.get(&self.0.id).map_or(0, |entry| entry.held);
			books
				.statements
				.iter()
				.any(|(id, entry)| *id != self.0.id && entry.waiting && entry.held <= held)
		}

		/// The places this statement holds.
		#[cfg(test)]
		pub(crate) fn held(&self) -> usize {
			self.0.pool.books().statements.get(&self.0.id).map_or(0, |entry| entry.held)
		}

		/// Whether this statement is marked waiting.
		#[cfg(test)]
		pub(crate) fn waiting(&self) -> bool {
			self.0.pool.books().statements.get(&self.0.id).is_some_and(|entry| entry.waiting)
		}
	}

	#[cfg(test)]
	impl Pool {
		/// The places no statement holds.
		pub(crate) fn free(&self) -> usize {
			self.books().free
		}

		/// The statements registered.
		pub(crate) fn registrations(&self) -> usize {
			self.books().statements.len()
		}
	}

	/// The node's pool: one place per core beyond the one a statement's task runs on.
	pub(in crate::exec::vm::morsel) fn node() -> &'static Arc<Pool> {
		static NODE: std::sync::LazyLock<Arc<Pool>> = std::sync::LazyLock::new(|| {
			let cores = std::thread::available_parallelism().map_or(1, |cores| cores.get());
			Pool::new(cores.saturating_sub(1))
		});
		&NODE
	}
}

#[cfg(not(target_family = "wasm"))]
pub(crate) use pool::Place;
#[cfg(all(test, not(target_family = "wasm")))]
pub(crate) use pool::Pool;
#[cfg(not(target_family = "wasm"))]
pub(crate) use pool::Statement;

/// A place in the pool; targets without worker threads never hold one.
#[cfg(target_family = "wasm")]
pub(crate) type Place = std::convert::Infallible;

/// A statement registered with the node's pool; targets without worker threads grant it none.
#[cfg(target_family = "wasm")]
#[derive(Clone, Debug)]
pub(crate) struct Statement;

#[cfg(target_family = "wasm")]
impl Statement {
	pub(crate) fn acquire(&self, _policy: Policy) -> Option<Place> {
		None
	}

	pub(crate) fn acquire_keeping_waiting(
		&self,
		_policy: Policy,
		_keep_waiting: bool,
	) -> Option<Place> {
		None
	}

	pub(crate) fn stop_waiting(&self) {}

	pub(crate) fn others_wait(&self) -> bool {
		false
	}
}

/// Register a statement with the node's pool.
#[cfg(not(target_family = "wasm"))]
pub(crate) fn register() -> Statement {
	pool::node().register()
}

#[cfg(target_family = "wasm")]
pub(crate) fn register() -> Statement {
	Statement
}

/// Holds a run's registration and clears its waiting mark when dropped. A request refused for the
/// last time therefore stops counting against other statements once the run ends.
#[derive(Debug, Default)]
pub(super) struct Asking(Option<Statement>);

impl Asking {
	/// Records the registration `state` holds, once it holds one. The first record is kept.
	pub(super) fn note(&mut self, state: &State) {
		if self.0.is_none() {
			self.0.clone_from(&state.places);
		}
	}
}

impl Drop for Asking {
	fn drop(&mut self) {
		if let Some(statement) = &self.0 {
			statement.stop_waiting();
		}
	}
}

/// The registration of the run `state` belongs to, created on the first request. Every request in
/// the run draws its places through it, so the run holds at most the policy's cap and the pool
/// serves the run as one statement.
pub(in crate::exec::vm) fn of(state: &mut State) -> &Statement {
	state.places.get_or_insert_with(register)
}
