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
	use std::collections::{BTreeMap, BTreeSet};
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
	///
	/// The statements marked waiting are also listed by the places each holds, so that what a
	/// statement is owed, and whether another waits, are answered without visiting every
	/// statement: a node under load registers a statement per query, and each asks at every
	/// chunk of its scan.
	#[derive(Debug)]
	struct Books {
		free: usize,
		next: u64,
		statements: BTreeMap<u64, Entry>,
		/// The ids of the waiting statements, by the places they hold. No set is empty.
		waiting: BTreeMap<usize, BTreeSet<u64>>,
	}

	impl Books {
		/// Lists waiting statement `id`, which holds `held` places.
		fn list(&mut self, id: u64, held: usize) {
			self.waiting.entry(held).or_default().insert(id);
		}

		/// Takes statement `id`, which holds `held` places, off the waiting list.
		fn unlist(&mut self, id: u64, held: usize) {
			if let Some(ids) = self.waiting.get_mut(&held) {
				ids.remove(&id);
				if ids.is_empty() {
					self.waiting.remove(&held);
				}
			}
		}

		/// The waiting statements other than `me` that a free place is owed to before `me`, which
		/// holds `held`: those that hold fewer, and those that hold as many and came first.
		/// Counted no further than `most`, which is all an answer is compared with.
		fn owed_before(&self, me: u64, held: usize, most: usize) -> usize {
			let mut owed = 0;
			for ids in self.waiting.range(..held).map(|(_, ids)| ids) {
				owed += ids.len();
				if owed >= most {
					return owed;
				}
			}
			if let Some(ids) = self.waiting.get(&held) {
				owed += ids.range(..me).take(most - owed).count();
			}
			owed
		}
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
					waiting: BTreeMap::new(),
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
			let mut books = self.pool.books();
			if let Some(entry) = books.statements.remove(&self.id)
				&& entry.waiting
			{
				books.unlist(self.id, entry.held);
			}
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
			let id = self.0.id;
			if let Some(entry) = books.statements.get_mut(&id) {
				let (held, waiting) = (entry.held, entry.waiting);
				entry.held = held.saturating_sub(1);
				if waiting {
					books.unlist(id, held);
					books.list(id, held.saturating_sub(1));
				}
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
			// A statement is registered for as long as it can ask.
			let &Entry {
				held,
				waiting,
			} = books.statements.get(&me)?;
			if held >= policy.place_cap() {
				return None;
			}
			let usable = books.free.saturating_sub(policy.oltp_reserve());
			if usable <= books.owed_before(me, held, usable) {
				if !waiting {
					books.list(me, held);
					books.statements.insert(
						me,
						Entry {
							held,
							waiting: true,
						},
					);
				}
				return None;
			}
			if waiting {
				books.unlist(me, held);
			}
			let held = held + 1;
			let waiting = keep_waiting && held < policy.place_cap();
			if waiting {
				books.list(me, held);
			}
			books.statements.insert(
				me,
				Entry {
					held,
					waiting,
				},
			);
			books.free -= 1;
			Some(Place(Arc::clone(&self.0)))
		}

		/// Clears this statement's waiting mark.
		pub(crate) fn stop_waiting(&self) {
			let mut books = self.0.pool.books();
			let id = self.0.id;
			if let Some(entry) = books.statements.get_mut(&id)
				&& entry.waiting
			{
				entry.waiting = false;
				let held = entry.held;
				books.unlist(id, held);
			}
		}

		/// Whether a statement other than this one waits for a place, and holds no more than this
		/// one does.
		pub(crate) fn others_wait(&self) -> bool {
			let books = self.0.pool.books();
			let me = self.0.id;
			let held = books.statements.get(&me).map_or(0, |entry| entry.held);
			books.waiting.range(..=held).any(|(_, ids)| ids.iter().any(|&id| id != me))
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

	#[cfg(test)]
	mod tests {
		use std::collections::BTreeMap;

		use super::{Place, Policy, Pool, Statement};
		use crate::exec::config::ExecConfig;

		/// The rules the books keep, over every statement: a free place is owed to the waiting
		/// statement that holds the fewest, then to the one that came first.
		#[derive(Default)]
		struct Model {
			free: usize,
			/// `(held, waiting)` by statement id, which is the order of registration.
			statements: BTreeMap<usize, (usize, bool)>,
		}

		impl Model {
			fn acquire(&mut self, me: usize, cap: usize, reserve: usize, keep: bool) -> bool {
				let held = self.statements[&me].0;
				if held >= cap {
					return false;
				}
				let owed = self
					.statements
					.iter()
					.filter(|(id, (h, w))| {
						**id != me && *w && (*h < held || (*h == held && **id < me))
					})
					.count();
				let entry = self.statements.get_mut(&me).unwrap();
				if self.free.saturating_sub(reserve) <= owed {
					entry.1 = true;
					return false;
				}
				entry.0 += 1;
				entry.1 = keep && entry.0 < cap;
				self.free -= 1;
				true
			}

			fn others_wait(&self, me: usize) -> bool {
				let held = self.statements[&me].0;
				self.statements.iter().any(|(id, (h, w))| *id != me && *w && *h <= held)
			}
		}

		/// Every grant, refusal and answer of `others_wait` is the one the rules give, over
		/// random runs of registrations, grants, returned places and ended statements.
		#[test]
		fn the_books_answer_as_the_rules_over_every_statement_do() {
			let mut seed = 0x2545_f491_4f6c_dd1du64;
			let mut next = |n: usize| {
				seed ^= seed << 13;
				seed ^= seed >> 7;
				seed ^= seed << 17;
				(seed % n as u64) as usize
			};
			for round in 0..200 {
				let places = 1 + next(12);
				let config = ExecConfig {
					morsel_places: 1 + next(6),
					morsel_oltp_reserve: next(3),
					..ExecConfig::default()
				};
				let policy = Policy::from_config(&config);
				let (cap, reserve) = (policy.place_cap(), policy.oltp_reserve());
				let pool = Pool::new(places);
				let mut model = Model {
					free: places,
					..Model::default()
				};
				let mut statements: BTreeMap<usize, Statement> = BTreeMap::new();
				let mut held: Vec<(usize, Place)> = Vec::new();
				let mut ids = 0;
				for step in 0..400 {
					let what = format!("round {round}, step {step}");
					match next(10) {
						0 | 1 => {
							statements.insert(ids, pool.register());
							model.statements.insert(ids, (0, false));
							ids += 1;
						}
						2..=5 if !statements.is_empty() => {
							let id = *statements.keys().nth(next(statements.len())).unwrap();
							let keep = next(4) == 0;
							let granted = statements[&id].acquire_keeping_waiting(policy, keep);
							assert_eq!(
								granted.is_some(),
								model.acquire(id, cap, reserve, keep),
								"{what}: acquire"
							);
							if let Some(place) = granted {
								held.push((id, place));
							}
						}
						6 if !held.is_empty() => {
							let (id, place) = held.swap_remove(next(held.len()));
							drop(place);
							model.free += 1;
							if let Some(entry) = model.statements.get_mut(&id) {
								entry.0 -= 1;
							}
						}
						7 if !statements.is_empty() => {
							let id = *statements.keys().nth(next(statements.len())).unwrap();
							statements[&id].stop_waiting();
							model.statements.get_mut(&id).unwrap().1 = false;
						}
						8 if !statements.is_empty() => {
							let id = *statements.keys().nth(next(statements.len())).unwrap();
							assert_eq!(
								statements[&id].others_wait(),
								model.others_wait(id),
								"{what}: others_wait"
							);
						}
						9 if !statements.is_empty() => {
							// A statement ends; its registration goes with its last place.
							let id = *statements.keys().nth(next(statements.len())).unwrap();
							statements.remove(&id);
							held.retain(|(of, _)| *of != id);
							model.free += model.statements.remove(&id).unwrap().0;
						}
						_ => {}
					}
					for (id, statement) in &statements {
						let (h, w) = model.statements[id];
						assert_eq!((statement.held(), statement.waiting()), (h, w), "{what}: {id}");
					}
					assert_eq!(pool.free(), model.free, "{what}: free");
				}
				drop(held);
				drop(statements);
				assert_eq!(pool.registrations(), 0);
				assert!(pool.books().waiting.is_empty(), "round {round}: a waiting list is left");
			}
		}
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
