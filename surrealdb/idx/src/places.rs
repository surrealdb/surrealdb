//! The node's worker places, as the index engines take them.
//!
//! An index engine's parallel work runs in places from the node's pool, the
//! pool the query engine's morsel workers run in, under the same OLTP reserve
//! and the same fairness. The engines never name that pool: they take places
//! through a [`Ticket`], which the environment hands them
//! ([`crate::env::IndexEnv::places`]). Behind a ticket is one registration with
//! the pool: a blocking statement's own, the background compactor's, or a
//! build's. Every place a worker takes through the ticket is counted against
//! that registration.
//!
//! There are two ways to take places, because work divides in two ways:
//!
//! * **One at a time** ([`Ticket::acquire`]). Work that is a counter of independent jobs takes a
//!   place per worker as it starts one, and the worker gives its place way at a quantum boundary
//!   when another registration waits ([`Ticket::others_wait`], [`Ticket::give_way`]). A refused ask
//!   marks the ticket waiting, so a place freed later is owed to it; the asker clears the mark
//!   ([`Ticket::stop_waiting`]) once it stops asking.
//! * **Up front** ([`Ticket::acquire_places`]). Work that is split across a fixed number of tasks
//!   before it starts learns how many places it holds first, and sizes its split to that. The grant
//!   never waits and leaves no waiting mark behind.
//!
//! A ticket that is never granted a place is a valid ticket: everything then
//! runs on the calling task. Targets without worker threads hand out only such
//! tickets ([`Ticket::serial`]).

use std::fmt;
use std::sync::Arc;

/// A place in the node's pool. The place goes back to the pool when this is
/// dropped.
pub struct Place(#[allow(dead_code, reason = "held for its drop")] Box<dyn Send>);

impl Place {
	/// A place whose return to the pool is `guard`'s drop.
	pub fn new(guard: impl Send + 'static) -> Self {
		Self(Box::new(guard))
	}
}

impl fmt::Debug for Place {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Place")
	}
}

/// One registration with the node's pool, as the pool implements it.
///
/// Every method answers at once: none waits for a place to come free.
pub trait PlaceSource: Send + Sync {
	/// Takes one place for this registration, or returns `None`.
	///
	/// A registration at its cap gets `None` and is not marked waiting. Any
	/// other refusal marks it waiting, so a place freed later is owed to it,
	/// until it takes one or [`Self::stop_waiting`] clears the mark. A grant
	/// leaves the registration marked waiting when `keep_waiting` asks it to
	/// and it is still below its cap, and clears the mark otherwise, in the
	/// same step as the grant: no other registration ever sees it between the
	/// two.
	fn acquire(&self, keep_waiting: bool) -> Option<Place>;

	/// Whether another registration waits for a place and holds no more than
	/// this one does.
	fn others_wait(&self) -> bool;

	/// Clears this registration's waiting mark.
	fn stop_waiting(&self);

	/// The jobs a worker runs on a place between two checks of
	/// [`Self::others_wait`].
	fn quantum(&self) -> u32;
}

/// A source with no places: every ask is refused, and nothing waits.
struct NoPlaces;

impl PlaceSource for NoPlaces {
	fn acquire(&self, _keep_waiting: bool) -> Option<Place> {
		None
	}

	fn others_wait(&self) -> bool {
		false
	}

	fn stop_waiting(&self) {}

	fn quantum(&self) -> u32 {
		1
	}
}

/// The handle an index engine takes places through. Cloning it shares the
/// registration, and with it whether the registration has given a place way
/// ([`Self::give_way`]) and which of its users wait ([`Asking`]).
#[derive(Clone)]
pub struct Ticket(Arc<TicketInner>);

struct TicketInner {
	source: Box<dyn PlaceSource>,
	/// Held across every ask, every giving way and every change to who waits,
	/// so an ask never takes the place being given way, and the pool's waiting
	/// mark always follows the users that wait.
	state: parking_lot::Mutex<TicketState>,
}

/// What a ticket knows of its registration beyond the pool's books.
#[derive(Default)]
struct TicketState {
	/// Whether a place of this registration was given way to a waiting
	/// registration, and no ask has found none waiting since.
	gave_way: bool,
	/// The users of the ticket ([`Asking`]) refused a place and still asking.
	/// The pool's waiting mark for the registration is kept while this is
	/// above zero.
	waiting: usize,
	/// Advanced whenever `waiting` is reset, so a user counted before the
	/// reset is not taken off the count again.
	epoch: u64,
}

impl fmt::Debug for Ticket {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Ticket")
	}
}

impl Ticket {
	/// A ticket over `source`.
	pub fn new(source: impl PlaceSource + 'static) -> Self {
		Self(Arc::new(TicketInner {
			source: Box::new(source),
			state: parking_lot::Mutex::new(TicketState::default()),
		}))
	}

	/// A ticket that is never granted a place, so its work runs on the
	/// calling task.
	pub fn serial() -> Self {
		Self::new(NoPlaces)
	}

	/// Whether `self` and `other` are the same registration.
	pub fn same(&self, other: &Self) -> bool {
		Arc::ptr_eq(&self.0, &other.0)
	}

	/// Takes one place, or returns `None` at once; see
	/// [`PlaceSource::acquire`] for the waiting mark a refusal leaves. An ask
	/// made through an [`Asking`] user is counted as that user's.
	///
	/// Once the registration has given a place way ([`Self::give_way`]), it
	/// does not ask while another registration waits: the answer is `None`,
	/// and no waiting mark is left. So it never takes back the place it gave
	/// way, though it may have registered first and win the pool's tie. This
	/// holds for every holder of the ticket, whichever of them gave way: a
	/// statement's ticket lent to its build defers on both sides, since the
	/// pool serves the registration, not its holders. An ask that finds none
	/// waiting asks as usual, and the registration has no place given way any
	/// more.
	///
	/// While a user of the ticket waits, a grant leaves the registration
	/// marked waiting, in the same step as the grant.
	pub fn acquire(&self) -> Option<Place> {
		let mut state = self.0.state.lock();
		let keep_waiting = state.waiting > 0;
		self.ask(&mut state, keep_waiting)
	}

	/// Asks the pool for a place, unless the registration defers to a waiting
	/// one; `None` either way. A grant leaves the registration marked waiting
	/// when `keep_waiting` asks it to ([`PlaceSource::acquire`]).
	fn ask(&self, state: &mut TicketState, keep_waiting: bool) -> Option<Place> {
		if state.gave_way {
			if self.0.source.others_wait() {
				return None;
			}
			state.gave_way = false;
		}
		self.0.source.acquire(keep_waiting)
	}

	/// Gives `place` way to a waiting registration: records that the
	/// registration gave way, clears its waiting mark, and returns the place
	/// to the pool, all before any ask of the registration's can take it. No
	/// user of the ticket waits after: each defers instead, until an ask finds
	/// no other registration waiting.
	pub fn give_way(&self, place: Place) {
		let mut state = self.0.state.lock();
		state.gave_way = true;
		state.waiting = 0;
		state.epoch += 1;
		self.0.source.stop_waiting();
		drop(place);
	}

	/// Takes up to `max` places at once, without waiting.
	///
	/// Each place is taken as [`Self::acquire`] takes one, under the same
	/// reserve, cap and fairness, until `max` are held or one is refused. The
	/// grant then stops asking: the caller sizes its work from what it holds
	/// and does not ask again until its next unit of work, and the refusal
	/// leaves no waiting mark unless another user of the ticket waits. A grant
	/// holds its places until it is dropped, and covers one bounded unit of
	/// work, so a waiting registration is served at the next unit's ask.
	pub fn acquire_places(&self, max: usize) -> PlaceGrant {
		let asking = self.asking();
		let mut places = Vec::new();
		while places.len() < max {
			match asking.acquire() {
				Some(place) => places.push(place),
				None => break,
			}
		}
		PlaceGrant {
			places,
		}
	}

	/// Whether another registration waits for a place and holds no more than
	/// this one does.
	pub fn others_wait(&self) -> bool {
		self.0.source.others_wait()
	}

	/// Clears this ticket's waiting mark, unless a user of the ticket waits
	/// ([`Asking`]).
	pub fn stop_waiting(&self) {
		let state = self.0.state.lock();
		if state.waiting == 0 {
			self.0.source.stop_waiting();
		}
	}

	/// The jobs a worker runs on a place between two checks of
	/// [`Self::others_wait`]; at least one.
	pub fn quantum(&self) -> usize {
		self.0.source.quantum().max(1) as usize
	}

	/// A user of the ticket: work that asks for places one at a time for as
	/// long as it may ask. A refused ask through it counts it as waiting until
	/// it is granted a place, stops ([`Asking::stop_waiting`]) or is dropped.
	/// The pool's waiting mark for the registration is kept exactly while a
	/// user waits, so one user stopping does not clear the mark another still
	/// waits under.
	pub fn asking(&self) -> Asking {
		Asking {
			ticket: self.clone(),
			counted: parking_lot::Mutex::new(None),
		}
	}
}

/// A user of a ticket; see [`Ticket::asking`]. It stops waiting when dropped.
pub struct Asking {
	ticket: Ticket,
	/// The ticket's epoch when this user was counted as waiting, while it is.
	counted: parking_lot::Mutex<Option<u64>>,
}

impl fmt::Debug for Asking {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.write_str("Asking")
	}
}

impl Asking {
	/// The ticket this user asks through.
	pub fn ticket(&self) -> &Ticket {
		&self.ticket
	}

	/// Takes one place as [`Ticket::acquire`] does, counting this user as
	/// waiting while its asks are refused.
	pub fn acquire(&self) -> Option<Place> {
		let inner = &self.ticket.0;
		let mut state = inner.state.lock();
		let mut counted = self.counted.lock();
		let mine = *counted == Some(state.epoch);
		// A grant to this user keeps the registration waiting while another
		// user of the ticket still waits.
		let keep_waiting = state.waiting > usize::from(mine);
		let deferring = state.gave_way;
		let place = self.ticket.ask(&mut state, keep_waiting);
		match &place {
			// A registration that defers to a waiting one asks for nothing,
			// and does not wait.
			None if deferring && state.gave_way => {}
			None => {
				if !mine {
					state.waiting += 1;
					*counted = Some(state.epoch);
				}
			}
			Some(_) => {
				if mine {
					state.waiting -= 1;
				}
				*counted = None;
			}
		}
		place
	}

	/// Stops this user waiting. The pool's waiting mark is cleared once no
	/// user of the ticket waits.
	pub fn stop_waiting(&self) {
		let inner = &self.ticket.0;
		let mut state = inner.state.lock();
		let mut counted = self.counted.lock();
		if *counted == Some(state.epoch) {
			state.waiting -= 1;
		}
		*counted = None;
		if state.waiting == 0 {
			inner.source.stop_waiting();
		}
	}
}

impl Drop for Asking {
	fn drop(&mut self) {
		self.stop_waiting();
	}
}

/// Places taken up front by [`Ticket::acquire_places`]. They go back to the
/// pool when the grant is dropped.
#[derive(Debug)]
pub struct PlaceGrant {
	places: Vec<Place>,
}

impl PlaceGrant {
	/// The places the grant holds.
	pub fn len(&self) -> usize {
		self.places.len()
	}

	/// Whether the grant holds no place, so its unit of work runs on the
	/// calling task alone.
	pub fn is_empty(&self) -> bool {
		self.places.is_empty()
	}
}
