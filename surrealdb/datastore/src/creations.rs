//! The records a transaction creates in the tables its writes are reading.
//!
//! A write that reads a table reads it a batch at a time through its own
//! transaction and writes between batches, so a later batch sees what the
//! write, or a statement nested in it, has created since it began reading. A
//! [`CreationWatch`] lets such a write tell those records apart from the ones
//! it set out to read: the watch is opened on a table before the write reads
//! it, every record created in a watched table is noted with the order it was
//! created in, and the watch answers whether a record was created after it
//! opened.
//!
//! A creation is noted only while a watch on its table is open, and a table's
//! notes go once no watch on it is open, so what the log holds is bounded by
//! the records created in each watched table while a watch on it is open.
//! With no watch open, noting a creation is a single atomic load. The log
//! holds the notes in memory and never spills them: [`Creations::bytes`]
//! estimates what it holds, for the query whose writes opened the watches to
//! charge to its memory.
//!
//! The notes follow the transaction's save points: a rollback to a save point
//! undoes the notes made since it was set, as it undoes the writes they
//! describe, and a released save point's notes belong to the one enclosing
//! it.

use std::collections::HashMap;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use surrealdb_catalog::{DatabaseId, NamespaceId};
use surrealdb_expr::val::TableName;

/// A transaction's log of the records created in its watched tables.
#[derive(Default)]
pub(crate) struct Creations(Arc<Log>);

#[derive(Default)]
struct Log {
	/// How many watches are open, read without the lock so a creation with no
	/// watch open notes nothing.
	open: AtomicUsize,
	/// How many creations have been noted in every table, which orders them:
	/// a creation a watch answers for is one noted at or after the count it
	/// opened at.
	noted: AtomicU64,
	/// [`LogState::bytes`], read without the lock.
	bytes: AtomicUsize,
	state: parking_lot::Mutex<LogState>,
}

#[derive(Default)]
struct LogState {
	/// The watched tables, each with the notes of its creations.
	tables: Vec<Watched>,
	/// Each note made while a save point is set, with what it replaced in its
	/// table's notes, so a rollback can restore both. Every entry belongs to
	/// a watched table: a table's entries go with its notes.
	journal: Vec<Undo>,
	/// The length of `journal`, and the count of creations noted, when each
	/// open save point was set, innermost last. The lengths never decrease
	/// from one mark to the next.
	marks: Vec<(usize, u64)>,
	/// The count of creations each open watch opened at: a rollback never
	/// takes the count below the highest, so no open watch is handed an order
	/// below its opening.
	opened: Vec<u64>,
	/// An estimate of the bytes the notes and the journal hold: each entry's
	/// key and slot ([`created_bytes`], [`undo_bytes`]). Spare capacity in the
	/// maps and vectors is not counted.
	bytes: usize,
}

/// A note made while a save point is set.
struct Undo {
	/// The count of the note's table, which identifies the table and which a
	/// rollback steps back.
	notes: Arc<AtomicU64>,
	/// The created record's key.
	key: Vec<u8>,
	/// The order the key's previous note held, if it had one.
	replaced: Option<u64>,
}

/// A watched table.
struct Watched {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	/// How many creations have been noted in the table, read without the lock
	/// by its watches: a watch that reads the count it opened at has seen no
	/// creation in its table since it opened, whatever other tables saw.
	notes: Arc<AtomicU64>,
	/// The count of the table's creations each open watch on it opened at,
	/// one per watch: a rollback never takes the table's count below the
	/// highest.
	opened: Vec<u64>,
	/// The key of each record created in the table while it is watched, with
	/// the count of creations noted before its latest creation.
	created: HashMap<Vec<u8>, u64>,
}

impl Watched {
	fn is(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) -> bool {
		self.ns == ns && self.db == db && self.tb == *tb
	}
}

/// The bytes a note of `key` holds in its table's notes.
fn created_bytes(key: &[u8]) -> usize {
	key.len() + size_of::<(Vec<u8>, u64)>() + 1
}

/// The bytes a journal entry holds.
fn undo_bytes(undo: &Undo) -> usize {
	undo.key.len() + size_of::<Undo>()
}

impl Creations {
	/// Open a watch on table `tb`, which answers for the records created in it
	/// from now until the watch is dropped.
	pub(crate) fn watch(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) -> CreationWatch {
		let mut state = self.0.state.lock();
		let at = match state.tables.iter().position(|table| table.is(ns, db, tb)) {
			Some(at) => at,
			None => {
				state.tables.push(Watched {
					ns,
					db,
					tb: tb.clone(),
					notes: Arc::new(AtomicU64::new(0)),
					opened: Vec::new(),
					created: HashMap::new(),
				});
				state.tables.len() - 1
			}
		};
		self.0.open.fetch_add(1, Ordering::SeqCst);
		let since = self.0.noted.load(Ordering::SeqCst);
		state.opened.push(since);
		let table = &mut state.tables[at];
		let notes = Arc::clone(&table.notes);
		let notes_since = notes.load(Ordering::SeqCst);
		table.opened.push(notes_since);
		CreationWatch {
			log: Arc::clone(&self.0),
			tb: tb.clone(),
			since,
			notes,
			notes_since,
		}
	}

	/// Whether any watch is open, so a creation may need noting.
	pub(crate) fn watched(&self) -> bool {
		self.0.open.load(Ordering::SeqCst) > 0
	}

	/// How many watches are open.
	pub(crate) fn open(&self) -> usize {
		self.0.open.load(Ordering::SeqCst)
	}

	/// An estimate of the bytes the log holds: the notes of the watched
	/// tables and the journal a rollback restores them from. Read without the
	/// lock; nothing with no watch open.
	pub(crate) fn bytes(&self) -> usize {
		self.0.bytes.load(Ordering::Relaxed)
	}

	/// Whether a watch is open on table `tb`, so a record created in it is
	/// noted. With no watch open on any table this takes no lock.
	pub(crate) fn watches(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) -> bool {
		self.watched() && self.0.state.lock().watches(ns, db, tb)
	}

	/// Note that the record stored at `key`, in table `tb`, was created.
	pub(crate) fn note(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName, key: Vec<u8>) {
		if !self.watched() {
			return;
		}
		let mut state = self.0.state.lock();
		let LogState {
			tables,
			journal,
			marks,
			bytes,
			..
		} = &mut *state;
		let Some(table) = tables.iter_mut().find(|table| table.is(ns, db, tb)) else {
			return;
		};
		table.notes.fetch_add(1, Ordering::SeqCst);
		let order = self.0.noted.fetch_add(1, Ordering::SeqCst);
		let held = created_bytes(&key);
		if marks.is_empty() {
			if table.created.insert(key, order).is_none() {
				*bytes += held;
			}
		} else {
			let replaced = table.created.insert(key.clone(), order);
			if replaced.is_none() {
				*bytes += held;
			}
			let undo = Undo {
				notes: Arc::clone(&table.notes),
				key,
				replaced,
			};
			*bytes += undo_bytes(&undo);
			journal.push(undo);
		}
		self.0.bytes.store(*bytes, Ordering::Relaxed);
	}

	/// Set a save point: the notes made from now on are undone by a rollback
	/// to it.
	pub(crate) fn push_save_point(&self) {
		let mut state = self.0.state.lock();
		let at = state.journal.len();
		let noted = self.0.noted.load(Ordering::SeqCst);
		state.marks.push((at, noted));
	}

	/// Release the last save point: its notes stand, and belong to the save
	/// point enclosing it.
	pub(crate) fn release_save_point(&self) {
		let mut state = self.0.state.lock();
		state.marks.pop();
		if state.marks.is_empty() {
			let freed: usize = state.journal.iter().map(undo_bytes).sum();
			state.journal = Vec::new();
			state.bytes -= freed;
			self.0.bytes.store(state.bytes, Ordering::Relaxed);
		}
	}

	/// Roll back to the last save point: the notes made since it was set are
	/// undone, and so are the counts they advanced, so a watch the rolled-back
	/// notes alone made answer for creations answers for none again. No count
	/// goes lower than an open watch opened at: an order below its opening
	/// would read to it as a creation before it, and a table's count back at
	/// its opening would read as no creation since. A watch already closed
	/// holds no count up.
	pub(crate) fn rollback_save_point(&self) {
		let mut state = self.0.state.lock();
		let Some((at, noted)) = state.marks.pop() else {
			return;
		};
		let LogState {
			tables,
			journal,
			bytes,
			..
		} = &mut *state;
		while journal.len() > at {
			let Some(undo) = journal.pop() else {
				break;
			};
			*bytes -= undo_bytes(&undo);
			undo.notes.fetch_sub(1, Ordering::SeqCst);
			let Some(table) =
				tables.iter_mut().find(|table| Arc::ptr_eq(&table.notes, &undo.notes))
			else {
				continue;
			};
			match undo.replaced {
				Some(order) => {
					table.created.insert(undo.key, order);
				}
				None => {
					if table.created.remove(&undo.key).is_some() {
						*bytes -= created_bytes(&undo.key);
					}
				}
			}
		}
		let floor = state.opened.iter().copied().max().unwrap_or(0);
		self.0.noted.store(noted.max(floor), Ordering::SeqCst);
		for table in &state.tables {
			if let Some(&floor) = table.opened.iter().max() {
				table.notes.fetch_max(floor, Ordering::SeqCst);
			}
		}
		self.0.bytes.store(state.bytes, Ordering::Relaxed);
	}
}

impl LogState {
	fn watches(&self, ns: NamespaceId, db: DatabaseId, tb: &TableName) -> bool {
		self.tables.iter().any(|table| table.is(ns, db, tb))
	}

	/// Stop watching the table at `at`, which no watch is open on: its notes
	/// go, and so do its journal entries, which no rollback then needs, each
	/// mark keeping the length of the journal entries it was set after.
	fn forget(&mut self, at: usize) {
		let table = self.tables.swap_remove(at);
		let mut freed: usize = table.created.keys().map(|key| created_bytes(key)).sum();
		let LogState {
			journal,
			marks,
			..
		} = self;
		let (mut seen, mut kept, mut mark) = (0, 0, 0);
		journal.retain(|undo| {
			while mark < marks.len() && marks[mark].0 == seen {
				marks[mark].0 = kept;
				mark += 1;
			}
			seen += 1;
			let keep = !Arc::ptr_eq(&undo.notes, &table.notes);
			match keep {
				true => kept += 1,
				false => freed += undo_bytes(undo),
			}
			keep
		});
		for mark in &mut marks[mark..] {
			mark.0 = kept;
		}
		self.bytes -= freed;
	}
}

/// An open watch on the records created in one table of a transaction,
/// closed when dropped.
pub struct CreationWatch {
	log: Arc<Log>,
	tb: TableName,
	/// The count of creations noted when the watch opened.
	since: u64,
	/// The count of creations noted in the watched table, shared with the
	/// table's other watches, which identifies the table in the log.
	notes: Arc<AtomicU64>,
	/// That count when the watch opened.
	notes_since: u64,
}

impl CreationWatch {
	/// The table the watch answers for.
	pub fn table(&self) -> &TableName {
		&self.tb
	}

	/// Whether any record has been created in the watched table since the
	/// watch opened; a creation in another table is none. When not, no record
	/// is [`Self::created`], and its key need not be encoded to ask.
	#[inline]
	pub fn any_created(&self) -> bool {
		self.notes.load(Ordering::Relaxed) != self.notes_since
	}

	/// Whether the record stored at `key`, a record of the watched table, was
	/// created since the watch opened.
	pub fn created(&self, key: &[u8]) -> bool {
		if !self.any_created() {
			return false;
		}
		// The lock orders this read after every note the count reflects.
		let state = self.log.state.lock();
		state
			.tables
			.iter()
			.find(|table| Arc::ptr_eq(&table.notes, &self.notes))
			.and_then(|table| table.created.get(key))
			.is_some_and(|order| *order >= self.since)
	}
}

impl Drop for CreationWatch {
	fn drop(&mut self) {
		let mut state = self.log.state.lock();
		if let Some(at) =
			state.tables.iter().position(|table| Arc::ptr_eq(&table.notes, &self.notes))
		{
			let table = &mut state.tables[at];
			if let Some(own) = table.opened.iter().position(|since| *since == self.notes_since) {
				table.opened.swap_remove(own);
			}
			if table.opened.is_empty() {
				state.forget(at);
			}
		}
		if let Some(own) = state.opened.iter().position(|since| *since == self.since) {
			state.opened.swap_remove(own);
		}
		self.log.open.fetch_sub(1, Ordering::SeqCst);
		if state.tables.is_empty() {
			// Every journal entry belongs to a watched table, so none is left.
			state.journal = Vec::new();
		}
		self.log.bytes.store(state.bytes, Ordering::Relaxed);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn ids() -> (NamespaceId, DatabaseId) {
		(NamespaceId(1), DatabaseId(2))
	}

	/// The notes the log holds, over every watched table.
	fn notes(log: &Creations) -> usize {
		log.0.state.lock().tables.iter().map(|table| table.created.len()).sum()
	}

	/// The bytes the log holds, counted afresh from its notes and journal.
	fn counted(log: &Creations) -> usize {
		let state = log.0.state.lock();
		let notes: usize = state
			.tables
			.iter()
			.flat_map(|table| table.created.keys())
			.map(|key| created_bytes(key))
			.sum();
		notes + state.journal.iter().map(undo_bytes).sum::<usize>()
	}

	/// The log's estimate of its bytes, checked against a fresh count.
	fn bytes(log: &Creations) -> usize {
		let bytes = log.bytes();
		assert_eq!(bytes, counted(log), "the estimate follows the notes and the journal");
		assert_eq!(bytes, log.0.state.lock().bytes);
		bytes
	}

	#[test]
	fn a_watch_answers_for_creations_after_it_opened() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		log.note(ns, db, &t, b"before".to_vec());
		let watch = log.watch(ns, db, &t);
		assert!(!watch.any_created());
		log.note(ns, db, &t, b"after".to_vec());
		assert!(watch.created(b"after"));
		assert!(!watch.created(b"before"));
	}

	#[test]
	fn a_creation_in_an_unwatched_table_is_not_noted() {
		let (ns, db) = ids();
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		let log = Creations::default();
		let watch = log.watch(ns, db, &t);
		assert!(log.watches(ns, db, &t) && !log.watches(ns, db, &u));
		log.note(ns, db, &u, b"u".to_vec());
		assert!(!watch.any_created());
		assert_eq!(notes(&log), 0);
	}

	#[test]
	fn a_creation_in_another_watched_table_keeps_the_fast_path() {
		let (ns, db) = ids();
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		let log = Creations::default();
		let on_t = log.watch(ns, db, &t);
		let on_u = log.watch(ns, db, &u);
		log.note(ns, db, &u, b"u".to_vec());
		assert!(!on_t.any_created(), "a creation in u is none in t");
		assert!(on_u.created(b"u"));
		log.note(ns, db, &t, b"t".to_vec());
		assert!(on_t.created(b"t"));
	}

	#[test]
	fn a_nested_watch_answers_only_for_its_own_span() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let outer = log.watch(ns, db, &t);
		log.note(ns, db, &t, b"first".to_vec());
		let inner = log.watch(ns, db, &t);
		log.note(ns, db, &t, b"second".to_vec());
		assert!(outer.created(b"first") && outer.created(b"second"));
		assert!(!inner.created(b"first") && inner.created(b"second"));
		drop(inner);
		assert!(outer.created(b"first"), "the outer watch still answers once the inner closes");
		drop(outer);
		assert!(!log.watched());
		assert_eq!(notes(&log), 0);
	}

	#[test]
	fn a_rollback_undoes_the_notes_made_since_its_save_point() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let watch = log.watch(ns, db, &t);
		log.note(ns, db, &t, b"kept".to_vec());
		log.push_save_point();
		log.note(ns, db, &t, b"undone".to_vec());
		log.note(ns, db, &t, b"kept".to_vec());
		log.rollback_save_point();
		assert!(!watch.created(b"undone"));
		assert!(watch.created(b"kept"), "a rollback restores the note it replaced");
		// A released save point's notes belong to the one enclosing it.
		log.push_save_point();
		log.push_save_point();
		log.note(ns, db, &t, b"inner".to_vec());
		log.release_save_point();
		assert!(watch.created(b"inner"));
		log.rollback_save_point();
		assert!(!watch.created(b"inner"));
		assert!(log.0.state.lock().journal.is_empty());
	}

	#[test]
	fn a_rollback_of_every_creation_restores_the_fast_path() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let watch = log.watch(ns, db, &t);
		log.push_save_point();
		log.note(ns, db, &t, b"undone".to_vec());
		assert!(watch.any_created());
		log.rollback_save_point();
		assert!(!watch.any_created(), "a rollback of every creation answers for none");
		log.note(ns, db, &t, b"after".to_vec());
		assert!(watch.created(b"after"));
	}

	#[test]
	fn a_rollback_keeps_the_orders_a_later_watch_opened_at() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let outer = log.watch(ns, db, &t);
		log.push_save_point();
		log.note(ns, db, &t, b"undone".to_vec());
		let inner = log.watch(ns, db, &t);
		log.rollback_save_point();
		assert!(!inner.any_created());
		log.note(ns, db, &t, b"after".to_vec());
		assert!(inner.created(b"after"), "an order below the inner watch's opening is not reused");
		assert!(outer.created(b"after"));
		assert!(!outer.created(b"undone"));
	}

	#[test]
	fn a_watch_closed_before_a_rollback_holds_no_count_up() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let outer = log.watch(ns, db, &t);
		log.push_save_point();
		log.note(ns, db, &t, b"undone".to_vec());
		let inner = log.watch(ns, db, &t);
		drop(inner);
		log.rollback_save_point();
		assert!(!outer.any_created(), "a closed inner watch leaves the outer one's fast path");
		log.note(ns, db, &t, b"after".to_vec());
		assert!(outer.created(b"after"));
	}

	#[test]
	fn a_record_created_again_answers_for_its_latest_creation() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let outer = log.watch(ns, db, &t);
		log.note(ns, db, &t, b"r".to_vec());
		let inner = log.watch(ns, db, &t);
		assert!(!inner.created(b"r"));
		log.note(ns, db, &t, b"r".to_vec());
		assert!(inner.created(b"r") && outer.created(b"r"));
	}

	#[test]
	fn a_tables_notes_go_when_its_last_watch_closes() {
		let (ns, db) = ids();
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		let log = Creations::default();
		let first_on_t = log.watch(ns, db, &t);
		let second_on_t = log.watch(ns, db, &t);
		let on_u = log.watch(ns, db, &u);
		log.note(ns, db, &t, b"t1".to_vec());
		log.note(ns, db, &u, b"u1".to_vec());
		assert_eq!(bytes(&log), created_bytes(b"t1") + created_bytes(b"u1"));
		drop(first_on_t);
		assert!(second_on_t.created(b"t1"), "a table's notes stay while a watch on it is open");
		drop(second_on_t);
		assert_eq!(notes(&log), 1, "t's notes go while u is still watched");
		assert_eq!(bytes(&log), created_bytes(b"u1"));
		assert!(on_u.created(b"u1"));
		log.note(ns, db, &t, b"t2".to_vec());
		assert_eq!(notes(&log), 1, "a creation in t is no longer noted");
		drop(on_u);
		assert_eq!(bytes(&log), 0);
	}

	#[test]
	fn a_record_noted_again_holds_no_more() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let _watch = log.watch(ns, db, &t);
		log.note(ns, db, &t, b"r".to_vec());
		let once = bytes(&log);
		log.note(ns, db, &t, b"r".to_vec());
		assert_eq!(bytes(&log), once);
		// Under a save point the journal holds what a rollback restores.
		log.push_save_point();
		log.note(ns, db, &t, b"r".to_vec());
		log.note(ns, db, &t, b"s".to_vec());
		assert!(bytes(&log) > once + created_bytes(b"s"));
		log.release_save_point();
		assert_eq!(bytes(&log), once + created_bytes(b"s"), "a released journal holds nothing");
		log.push_save_point();
		log.note(ns, db, &t, b"undone".to_vec());
		log.rollback_save_point();
		assert_eq!(bytes(&log), once + created_bytes(b"s"));
	}

	#[test]
	fn a_table_forgotten_under_a_save_point_leaves_the_others_journal() {
		let (ns, db) = ids();
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		let log = Creations::default();
		let on_t = log.watch(ns, db, &t);
		let on_u = log.watch(ns, db, &u);
		log.note(ns, db, &u, b"u0".to_vec());
		log.push_save_point();
		log.note(ns, db, &t, b"t1".to_vec());
		log.note(ns, db, &u, b"u1".to_vec());
		drop(on_t);
		assert_eq!(log.0.state.lock().journal.len(), 1, "t's journal entry goes with its notes");
		assert_eq!(bytes(&log), counted(&log));
		log.rollback_save_point();
		assert!(on_u.created(b"u0"), "a note before the save point stands");
		assert!(!on_u.created(b"u1"), "u's note since the save point is undone");
		assert_eq!(bytes(&log), created_bytes(b"u0"));
		drop(on_u);
		assert_eq!(bytes(&log), 0);
	}

	#[test]
	fn forgetting_a_table_keeps_each_save_point_at_its_entries() {
		let (ns, db) = ids();
		let (t, u) = (TableName::from("t"), TableName::from("u"));
		let log = Creations::default();
		let on_t = log.watch(ns, db, &t);
		let on_u = log.watch(ns, db, &u);
		log.push_save_point();
		log.note(ns, db, &u, b"a".to_vec());
		log.note(ns, db, &t, b"b".to_vec());
		log.push_save_point();
		log.note(ns, db, &t, b"c".to_vec());
		log.note(ns, db, &u, b"d".to_vec());
		drop(on_t);
		assert_eq!(
			log.0.state.lock().marks.iter().map(|mark| mark.0).collect::<Vec<_>>(),
			[0, 1],
			"each save point keeps the entries of u it was set after"
		);
		log.rollback_save_point();
		assert!(on_u.created(b"a") && !on_u.created(b"d"));
		log.rollback_save_point();
		assert!(!on_u.created(b"a"));
		assert!(!on_u.any_created(), "a rollback of every creation in u answers for none");
		assert_eq!(bytes(&log), 0);
	}

	#[test]
	fn a_table_watched_again_under_a_save_point_answers_for_its_new_watch() {
		let (ns, db) = ids();
		let t = TableName::from("t");
		let log = Creations::default();
		let first = log.watch(ns, db, &t);
		log.push_save_point();
		log.note(ns, db, &t, b"x".to_vec());
		drop(first);
		let again = log.watch(ns, db, &t);
		assert!(!again.created(b"x"), "a creation before the watch opened is not its own");
		log.note(ns, db, &t, b"y".to_vec());
		assert!(again.created(b"y"));
		log.rollback_save_point();
		assert!(!again.any_created() && !again.created(b"y"));
		assert_eq!(bytes(&log), 0);
		log.note(ns, db, &t, b"z".to_vec());
		assert!(again.created(b"z"));
	}
}
