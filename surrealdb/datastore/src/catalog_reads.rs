//! What a stretch of work reads of a transaction's catalog.
//!
//! A planner that compiles code once and reuses it — a function body shared
//! across requests, a block statement kept for later reaches — reuses it only
//! while everything the compilation read of the catalog still reads the same.
//! [`Transaction::record_catalog_reads`](crate::tx::Transaction) opens a
//! recording; every catalog read the transaction answers while it is open is
//! noted in it, and [`CatalogReadRecording::finish`] hands back what was read,
//! by name: the tables whose definitions, fields, indexes, events or views
//! were read, the functions, the parameters, the databases, the databases
//! whose tables were listed one by one, and the `REFERENCE` fields read for
//! the records of a table. What the caller compares later is the
//! state of each named entry, read again.
//!
//! A read of anything else in the catalog — a listing of every table, an
//! analyzer, an access method, a user, a sequence, a model, an API, a bucket,
//! a config — marks the recording [`CatalogReads::opaque`]: the names alone do
//! not say what such a read depended on, so what it compiled is not reused.
//!
//! Recordings nest: a note goes to every recording open on the transaction,
//! so a compilation holding another sees the reads of both. A note costs one
//! atomic load while no recording is open.
//!
//! A stretch of work may also be declared [`sealed`]: a catalog read made
//! while the innermost declaration around it is sealed is a fault, which a
//! build with debug assertions panics on. The physical plan's lowering into a
//! program runs sealed, and the mapping it reaches runs unsealed, so the
//! check proves the lowering takes every definition from the plan. A running
//! program runs sealed too, with the loads it is allowed declared unsealed
//! where they are made. A read the storage layer makes of a table's layout to
//! serve a record ([`crate::tx::Transaction::layout_tb`]) is its own, made
//! unsealed.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use surrealdb_catalog::{DatabaseId, NamespaceId};
use surrealdb_expr::val::TableName;

/// The catalog entries a recording saw read, by name.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatalogReads {
	/// Tables whose definition, fields, indexes, events, views or catalog
	/// version were read.
	pub tables: BTreeSet<(NamespaceId, DatabaseId, TableName)>,
	/// Tables whose definition was read by namespace and database name.
	pub named_tables: BTreeSet<(String, String, TableName)>,
	/// Functions whose definition was read, without their `fn::` prefix.
	pub functions: BTreeSet<(NamespaceId, DatabaseId, String)>,
	/// Parameters whose `DEFINE PARAM` was read.
	pub params: BTreeSet<(NamespaceId, DatabaseId, String)>,
	/// Databases read by name, as namespace and database names.
	pub databases: BTreeSet<(String, String)>,
	/// Databases whose listing of tables was read for a compilation that
	/// specialises per table: what the compilation holds depends on which
	/// tables are defined, as well as on the state of each.
	pub listings: BTreeSet<(NamespaceId, DatabaseId)>,
	/// Databases whose `REFERENCE` fields that can name a record of a table
	/// were read, by the table, or by `None` for the fields that can name a
	/// record of any table: what the compilation holds depends on which
	/// fields those are, and not on any other table of the database.
	pub references: BTreeSet<(NamespaceId, DatabaseId, Option<TableName>)>,
	/// Whether something else in the catalog was read.
	pub opaque: bool,
}

/// One read a transaction answered from its catalog.
#[derive(Clone, Copy)]
pub(crate) enum CatalogRead<'a> {
	Table(NamespaceId, DatabaseId, &'a TableName),
	TableByName(&'a str, &'a str, &'a TableName),
	Function(NamespaceId, DatabaseId, &'a str),
	Param(NamespaceId, DatabaseId, &'a str),
	Database(&'a str, &'a str),
	Listing(NamespaceId, DatabaseId),
	References(NamespaceId, DatabaseId, Option<&'a TableName>),
	Opaque,
}

impl CatalogReads {
	fn add(&mut self, read: &CatalogRead<'_>) {
		match read {
			CatalogRead::Table(ns, db, tb) => {
				self.tables.insert((*ns, *db, (*tb).clone()));
			}
			CatalogRead::TableByName(ns, db, tb) => {
				self.named_tables.insert(((*ns).to_owned(), (*db).to_owned(), (*tb).clone()));
			}
			CatalogRead::Function(ns, db, name) => {
				self.functions.insert((*ns, *db, (*name).to_owned()));
			}
			CatalogRead::Param(ns, db, name) => {
				self.params.insert((*ns, *db, (*name).to_owned()));
			}
			CatalogRead::Database(ns, db) => {
				self.databases.insert(((*ns).to_owned(), (*db).to_owned()));
			}
			CatalogRead::Listing(ns, db) => {
				self.listings.insert((*ns, *db));
			}
			CatalogRead::References(ns, db, target) => {
				self.references.insert((*ns, *db, target.cloned()));
			}
			CatalogRead::Opaque => self.opaque = true,
		}
	}
}

/// The recordings open on one transaction.
#[derive(Default)]
pub(crate) struct CatalogReadLog {
	/// How many recordings are open, read before taking the lock.
	open: AtomicUsize,
	next: AtomicU64,
	recordings: Mutex<Vec<(u64, CatalogReads)>>,
}

impl CatalogReadLog {
	/// Note `read` in every open recording.
	pub(crate) fn note(&self, read: CatalogRead<'_>) {
		#[cfg(debug_assertions)]
		check_unsealed(&read);
		self.answered(read);
	}

	/// Note in every open recording `read`, which the caller answered from
	/// what it read of the catalog earlier: a dependency, and no read of the
	/// catalog, so work [`sealed`] off it may note one.
	pub(crate) fn answered(&self, read: CatalogRead<'_>) {
		if self.open.load(Ordering::Acquire) == 0 {
			return;
		}
		let mut recordings = self.recordings.lock().unwrap_or_else(PoisonError::into_inner);
		for (_, reads) in recordings.iter_mut() {
			reads.add(&read);
		}
	}

	pub(crate) fn begin(&self) -> CatalogReadRecording<'_> {
		let id = self.next.fetch_add(1, Ordering::Relaxed);
		self.recordings
			.lock()
			.unwrap_or_else(PoisonError::into_inner)
			.push((id, CatalogReads::default()));
		self.open.fetch_add(1, Ordering::Release);
		CatalogReadRecording {
			log: self,
			id,
			done: false,
		}
	}

	fn end(&self, id: u64) -> CatalogReads {
		let mut recordings = self.recordings.lock().unwrap_or_else(PoisonError::into_inner);
		let reads = match recordings.iter().position(|(open, _)| *open == id) {
			Some(at) => recordings.remove(at).1,
			None => CatalogReads::default(),
		};
		self.open.fetch_sub(1, Ordering::Release);
		reads
	}
}

/// An open recording of catalog reads; see the module.
pub struct CatalogReadRecording<'a> {
	log: &'a CatalogReadLog,
	id: u64,
	done: bool,
}

impl CatalogReadRecording<'_> {
	/// Close the recording, answering what was read while it was open.
	pub fn finish(mut self) -> CatalogReads {
		self.done = true;
		self.log.end(self.id)
	}
}

impl Drop for CatalogReadRecording<'_> {
	fn drop(&mut self) {
		if !self.done {
			self.log.end(self.id);
		}
	}
}

#[cfg(debug_assertions)]
thread_local! {
	/// Whether the future being polled on this thread is sealed off the
	/// catalog; see [`sealed`].
	static SEALED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

#[cfg(debug_assertions)]
fn check_unsealed(read: &CatalogRead<'_>) {
	if SEALED.with(std::cell::Cell::get) {
		let what = match read {
			CatalogRead::Table(_, _, tb) | CatalogRead::TableByName(_, _, tb) => {
				format!("the table {tb}")
			}
			CatalogRead::Function(_, _, name) => format!("the function {name}"),
			CatalogRead::Param(_, _, name) => format!("the parameter {name}"),
			CatalogRead::Database(ns, db) => format!("the database {ns}/{db}"),
			CatalogRead::Listing(..) => "the listing of tables".to_owned(),
			CatalogRead::References(..) => "the reference fields".to_owned(),
			CatalogRead::Opaque => "the catalog".to_owned(),
		};
		panic!("{what} was read from the catalog by work sealed off it");
	}
}

/// `inner`, run sealed off the catalog when `sealed` holds and unsealed when
/// it does not, whatever the work around it is: every poll of `inner` sees
/// the declaration, and the work around it sees its own again once the poll
/// returns. In a build without debug assertions the declaration is not kept
/// and `inner` runs as it is.
pub fn sealed<F: std::future::Future>(sealed: bool, inner: F) -> Sealed<F> {
	Sealed {
		sealed,
		inner,
	}
}

/// Declare the rest of the caller's synchronous scope sealed off the catalog
/// when `sealed` holds and unsealed when it does not, until the answered
/// declaration is dropped, which puts back the declaration around it. For a
/// poll that cannot be wrapped in [`sealed`], such as a stream's. In a build
/// without debug assertions nothing is declared.
pub fn declare(sealed: bool) -> Declaration {
	#[cfg(debug_assertions)]
	{
		Declaration {
			_restore: Restore(SEALED.with(|cell| cell.replace(sealed))),
		}
	}
	#[cfg(not(debug_assertions))]
	{
		let _ = sealed;
		Declaration {}
	}
}

/// A declaration made by [`declare`], in force until it is dropped.
#[must_use = "the declaration ends when it is dropped"]
pub struct Declaration {
	#[cfg(debug_assertions)]
	_restore: Restore,
}

/// A future [`sealed`] or unsealed off the catalog.
pub struct Sealed<F> {
	#[cfg_attr(not(debug_assertions), allow(dead_code))]
	sealed: bool,
	inner: F,
}

impl<F: std::future::Future> std::future::Future for Sealed<F> {
	type Output = F::Output;

	fn poll(
		self: std::pin::Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> std::task::Poll<F::Output> {
		#[cfg(debug_assertions)]
		let _restore = Restore(SEALED.with(|cell| cell.replace(self.sealed)));
		// SAFETY: `inner` is pinned whenever `self` is: it is never moved out
		// of `self`, and `Sealed` implements neither `Unpin` by hand nor `Drop`.
		let inner = unsafe { self.map_unchecked_mut(|this| &mut this.inner) };
		inner.poll(cx)
	}
}

/// Puts back the declaration of the work around a poll, on return or unwind.
#[cfg(debug_assertions)]
struct Restore(bool);

#[cfg(debug_assertions)]
impl Drop for Restore {
	fn drop(&mut self) {
		SEALED.with(|cell| cell.set(self.0));
	}
}

#[cfg(all(test, debug_assertions))]
mod tests {
	use super::{CatalogRead, CatalogReadLog, declare, sealed};

	#[tokio::test]
	#[should_panic(expected = "sealed off it")]
	async fn a_read_made_sealed_panics() {
		let log = CatalogReadLog::default();
		sealed(true, async { log.note(CatalogRead::Opaque) }).await;
	}

	#[tokio::test]
	async fn a_read_made_unsealed_inside_sealed_work_passes() {
		let log = CatalogReadLog::default();
		sealed(true, sealed(false, async { log.note(CatalogRead::Opaque) })).await;
		// Once the sealed work returns, the work around it reads freely.
		log.note(CatalogRead::Opaque);
	}

	#[tokio::test]
	async fn a_noted_dependency_is_no_read() {
		let log = CatalogReadLog::default();
		let recording = log.begin();
		sealed(true, async { log.answered(CatalogRead::Opaque) }).await;
		assert!(recording.finish().opaque);
	}

	#[test]
	#[should_panic(expected = "sealed off it")]
	fn a_read_made_under_a_sealed_declaration_panics() {
		let log = CatalogReadLog::default();
		let _declared = declare(true);
		log.note(CatalogRead::Opaque);
	}

	#[test]
	fn a_declaration_ends_when_it_is_dropped() {
		let log = CatalogReadLog::default();
		drop(declare(true));
		log.note(CatalogRead::Opaque);
	}
}
