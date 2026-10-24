//! Walks over the catalog that a paged pass keeps between its passes: over
//! its databases, and over its indexes, a step at a time; and a probe of the
//! keyspace for the databases that hold keys, whatever their definitions.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;

use anyhow::Result;
use surrealdb_datastore::tx::refused_by_release;
use surrealdb_strand::TableName;

use crate::catalog::{
	DatabaseDefinition, DatabaseId, FromStored, IndexDefinition, NamespaceDefinition, NamespaceId,
	StoredIndexDefinition, StoredTableDefinition, TableId,
};
use crate::key::schema::{
	DatabasePrefix, DbPrefix, DbRoot, IndexDefPrefix, NamespacePrefix, NsPrefix, NsRoot,
	TablePrefix,
};
use crate::key::{KVSubspace, KVValue, Key, RawRange, Resumable, TypedRange};
use crate::kvs::{Direction, NORMAL_BATCH_SIZE, Transaction};

/// One step of a walk over the catalog.
///
/// A walk descends the catalog's lists a level at a time, and reads every
/// list a page at a time as it needs it, so a step costs at most a page of
/// each level it moves through. A caller that charges every step to its
/// budget bounds what a pass reads however many levels the catalog holds
/// that are empty of what the walk is after: a namespace without a database,
/// or a table without an index, is passed in a step of its own.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Step<T> {
	/// The walk reached one of what it is after.
	Reached(T),
	/// The walk entered a level — began the list of a namespace, a database
	/// or a table — passed an item it is not after, or read a page of a list
	/// holding only entries a newer release wrote, and reached nothing.
	Passed,
}

/// The databases of the catalog, reached one at a time in catalog order.
///
/// The lists the walk reads, the namespaces and each namespace's databases,
/// are read a page of at most [`NORMAL_BATCH_SIZE`] definitions at a time as
/// the walk needs them, so a pass that takes a step of the walk reads at most
/// a page of each list however many definitions the lists hold, and a walk
/// that spans passes reads each page once. A list is read as it stands page
/// by page, up to the last definition there was when its first page was read:
/// a definition added behind the walk's position, or past that one while the
/// walk runs, is the next walk's, so a walk ends however fast definitions are
/// added.
#[derive(Clone, Debug, Default)]
pub(crate) struct CatalogWalk {
	/// The scan of the namespace list; `None` before the walk has begun.
	namespaces: Option<ListScan<NamespaceDefinition>>,
	/// The namespace the walk is in, by name, and the scan of its database
	/// list.
	databases: Option<(Arc<str>, ListScan<DatabaseDefinition>)>,
	/// The entries the scans the walk has moved past skipped because a newer
	/// release wrote them, since the count was last taken.
	refused: usize,
}

impl CatalogWalk {
	/// The next step of the walk: the next database of the catalog, with the
	/// name of its namespace; the namespace entered to reach one; or `None`
	/// once the walk has passed them all. Reads through `txn` the page of a
	/// list it needs next, if any: at most a page of the namespace in hand's
	/// databases, and a page of the namespaces.
	pub(crate) async fn next(
		&mut self,
		txn: &Transaction,
	) -> Result<Option<Step<(Arc<str>, DatabaseDefinition)>>> {
		let namespaces = match &mut self.namespaces {
			Some(namespaces) => namespaces,
			None => self.namespaces.insert(ListScan::new(NamespacePrefix {}.range()?)),
		};
		if let Some((ns, dbs)) = &mut self.databases {
			match dbs.next(txn).await? {
				Some(Step::Reached(db)) => return Ok(Some(Step::Reached((Arc::clone(ns), db)))),
				Some(Step::Passed) => return Ok(Some(Step::Passed)),
				None => {}
			}
		}
		// The namespace in hand, if any, has no database left: enter the
		// next, whose databases the next step reaches.
		let ns = match namespaces.next(txn).await? {
			Some(Step::Reached(ns)) => ns,
			Some(Step::Passed) => return Ok(Some(Step::Passed)),
			None => return Ok(None),
		};
		let dbs = ListScan::new(
			DatabasePrefix {
				ns: ns.namespace_id,
			}
			.range()?,
		);
		if let Some((_, passed)) = &mut self.databases {
			self.refused += passed.take_refused();
		}
		self.databases = Some((Arc::from(ns.name.to_string()), dbs));
		Ok(Some(Step::Passed))
	}

	/// Whether the walk has a namespace or a database still to reach. A walk
	/// that has not begun has: the catalog is unread.
	pub(crate) fn has_more(&self) -> bool {
		match &self.namespaces {
			None => true,
			Some(namespaces) => {
				namespaces.has_more()
					|| self.databases.as_ref().is_some_and(|(_, dbs)| dbs.has_more())
			}
		}
	}

	/// The namespaces and databases the walk skipped since the count was
	/// last taken because a newer release wrote them. A skipped namespace
	/// hides its databases, and a skipped database what it holds.
	pub(crate) fn take_refused(&mut self) -> usize {
		std::mem::take(&mut self.refused)
			+ self.namespaces.as_mut().map_or(0, ListScan::take_refused)
			+ self.databases.as_mut().map_or(0, |(_, dbs)| dbs.take_refused())
	}
}

/// The indexes of the catalog, reached one at a time in catalog order: by
/// namespace, database, table and index name.
///
/// Built on [`CatalogWalk`]: each database's tables and each table's indexes
/// are read a page at a time as the walk needs them, the way the walk reads
/// the namespaces and databases, so a step reads at most a page of each level
/// it moves through. An index pending removal is passed rather than reached:
/// the tombstone reaper clears it.
#[derive(Clone, Debug, Default)]
pub(crate) struct IndexWalk {
	/// The walk over the databases.
	databases: CatalogWalk,
	/// The database the walk is in and the scan of its table list.
	tables: Option<(WalkedDatabase, ListScan<StoredTableDefinition>)>,
	/// The table the walk is in, by id, and the scan of its index list.
	indexes: Option<(TableId, ListScan<StoredIndexDefinition>)>,
	/// Indexes the walk reached that were given back to it, reached again
	/// ahead of the scans in the order they were given.
	given_back: VecDeque<WalkedIndex>,
	/// The entries the scans the walk has moved past skipped because a newer
	/// release wrote them, since the count was last taken.
	refused: usize,
}

/// The database an [`IndexWalk`] is in: the names its indexes' build contexts
/// resolve it by, and the ids its keys are under.
#[derive(Clone, Debug)]
struct WalkedDatabase {
	ns_name: Arc<str>,
	db_name: Arc<str>,
	ns: NamespaceId,
	db: DatabaseId,
}

/// An index an [`IndexWalk`] reached, with the names and ids its build state
/// is addressed by, as the catalog held them when the walk reached it.
#[derive(Clone, Debug)]
pub(crate) struct WalkedIndex {
	pub(crate) ns_name: Arc<str>,
	pub(crate) db_name: Arc<str>,
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	pub(crate) tb: TableId,
	pub(crate) index: IndexDefinition,
}

impl IndexWalk {
	/// The next step of the walk: the next index of the catalog; the
	/// namespace, database or table entered to reach one, or an index pending
	/// removal passed; or `None` once the walk has passed them all. Reads
	/// through `txn` the page of a list it needs next, if any: at most a page
	/// of the table in hand's indexes, of the database in hand's tables, and
	/// of the lists the database walk reads.
	pub(crate) async fn next(&mut self, txn: &Transaction) -> Result<Option<Step<WalkedIndex>>> {
		if let Some(index) = self.given_back.pop_front() {
			return Ok(Some(Step::Reached(index)));
		}
		if let Some((database, _)) = &self.tables
			&& let Some((tb, ixs)) = &mut self.indexes
		{
			match ixs.next(txn).await? {
				Some(Step::Reached(ix)) => {
					if ix.prepare_remove {
						return Ok(Some(Step::Passed));
					}
					return Ok(Some(Step::Reached(WalkedIndex {
						ns_name: Arc::clone(&database.ns_name),
						db_name: Arc::clone(&database.db_name),
						ns: database.ns,
						db: database.db,
						tb: *tb,
						index: IndexDefinition::from_stored(&ix)?,
					})));
				}
				Some(Step::Passed) => return Ok(Some(Step::Passed)),
				None => {}
			}
		}
		// The table in hand, if any, has no index left: enter the next table
		// of the database in hand, if any, whose indexes the next step reaches.
		if let Some((database, tbs)) = &mut self.tables {
			match tbs.next(txn).await? {
				Some(Step::Reached(tb)) => {
					let name = TableName::from(tb.name);
					let ixs = ListScan::new(
						IndexDefPrefix {
							ns: database.ns,
							db: database.db,
							tb: Cow::Borrowed(&name),
						}
						.range()?,
					);
					if let Some((_, passed)) = &mut self.indexes {
						self.refused += passed.take_refused();
					}
					self.indexes = Some((tb.table_id, ixs));
					return Ok(Some(Step::Passed));
				}
				Some(Step::Passed) => return Ok(Some(Step::Passed)),
				None => {}
			}
		}
		// The database in hand, if any, has no table left: enter the next
		// database, or the namespace on the way to one.
		match self.databases.next(txn).await? {
			Some(Step::Reached((ns_name, db))) => {
				let tbs = ListScan::new(
					TablePrefix {
						ns: db.namespace_id,
						db: db.database_id,
					}
					.range()?,
				);
				let database = WalkedDatabase {
					ns_name,
					db_name: Arc::from(db.name.as_str()),
					ns: db.namespace_id,
					db: db.database_id,
				};
				if let Some((_, passed)) = &mut self.tables {
					self.refused += passed.take_refused();
				}
				if let Some((_, passed)) = &mut self.indexes {
					self.refused += passed.take_refused();
				}
				self.tables = Some((database, tbs));
				self.indexes = None;
				Ok(Some(Step::Passed))
			}
			Some(Step::Passed) => Ok(Some(Step::Passed)),
			None => Ok(None),
		}
	}

	/// Give back indexes the walk reached that were not checked, in the order
	/// they were reached, so the walk reaches them again before it goes on.
	pub(crate) fn give_back(&mut self, indexes: impl IntoIterator<Item = WalkedIndex>) {
		self.given_back.extend(indexes);
	}

	/// Whether the walk has an index, or a level that may hold one, still to
	/// reach. A walk that has not begun has: the catalog is unread.
	pub(crate) fn has_more(&self) -> bool {
		!self.given_back.is_empty()
			|| self.databases.has_more()
			|| self.tables.as_ref().is_some_and(|(_, tbs)| tbs.has_more())
			|| self.indexes.as_ref().is_some_and(|(_, ixs)| ixs.has_more())
	}

	/// The catalog entries the walk skipped since the count was last taken
	/// because a newer release wrote them, at every level: a skipped
	/// namespace, database or table hides the indexes under it.
	pub(crate) fn take_refused(&mut self) -> usize {
		std::mem::take(&mut self.refused)
			+ self.databases.take_refused()
			+ self.tables.as_mut().map_or(0, |(_, tbs)| tbs.take_refused())
			+ self.indexes.as_mut().map_or(0, |(_, ixs)| ixs.take_refused())
	}
}

/// The databases that hold keys, found from the keyspace rather than from
/// the catalog, a key read a step, in key order: by namespace id, then by
/// database id.
///
/// A catalog definition this build cannot read hides the id its subtree is
/// addressed by, so a [`CatalogWalk`] passes such a namespace or database
/// without naming it. Its keys still sit under `/*{ns}` and `/*{ns}*{db}`, so
/// the probe reaches every database subtree that holds a key, whatever its
/// definition: one this build reads, one a newer release wrote, or one removed
/// whose keys are still being reclaimed. A database whose subtree holds no key
/// is not reached. A step reads the first key of the next database subtree in
/// the namespace in hand and steps past the subtree, or, once the namespace has
/// none left, the first key of the next namespace subtree, which it enters; so
/// a step reads at most two keys however many a subtree holds. A key that
/// names no subtree is passed in a step of its own.
#[derive(Debug)]
pub(crate) struct DatabaseProbe {
	/// What is left of the namespaces' band.
	namespaces: RawRange,
	/// The namespace the probe is in, and what is left of its databases'
	/// band; `None` when the next step enters the next namespace.
	databases: Option<(NamespaceId, RawRange)>,
}

impl DatabaseProbe {
	pub(crate) fn new() -> Result<Self> {
		Ok(Self {
			namespaces: NsPrefix {}.range()?,
			databases: None,
		})
	}

	/// The next step of the probe: the next database subtree, by id; a
	/// namespace entered, or a key that names no subtree, passed; or `None`
	/// once every namespace subtree is passed. Reads through `txn`.
	pub(crate) async fn next(
		&mut self,
		txn: &Transaction,
	) -> Result<Option<Step<(NamespaceId, DatabaseId)>>> {
		if let Some((ns, band)) = &mut self.databases {
			let ns = *ns;
			if let Some(key) = txn.keys_raw(band.clone(), 1, 0, None).await?.into_iter().next() {
				let prefix = KVSubspace::encode_bound(&DbPrefix {
					ns,
				})?;
				*band = match database_subtree(ns, &prefix, &key) {
					Some((db, past)) => {
						let rest = band.part(past.as_slice(), band.bounds().1);
						self.databases = Some((ns, rest));
						return Ok(Some(Step::Reached((ns, db))));
					}
					None => band.clone().resume_after(&key, Direction::Forward),
				};
				return Ok(Some(Step::Passed));
			}
			self.databases = None;
		}
		let Some(key) = txn.keys_raw(self.namespaces.clone(), 1, 0, None).await?.into_iter().next()
		else {
			return Ok(None);
		};
		let prefix = KVSubspace::encode_bound(&NsPrefix {})?;
		self.namespaces = match namespace_subtree(&prefix, &key) {
			Some((ns, past)) => {
				self.databases = Some((
					ns,
					DbPrefix {
						ns,
					}
					.range()?,
				));
				self.namespaces.part(past.as_slice(), self.namespaces.bounds().1)
			}
			None => self.namespaces.clone().resume_after(&key, Direction::Forward),
		};
		Ok(Some(Step::Passed))
	}
}

/// The namespace whose subtree holds `key`, a key in the namespaces' band
/// whose encoded bound is `prefix`, with the first key past that subtree;
/// `None` for a key that names no namespace subtree.
pub(crate) fn namespace_subtree(prefix: &[u8], key: &[u8]) -> Option<(NamespaceId, Key<'static>)> {
	let ns = decode_after::<NamespaceId>(prefix, key)?;
	let root = NsRoot {
		ns,
	};
	holds(&root.range_subtree().ok()?, key)?;
	Some((ns, root.skip_extensions().ok()?))
}

/// The database of namespace `ns` whose subtree holds `key`, a key in the
/// namespace's databases' band whose encoded bound is `prefix`, with the first
/// key past that subtree; `None` for a key that names no database subtree.
pub(crate) fn database_subtree(
	ns: NamespaceId,
	prefix: &[u8],
	key: &[u8],
) -> Option<(DatabaseId, Key<'static>)> {
	let db = decode_after::<DatabaseId>(prefix, key)?;
	let root = DbRoot {
		ns,
		db,
	};
	holds(&root.range_subtree().ok()?, key)?;
	Some((db, root.skip_extensions().ok()?))
}

/// The `T` encoded in `key` right after `prefix`, the encoded bound of the band it sits in.
pub(crate) fn decode_after<'k, T: storekey::BorrowDecode<'k, ()>>(
	prefix: &[u8],
	key: &'k [u8],
) -> Option<T> {
	let mut reader = storekey::BorrowReader::new(key.strip_prefix(prefix)?);
	T::borrow_decode(&mut reader).ok()
}

/// `Some` when `range` holds `key`.
pub(crate) fn holds(range: &RawRange, key: &[u8]) -> Option<()> {
	let (start, end) = range.bounds();
	(start <= key && key < end).then_some(())
}

/// A paged scan of one catalog list, holding at most a page of it.
#[derive(Clone, Debug)]
pub(crate) struct ListScan<T> {
	/// The list's whole key range.
	range: TypedRange<T>,
	/// The definitions of the page read last that the scan has still to
	/// pass, front first.
	page: VecDeque<T>,
	/// The last key of the page read last, which the next page is read after;
	/// `None` before the first page.
	after: Option<Vec<u8>>,
	/// The last key the list held when its first page proved full. The page
	/// that reaches it ends the scan and is cut there, so definitions added
	/// past it while the scan runs are the next scan's, and the scan ends
	/// however fast they are added. `None` before that, and for a list one
	/// page held whole.
	end: Option<Vec<u8>>,
	/// Whether the page read last was the scan's last.
	done: bool,
	/// The entries skipped because a newer release wrote them, since the
	/// count was last taken.
	refused: usize,
}

impl<T: KVValue<KeyContext = ()>> ListScan<T> {
	pub(crate) fn new(range: TypedRange<T>) -> Self {
		Self {
			range,
			page: VecDeque::new(),
			after: None,
			end: None,
			done: false,
			refused: 0,
		}
	}

	/// The next step of the list: the next definition, reading the next page
	/// through `txn` once the page in hand is passed; a page read that held
	/// only entries a newer release wrote, passed as a step of its own; or
	/// `None` once the last page is read.
	///
	/// Such a page is a step so that a caller charging every step to its
	/// budget reads one page a step, however many pages of entries it cannot
	/// read the list holds; the entries are counted for
	/// [`Self::take_refused`].
	pub(crate) async fn next(&mut self, txn: &Transaction) -> Result<Option<Step<T>>> {
		if let Some(item) = self.page.pop_front() {
			return Ok(Some(Step::Reached(item)));
		}
		let Some(page) = self.next_page(txn).await? else {
			return Ok(None);
		};
		self.page = page.into();
		match self.page.pop_front() {
			Some(item) => Ok(Some(Step::Reached(item))),
			// The list's last page, holding nothing to reach, ends the scan.
			None if self.done => Ok(None),
			None => Ok(Some(Step::Passed)),
		}
	}

	/// The next page of the list: the definitions in hand not yet passed, if
	/// any, and otherwise the page read next through `txn`, of at most
	/// [`NORMAL_BATCH_SIZE`] definitions; or `None` once the last page is
	/// read. The definitions of a page returned here are passed by it. The
	/// last page is the one the list ends in, or the one that reaches the
	/// last key the list held when its first page proved full, cut there.
	pub(crate) async fn next_page(&mut self, txn: &Transaction) -> Result<Option<Vec<T>>> {
		if !self.page.is_empty() {
			return Ok(Some(std::mem::take(&mut self.page).into()));
		}
		if self.done {
			return Ok(None);
		}
		let range = match &self.after {
			Some(after) => self.range.clone().resume_after(after, Direction::Forward),
			None => self.range.clone(),
		};
		let batch = txn.batch_keys_vals(range, NORMAL_BATCH_SIZE, None).await?;
		let mut entries = batch.result;
		// A short page is the list's end. A full page means the scan may span
		// pages and passes, so its end is fixed at the last key the list holds
		// now, if not already; a list that reads as empty here ends at this
		// page.
		self.done = batch.next.is_none();
		if let Some((last, _)) = entries.last()
			&& !self.done
			&& self.end.is_none()
		{
			let tail = txn.keysr(self.range.clone(), 1, 0, None).await?;
			self.end = Some(tail.into_iter().next().unwrap_or_else(|| last.clone()));
		}
		if let Some(end) = &self.end {
			// The page that reaches the end ends the scan, and what it holds
			// past the end is the next scan's.
			let within = entries.iter().take_while(|(k, _)| k <= end).count();
			if within < entries.len() {
				entries.truncate(within);
				self.done = true;
			} else if entries.last().is_some_and(|(k, _)| k == end) {
				self.done = true;
			}
		}
		let mut page = Vec::with_capacity(entries.len());
		for (_, v) in entries.iter() {
			match T::kv_decode_value(v, ()) {
				Ok(value) => page.push(value),
				// An entry a newer release wrote is skipped and counted, so the
				// pass carries on past it; what it holds is left as it is.
				Err(e) if refused_by_release(&e) => self.refused += 1,
				Err(e) => return Err(e),
			}
		}
		self.after = entries.last().map(|(k, _)| k.clone());
		Ok(Some(page))
	}

	/// Whether the scan has a definition still to reach.
	pub(crate) fn has_more(&self) -> bool {
		!self.page.is_empty() || !self.done
	}

	/// The last key of the page read last, whether or not this build read
	/// its entry, which the next page is read after; `None` before the first
	/// page.
	pub(crate) fn last_key(&self) -> Option<&[u8]> {
		self.after.as_deref()
	}

	/// The entries skipped since the count was last taken because a newer
	/// release wrote them.
	pub(crate) fn take_refused(&mut self) -> usize {
		std::mem::take(&mut self.refused)
	}
}

#[cfg(test)]
mod tests {
	use surrealdb_kvs::TransactionType::{Read, Write};

	use super::*;
	use crate::catalog::providers::CatalogProvider;
	use crate::dbs::Session;
	use crate::key::KVKey;
	use crate::key::schema::{DatabaseKey, NamespaceKey};
	use crate::kvs::Datastore;
	use crate::kvs::test_support::with_future_floor;

	/// A namespace's databases are read a page at a time as the walk passes
	/// them, so reaching a namespace costs at most a page of its list however
	/// many databases it holds.
	///
	/// The walk listed a namespace's databases whole on reaching it, so a
	/// pass that took one database from a namespace of many read them all, in
	/// a slice a host driving maintenance itself waits on.
	#[tokio::test]
	async fn a_namespace_is_read_a_page_at_a_time() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let txn = ds.transaction(Write).await?;
		for n in 0..page + 1 {
			txn.ensure_ns_db(None, "test", &format!("db{n:04}")).await?;
		}
		txn.commit().await?;

		let txn = ds.transaction(Read).await?;
		let mut walk = CatalogWalk::default();
		assert!(
			matches!(walk.next(&txn).await?, Some(Step::Passed)),
			"entering the namespace is a step of its own"
		);
		let Some(Step::Reached((ns, _))) = walk.next(&txn).await? else {
			panic!("the namespace holds databases");
		};
		assert_eq!(ns.as_ref(), "test");
		let (_, scan) = walk.databases.as_ref().expect("the walk is in the namespace");
		assert_eq!(scan.page.len(), page - 1, "one page is read, and its first database passed");
		assert!(!scan.done, "the list has a page still to read");
		assert!(walk.has_more());

		let mut reached = 1;
		while let Some(step) = walk.next(&txn).await? {
			if matches!(step, Step::Reached(_)) {
				reached += 1;
			}
		}
		assert_eq!(reached, page + 1, "every database is reached once");
		assert!(!walk.has_more());
		txn.cancel().await?;
		Ok(())
	}

	/// A page of a list holding only entries a newer release wrote is passed
	/// in a step of its own, so a pass charging every step reads one page a
	/// step however many such pages the list holds, and the definitions
	/// after them are reached.
	///
	/// The scan read on past such a page inside one step, so a pass over a
	/// catalog a newer release had written read the whole of it in a step it
	/// charged as one.
	#[tokio::test]
	async fn a_page_of_entries_a_newer_release_wrote_is_a_step_of_its_own() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let txn = ds.transaction(Write).await?;
		let db = txn.ensure_ns_db(None, "test", "readable").await?;
		// A page of databases this build cannot read, sorting before the one
		// it can.
		for n in 0..page {
			let key = DatabaseKey {
				ns: db.namespace_id,
				db: Cow::Owned(format!("db{n:04}")),
			}
			.encode_key()?;
			txn.set(key, with_future_floor(&[0x00])).await?;
		}
		txn.commit().await?;

		let txn = ds.transaction(Read).await?;
		let mut walk = CatalogWalk::default();
		assert!(
			matches!(walk.next(&txn).await?, Some(Step::Passed)),
			"entering the namespace is a step of its own"
		);
		assert!(
			matches!(walk.next(&txn).await?, Some(Step::Passed)),
			"the page of entries a newer release wrote is a step of its own"
		);
		let Some(Step::Reached((_, reached))) = walk.next(&txn).await? else {
			panic!("the database after them is reached");
		};
		assert_eq!(reached.name.as_str(), "readable");
		assert!(walk.next(&txn).await?.is_none());
		assert!(!walk.has_more());
		assert_eq!(walk.take_refused(), page, "every entry passed is counted");
		txn.cancel().await?;
		Ok(())
	}

	/// The probe reaches every database subtree that holds a key, once each
	/// and in id order, including one under a namespace whose definition a
	/// newer release wrote, which a walk over the catalog passes unnamed.
	#[tokio::test]
	async fn the_probe_reaches_a_database_the_walk_cannot_name() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let mut expected = Vec::new();
		for (ns, db) in [("a", "x"), ("a", "y"), ("b", "z")] {
			let txn = ds.transaction(Write).await?;
			let def = txn.ensure_ns_db(None, ns, db).await?;
			txn.commit().await?;
			expected.push((def.namespace_id, def.database_id));
			let session = Session::owner().with_ns(ns).with_db(db);
			let res = ds.execute("DEFINE TABLE t", &session, None).await?;
			assert!(res[0].result.is_ok(), "{res:?}");
		}
		expected.sort();
		let txn = ds.transaction(Write).await?;
		let key = NamespaceKey::new(Cow::Borrowed("b")).encode_key()?;
		txn.set(key, with_future_floor(&[0x00])).await?;
		txn.commit().await?;

		let txn = ds.transaction(Read).await?;
		let mut walk = CatalogWalk::default();
		let mut walked = Vec::new();
		while let Some(step) = walk.next(&txn).await? {
			if let Step::Reached((_, db)) = step {
				walked.push((db.namespace_id, db.database_id));
			}
		}
		assert_eq!(walked, expected[..2], "the walk names the databases it can read");
		assert_eq!(walk.take_refused(), 1, "and passes the namespace it cannot");

		let mut probe = DatabaseProbe::new()?;
		let mut reached = Vec::new();
		while let Some(step) = probe.next(&txn).await? {
			if let Step::Reached(ids) = step {
				reached.push(ids);
			}
		}
		assert_eq!(reached, expected, "the probe reaches every database subtree");
		txn.cancel().await?;
		Ok(())
	}

	/// A list scan ends at the last definition there was when its first page
	/// proved full, so a walk over a list that grows a page ahead of it ends,
	/// and the definitions added past that one are the next walk's.
	///
	/// The scan ended only on a short page, so a list that grew at least as
	/// fast as the scan read it never ended: a router lap stayed pinned to
	/// its watermark, and the changefeed and stalled-build sweeps never came
	/// back round to the databases they had visited.
	#[tokio::test]
	async fn a_list_scan_ends_at_the_tail_it_began_with() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let define = |from: usize| {
			let ds = &ds;
			async move {
				let txn = ds.transaction(Write).await?;
				for n in from..from + page + 1 {
					txn.ensure_ns_db(None, "test", &format!("db{n:04}")).await?;
				}
				txn.commit().await
			}
		};
		define(0).await?;

		// The first page, read in one pass's transaction, proves the list
		// spans pages and fixes the scan's end.
		let mut walk = CatalogWalk::default();
		let txn = ds.transaction(Read).await?;
		assert!(matches!(walk.next(&txn).await?, Some(Step::Passed)));
		assert!(matches!(walk.next(&txn).await?, Some(Step::Reached(_))));
		txn.cancel().await?;
		// A page and one of databases are added past the list's end.
		define(page + 1).await?;
		// The rest of the walk, in the next pass's transaction, ends at the
		// database the list ended in when the scan began.
		let txn = ds.transaction(Read).await?;
		let mut reached = 1;
		while let Some(step) = walk.next(&txn).await? {
			if matches!(step, Step::Reached(_)) {
				reached += 1;
			}
		}
		assert_eq!(reached, page + 1, "the databases added past the end are not this walk's");
		assert!(!walk.has_more());

		// They are the next walk's.
		let mut walk = CatalogWalk::default();
		let mut reached = 0;
		while let Some(step) = walk.next(&txn).await? {
			if matches!(step, Step::Reached(_)) {
				reached += 1;
			}
		}
		assert_eq!(reached, 2 * (page + 1));
		txn.cancel().await?;
		Ok(())
	}

	/// A namespace without a database is passed in a step of its own, so a
	/// caller charging every step bounds a pass over a catalog of them.
	///
	/// The walk stepped through every such namespace inside one call, reading
	/// page after page of the namespace list, so a pass over a catalog of
	/// empty namespaces read all of it before its caller could charge a thing.
	#[tokio::test]
	async fn an_empty_namespace_is_a_step_of_its_own() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		for ns in ["a", "b"] {
			ds.execute(&format!("DEFINE NAMESPACE {ns}"), &Session::owner(), None).await?;
		}
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;

		let txn = ds.transaction(Read).await?;
		let mut walk = CatalogWalk::default();
		let mut steps = Vec::new();
		while let Some(step) = walk.next(&txn).await? {
			steps.push(match step {
				Step::Reached((ns, db)) => format!("{ns}/{}", db.name.as_str()),
				Step::Passed => "-".to_string(),
			});
		}
		assert_eq!(steps, ["-", "-", "-", "test/test"], "a step per namespace, then the database");
		txn.cancel().await?;
		Ok(())
	}

	/// A database's tables are read a page at a time as the index walk passes
	/// them, so reaching a database costs at most a page of its table list
	/// however many tables it holds, and an index behind a page of tables is
	/// reached once they are passed.
	///
	/// The stalled-build scan listed every namespace, each namespace's
	/// databases, each database's tables and each table's indexes whole, so a
	/// pass over a database of many tables read them all, in a slice a host
	/// driving maintenance itself waits on.
	#[tokio::test]
	async fn a_database_s_tables_are_read_a_page_at_a_time() -> Result<()> {
		let ds = Datastore::new("memory").await?;
		let page = NORMAL_BATCH_SIZE as usize;
		let txn = ds.transaction(Write).await?;
		txn.ensure_ns_db(None, "test", "test").await?;
		txn.commit().await?;
		let owner = Session::owner().with_ns("test").with_db("test");
		// A page and one of tables, the last of them indexed.
		let mut sql = String::new();
		for n in 0..page + 1 {
			sql.push_str(&format!("DEFINE TABLE tb{n:04}; "));
		}
		sql.push_str(&format!("DEFINE INDEX i ON tb{page:04} FIELDS f;"));
		for res in ds.execute(&sql, &owner, None).await? {
			res.result?;
		}

		let txn = ds.transaction(Read).await?;
		let mut walk = IndexWalk::default();
		for level in ["namespace", "database", "first table"] {
			assert!(
				matches!(walk.next(&txn).await?, Some(Step::Passed)),
				"entering the {level} is a step of its own"
			);
		}
		let (_, scan) = walk.tables.as_ref().expect("the walk is in the database");
		assert_eq!(scan.page.len(), page - 1, "one page is read, and its first table entered");
		assert!(!scan.done, "the list has a page still to read");
		assert!(walk.has_more());

		let mut steps = 3;
		let mut reached = Vec::new();
		while let Some(step) = walk.next(&txn).await? {
			steps += 1;
			if let Step::Reached(index) = step {
				reached.push(index);
			}
		}
		assert_eq!(reached.len(), 1, "the one index is reached once");
		assert_eq!(reached[0].index.table_name.to_string(), format!("tb{page:04}"));
		assert_eq!(reached[0].ns_name.as_ref(), "test");
		assert_eq!(steps, page + 4, "a step per level entered and per index reached");
		assert!(!walk.has_more());
		txn.cancel().await?;
		Ok(())
	}
}
