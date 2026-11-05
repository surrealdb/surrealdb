//! The table migration job: rewrites the rows a table's migration steps have not reached, and
//! verifies whether the stored values of fields queued for verification conform to their
//! definitions.
//!
//! A statement that gives a table work writes a request at `/!tq`
//! ([`crate::key::table_migration::TableMigrationRequest`]) under an id of its own, in the
//! transaction that makes the change. The job keeps its progress through each table at `/!tm`
//! ([`TableMigrationState`]), a key no statement writes, so a statement and a page of the job never
//! write the same queue key and neither can fail the other's commit through it. The job holds one
//! lease ([`TaskLeaseType::TableMigration`]) for every table, visits each table with requests and
//! then each whose progress asks for work, and works through each table in laps of pages. A pass
//! runs at most [`SLICE_PAGES`] pages, and one that stops at them keeps its place in the queues
//! for the next ([`TableMigrationLap`]):
//!
//! - A page that finds requests folds them into the progress, in the order made, deletes them, and
//!   commits nothing else. A request committed after the page's snapshot is not deleted, so a later
//!   page folds it.
//! - Every other page runs in one write transaction. It reads the table's generations and steps
//!   from its own snapshot, then up to a page of records after the progress's cursor. A record
//!   stamped before the table's newest step is brought through the steps it has not passed, and,
//!   when the progress asks for a rewrite, stored again in the layout of the table's head
//!   generation, stamped with it. The page then advances the progress by compare-and-set on the
//!   state it read, so a page whose progress was replaced (by a node that took the lease over) or
//!   deleted meanwhile commits nothing. The page takes locked reads of the definitions of the
//!   table, its database and its namespace, so a removal of any of them that commits first fails
//!   the page's commit instead of leaving rows written beneath what was removed. No removal writes
//!   the progress or deletes a request, so a page that commits first fails a removal only through
//!   the rows it rewrote, which `REMOVE TABLE` deletes key by key in its own transaction. A removal
//!   therefore holds the job off the table until its transaction ends
//!   ([`Transaction::hold_table_for_removal`]): on the removing datastore at once
//!   ([`surrealdb_datastore::triggers::TableRemovals`]), and on every node through a hold recorded
//!   in the progress ([`crate::key::table_migration::RemovalHold`]), which a page committed after
//!   it fails on, since the page writes the progress by compare-and-set. A page of a rewrite is
//!   neither begun nor committed while either stands, and the table is left for a lap after the
//!   removal ends, which finds it gone or resumes it. A hold whose node is gone, or that has stood
//!   longer than any transaction lasts, lost its release, and the job takes it out. `REMOVE
//!   DATABASE` and `REMOVE NAMESPACE` delete only the definition and leave the rows to a background
//!   reclaim, so a page conflicts with neither.
//! - At the end of a lap the rewrite is done if the table's newest step is the one the lap began
//!   under. Otherwise another lap starts. A step minted after the last page's snapshot comes with a
//!   request that page did not delete, so the next page starts a lap for it. The steps stay in
//!   their generations' descriptors: a row stamped before one, such as one a transaction that began
//!   earlier committed after the lap passed it, is replayed whenever it is read.
//! - A verification judges each record's value, brought through any pending step, without writing
//!   it, against the test a migration step of the field would apply: its type with the definitions
//!   beneath it folded in, so a key no definition declares beneath an object of a `SCHEMAFULL`
//!   table fails it, and its `ASSERT` over `$value` ([`crate::exec::migrate::verification_test`]).
//!   A field whose values all conformed has its claim made at the end of the lap, when that test is
//!   the field's whole definition, the table's claims can state it
//!   ([`surrealdb_catalog::birth_claims_of`]), the rewrite is done, and the lap began after every
//!   transaction that could still commit an unjudged row had ended: on a store this datastore
//!   writes alone, the job waits for the transactions older than the fold of the request before the
//!   lap's first page; on any other store a pass reports its counts and never makes a claim. A
//!   field that did not conform withdraws the table's claims if one is on the definition the lap
//!   judged.
//! - The check for undeclared keys judges each record, brought through any pending step, by its
//!   top-level keys: one other than `id`, `in` and `out` that no definition's path begins with is a
//!   key a write of the row refuses ([`crate::exec::migrate::holds_undeclared_key`]). The check
//!   states no claim. It reports the rows it counted, unless the lap judged every row and counted
//!   none, which leaves nothing to report. A lap that has passed rows under names the table no
//!   longer declares starts over.
//!
//! The job writes only record keys, never through `put_record` or `set_record`, which would store
//! the record through the transaction's record cache. It runs no events, live queries or
//! changefeed writes: a rewrite changes what a row costs to read, never what it reads as.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use surrealdb_datastore::tx::refused_by_release;
use surrealdb_observe::TaskLeaseState;
use tokio_util::sync::CancellationToken;
use web_time::Instant;

use super::Datastore;
use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, NodeProvider, TableProvider};
use crate::catalog::{
	Conformance, DatabaseDefinition, DatabaseId, FieldCandidate, Generation, NamespaceDefinition,
	NamespaceId, TableLayouts, birth_claims_of,
};
use crate::exec::ExecutionContext;
use crate::exec::migrate::{Replay, into_error};
use crate::iam::{Auth, Role};
use crate::key::schema::{
	DatabaseKey, DatabasePrefix, NamespaceKey, NamespacePrefix, RecordKey, RecordPrefix, TableKey,
	TableMigrationKey, TableMigrationPrefix, TableMigrationRequestKey, TableMigrationRequestPrefix,
};
use crate::key::table_migration::{TableMigrationRequest, TableMigrationState};
use crate::key::{KVKeyDecode, Key, Resumable};
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::tasks::{Pass, Progress};
use crate::kvs::walk::ListScan;
use crate::kvs::{DatastoreError, Direction, Transaction, TransactionType};
use crate::val::{RecordId, TableName, Value};

/// The most records a page reads.
const PAGE_ROWS: u32 = 256;

/// The most queue entries read at once. A pass pages through each queue in batches of this size,
/// so an entry that asks for nothing never keeps a later one from being visited, and a page folds
/// at most this many of a table's requests.
const QUEUE_BATCH: u32 = 1024;

/// How long a pass waits between checks for the transactions older than a verification to end.
const SETTLE_POLL: Duration = Duration::from_millis(10);

/// How a pass over one table ended.
enum TableOutcome {
	/// The table has nothing left for this pass.
	Done,
	/// The pass's pages are spent with the table's work unfinished.
	Spent,
	/// Another node holds the job's lease.
	LeaseLost,
}

/// A table the job works on, with the names a catalog write addresses it by.
struct Target {
	ns: NamespaceId,
	db: DatabaseId,
	tb: TableName,
	ns_name: String,
	db_name: String,
}

impl Target {
	fn key(&self) -> TableMigrationKey<'_> {
		TableMigrationKey {
			ns: self.ns,
			db: self.db,
			tb: Cow::Borrowed(&self.tb),
		}
	}
}

/// Pages one table migration pass runs before it yields; see
/// [`crate::kvs::INDEX_COMPACTION_SLICE_BATCHES`]. A page is a transaction of a
/// table's work, folding its requests or rewriting or verifying up to [`PAGE_ROWS`]
/// of its rows, a batch of either queue read, or a page of the catalog's
/// namespace or database list read to name a table's database, so a pass costs a
/// bounded amount of work however many tables are queued, however many rows they
/// hold, and however large the catalog is.
const SLICE_PAGES: usize = 16;

/// How long the table migration job leaves a removal hold standing, from when it first saw it,
/// when the datastore sets no transaction timeout. A removal's transaction that outlasts this
/// loses the protection of its hold.
const REMOVAL_HOLD_LIMIT: Duration = Duration::from_secs(600);

/// How long past the datastore's transaction timeout the job leaves a removal hold standing, for
/// the transaction that set it to have ended and taken it out.
const REMOVAL_HOLD_MARGIN: Duration = Duration::from_secs(60);

/// How long the job leaves a removal hold standing before it takes the hold out as lost: past the
/// longest a transaction may last, or [`REMOVAL_HOLD_LIMIT`] where none is set.
fn removal_hold_limit(transaction_timeout: Option<Duration>) -> Duration {
	transaction_timeout.map_or(REMOVAL_HOLD_LIMIT, |timeout| timeout + REMOVAL_HOLD_MARGIN)
}

/// When the table migration job first saw each removal hold it found standing
/// ([`crate::key::table_migration::RemovalHold`]), on this datastore's own clock, so the age of a
/// hold is judged without comparing the clocks of the node that recorded it and of this one. A
/// hold first seen by a node that took the job's lease over is judged from then.
#[derive(Default)]
pub(super) struct RemovalHoldsSeen(parking_lot::Mutex<HashMap<uuid::Uuid, Instant>>);

impl RemovalHoldsSeen {
	/// Whether the hold `id` has stood for `limit` since it was first seen, recording it as seen
	/// now if it was not.
	fn stood(&self, id: uuid::Uuid, limit: Duration) -> bool {
		self.0.lock().entry(id).or_insert_with(Instant::now).elapsed() >= limit
	}

	/// Forget the holds first seen longer ago than `age`, which are long since taken out.
	fn forget_older_than(&self, age: Duration) {
		self.0.lock().retain(|_, seen| seen.elapsed() < age);
	}
}

/// Where the table migration job's walk of its queues stands between passes.
///
/// A lap visits each table with requests, then each whose progress asks for work,
/// each table once. A pass that spends its pages part-way through keeps the lap
/// here, with the table it stopped inside, which the next pass continues first:
/// a table's progress is durable, so the table resumes where its last committed
/// page left it. A pass that ends the lap, loses the lease, finds it elsewhere or
/// fails drops it, and the next pass begins a lap over the queues as they then
/// stand.
#[derive(Default)]
pub(super) struct TableMigrationLap {
	/// The table the last pass stopped inside, and whether its entry asked for work.
	current: Option<(TableRef, bool)>,
	/// The queue the lap is reading, with the last key it took from it.
	queue: LapQueue,
	/// The tables the lap has visited.
	visited: HashSet<TableRef>,
	/// The names of the databases the lap's tables are in.
	names: LapNames,
}

/// The names the databases of a lap's tables are addressed by, keyed by the ids
/// their queue keys name them by, and the lookup a pass stopped part-way through.
///
/// A queue key names a table's database by id, and the catalog lists namespaces
/// and databases by name, so a database's names are found by reading its
/// namespace's list and then the namespace's database list, a page of at most
/// [`crate::kvs::NORMAL_BATCH_SIZE`] definitions at a time, each page spending
/// one of the pass's budget. A lookup the budget stops is continued by the next
/// pass. Names found are a hint for the rest of the lap: each use reads the
/// database under them again, and names that no longer address the ids are
/// looked up afresh.
#[derive(Default)]
struct LapNames {
	/// The namespace and database names found, by database.
	found: HashMap<(NamespaceId, DatabaseId), (String, String)>,
	/// The databases found to be removed during this lap.
	gone: HashSet<(NamespaceId, DatabaseId)>,
	/// The lookup a pass stopped part-way through, and the database it is for.
	pending: Option<((NamespaceId, DatabaseId), Lookup)>,
}

/// A lookup of a database's names, part-way through the catalog's lists.
enum Lookup {
	/// Reading the namespace list for the namespace with the id.
	Namespace(ListScan<NamespaceDefinition>),
	/// Reading the database list of the namespace found, by name, for the
	/// database with the id.
	Database(String, ListScan<DatabaseDefinition>),
}

/// How a lookup of a database's names ended.
enum Names {
	/// The namespace's name and the database's.
	Found(String, String),
	/// The pass's budget ran out first; the lookup is kept for the next pass.
	Spent,
	/// Another node holds the job's lease.
	LeaseLost,
	/// No namespace or database has the ids: it was removed.
	Gone,
	/// Not found among the definitions this build reads, where the list held
	/// definitions a newer release wrote, which it may be among.
	Unread,
}

/// A table by the ids its queue keys name it by.
type TableRef = (NamespaceId, DatabaseId, TableName);

/// The queue a [`TableMigrationLap`] is reading, with the last key it took from
/// it: `None` at the queue's head.
enum LapQueue {
	Requests(Option<Vec<u8>>),
	Progress(Option<Vec<u8>>),
}

impl Default for LapQueue {
	fn default() -> Self {
		Self::Requests(None)
	}
}

/// How a pass's walk of the queues stopped.
enum LapStop {
	/// Both queues are read to their end.
	Ended,
	/// The pass's pages are spent with the lap unfinished.
	Spent,
	/// Another node holds the job's lease.
	LeaseLost,
}

impl Datastore {
	/// Work through every table queued for the table migration job, under the job's lease:
	/// rewrite the rows its steps have not reached, and verify the fields queued for
	/// verification. Runs [`Self::table_migration_pass`] until a pass finishes its lap.
	pub async fn table_migration(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<()> {
		while Self::table_migration_pass(Arc::clone(&dbs), interval, canceller.clone())
			.await?
			.progress == Progress::More
		{}
		Ok(())
	}

	/// One bounded pass of the table migration job: at most [`SLICE_PAGES`] pages of the
	/// lap [`TableMigrationLap`] holds, the tables with requests visited first, then the
	/// tables whose progress asks for work that the lap has not visited. Reports the tables
	/// worked through as its items, and [`Progress::More`] when it stopped at its pages
	/// with the lap unfinished, which the next pass continues. A pass that finds the lease
	/// held by another node, or finds it passed to one part-way, reports
	/// [`TaskLeaseState::Elsewhere`]: the holder works through the rest of the queue.
	pub(crate) async fn table_migration_pass(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
	) -> Result<Pass> {
		Self::table_migration_bounded(dbs, interval, canceller, SLICE_PAGES).await
	}

	/// One bounded pass of the table migration job, for a host that builds the datastore
	/// [`without_maintenance_tasks`](super::builder::Builder::without_maintenance_tasks) and
	/// drives the job itself: the pass [`Self::table_migration_pass`] runs, on the calling task.
	///
	/// - **Lease.** The pass takes the job's lease, which lasts two `interval`s, and reports
	///   [`TaskLeaseState::Elsewhere`] with nothing done while another node holds it. A host that
	///   runs a pass at least once every `interval` while work remains keeps the lease.
	/// - **Bounds.** At most [`SLICE_PAGES`] pages, each one transaction of a table's work (at most
	///   [`PAGE_ROWS`] rows), a batch of at most [`QUEUE_BATCH`] queue keys, or a page of the
	///   catalog's lists. A pass stopped anywhere, its future dropped included, loses at most its
	///   running page: progress is committed per page and the next pass resumes from it. A host
	///   cancels a pass by dropping its future, as with [`Self::process_async_events`] and
	///   [`Self::run_index_builds`].
	/// - **Progress.** [`Pass::items`] counts the tables the pass finished. [`Progress::More`]
	///   means the pass spent its pages with work left, and the host calls again;
	///   [`Progress::Done`] means the pass ended its lap: every table queued when it read the
	///   queues was visited, and each has nothing left that a pass can do now.
	/// - **Waits.** Nothing is spawned. A page that loses a commit conflict waits [`SETTLE_POLL`]
	///   on the engine's timer before the next. On a store this datastore writes alone, a
	///   verification first waits up to `interval` for the transactions opened through this
	///   datastore before it, counted until dropped, to end; one that waits it out leaves its table
	///   for a later pass, and the pass can report [`Progress::Done`] with that table's work left.
	///   On any other store it never waits.
	/// - **Wakes.** Every statement that gives a table work writes a key within
	///   [`Self::table_migration_request_bounds`] in its own commit, and
	///   [`CommitTriggers::table_migration`](surrealdb_datastore::triggers::CommitTriggers::table_migration)
	///   on [`Self::commit_triggers`] is notified after that commit. No other write gives the job
	///   work, so a host runs a pass on either signal and again while a pass reports
	///   [`Progress::More`]. On a store this datastore writes alone, a host also runs a pass at
	///   least once every `interval` whatever the last one reported: a verification that waited out
	///   `interval` is left for a later pass, and no write wakes the job for it.
	pub async fn run_table_migrations(self: &Arc<Self>, interval: Duration) -> Result<Pass> {
		Self::table_migration_pass(Arc::clone(self), interval, CancellationToken::new()).await
	}

	/// The key bounds `[start, end)` of the table migration job's requests: every statement that
	/// gives a table work writes a key within them in its own commit, and nothing else does.
	pub fn table_migration_request_bounds() -> Result<(Vec<u8>, Vec<u8>)> {
		let range = TableMigrationRequestPrefix {}.range()?;
		Ok((range.start().as_slice().to_vec(), range.end().as_slice().to_vec()))
	}

	/// [`Self::table_migration_pass`] with its page budget lowered, so a test can drive a
	/// pass that stops part-way through a table or a queue.
	#[cfg(test)]
	pub(crate) async fn table_migration_with_pages(
		dbs: Arc<Datastore>,
		interval: Duration,
		pages: usize,
	) -> Result<Pass> {
		Self::table_migration_bounded(dbs, interval, CancellationToken::new(), pages).await
	}

	async fn table_migration_bounded(
		dbs: Arc<Datastore>,
		interval: Duration,
		canceller: CancellationToken,
		pages: usize,
	) -> Result<Pass> {
		let lh = LeaseHandler::new_with_canceller(
			dbs.sequences.clone(),
			dbs.id,
			dbs.transaction_factory.clone(),
			TaskLeaseType::TableMigration,
			interval * 2,
			canceller.clone(),
		)?;
		if !lh.has_lease().await? {
			*dbs.table_migration_lap.lock() = TableMigrationLap::default();
			return Ok(Pass::elsewhere());
		}
		let mut pass = Pass::done(TaskLeaseState::Held);
		let mut budget = pages;
		// Taken for the pass, and put back only by a pass that stops at its pages.
		let mut lap = std::mem::take(&mut *dbs.table_migration_lap.lock());
		match Self::walk_lap(&dbs, &lh, &canceller, interval, &mut lap, &mut budget, &mut pass)
			.await?
		{
			LapStop::Spent => {
				*dbs.table_migration_lap.lock() = lap;
				pass.progress = Progress::More;
			}
			LapStop::Ended | LapStop::LeaseLost => {}
		}
		Ok(pass)
	}

	/// Continue `lap`: the table it stopped inside, then each queue from the key it last
	/// took, a batch of keys at a time, each batch and each page of a table's work spending
	/// one of `budget`.
	async fn walk_lap(
		dbs: &Arc<Datastore>,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		interval: Duration,
		lap: &mut TableMigrationLap,
		budget: &mut usize,
		pass: &mut Pass,
	) -> Result<LapStop> {
		if let Some((table, has_work)) = lap.current.take() {
			match Self::visit_table(
				dbs,
				lh,
				canceller,
				&table,
				has_work,
				&mut lap.names,
				interval,
				budget,
				pass,
			)
			.await?
			{
				TableOutcome::Done => {}
				TableOutcome::Spent => {
					lap.current = Some((table, has_work));
					return Ok(LapStop::Spent);
				}
				TableOutcome::LeaseLost => return Ok(LapStop::LeaseLost),
			}
		}
		loop {
			if *budget == 0 {
				return Ok(LapStop::Spent);
			}
			*budget -= 1;
			let requests = matches!(lap.queue, LapQueue::Requests(_));
			let (entries, full) = match &lap.queue {
				// The tables with requests, each once: a table's requests sort together.
				LapQueue::Requests(after) => {
					let mut range = TableMigrationRequestPrefix {}.range()?;
					if let Some(after) = after {
						range = range.resume_after(after, Direction::Forward);
					}
					let txn = dbs.transaction(TransactionType::Read).await?;
					let res = txn.keys(range, QUEUE_BATCH, 0, None).await;
					let _ = txn.cancel().await;
					let keys = res?;
					let full = keys.len() >= QUEUE_BATCH as usize;
					let entries = keys
						.into_iter()
						.map(|key| {
							let table = match TableMigrationRequestKey::decode_key(&key) {
								Ok(k) => Some(((k.ns, k.db, k.tb.into_owned()), true)),
								Err(e) => {
									warn!("Skipping undecodable table migration request: {e}");
									None
								}
							};
							(key, table)
						})
						.collect::<Vec<_>>();
					(entries, full)
				}
				// The tables whose progress asks for work, as a pass that stopped left them, and
				// those whose database or namespace was removed, whose progress is deleted on the
				// visit.
				LapQueue::Progress(after) => {
					let mut range = TableMigrationPrefix {}.range()?;
					if let Some(after) = after {
						range = range.resume_after(after, Direction::Forward);
					}
					let txn = dbs.transaction(TransactionType::Read).await?;
					let res = txn.scan(range, QUEUE_BATCH, 0, None).await;
					let _ = txn.cancel().await;
					let scanned = res?;
					let full = scanned.len() >= QUEUE_BATCH as usize;
					let entries = scanned
						.into_iter()
						.map(|(key, state)| {
							let table = match TableMigrationKey::decode_key(&key) {
								// A held entry is visited for its holds, so one whose release
								// was lost is taken out.
								Ok(k) => {
									let visit = state.has_work() || state.held();
									Some(((k.ns, k.db, k.tb.into_owned()), visit))
								}
								Err(e) => {
									warn!("Skipping undecodable table migration queue entry: {e}");
									None
								}
							};
							(key, table)
						})
						.collect::<Vec<_>>();
					(entries, full)
				}
			};
			for (key, entry) in entries {
				Self::ensure_not_cancelled(canceller)?;
				// The key is taken, so a pass that resumes the queue reads after it; a table
				// it stops inside is continued first.
				lap.queue = if requests {
					LapQueue::Requests(Some(key))
				} else {
					LapQueue::Progress(Some(key))
				};
				let Some((table, has_work)) = entry else {
					continue;
				};
				if !lap.visited.insert(table.clone()) {
					continue;
				}
				match Self::visit_table(
					dbs,
					lh,
					canceller,
					&table,
					has_work,
					&mut lap.names,
					interval,
					budget,
					pass,
				)
				.await?
				{
					TableOutcome::Done => {}
					TableOutcome::Spent => {
						lap.current = Some((table, has_work));
						return Ok(LapStop::Spent);
					}
					TableOutcome::LeaseLost => return Ok(LapStop::LeaseLost),
				}
			}
			if !full {
				if !requests {
					return Ok(LapStop::Ended);
				}
				lap.queue = LapQueue::Progress(None);
			}
		}
	}

	/// Work through `table`, as [`Self::migrate_table`] does, spending one of `budget` per
	/// page, once its database's names are found ([`Self::database_names`]). A table worked
	/// through counts as one of `pass`'s items; a table whose pass fails is logged and left for
	/// the next lap. A lease found held by another node is reported in `pass`. A table whose
	/// namespace or database is gone has its progress and requests deleted.
	#[allow(clippy::too_many_arguments)]
	async fn visit_table(
		dbs: &Arc<Datastore>,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		(ns, db, tb): &TableRef,
		has_work: bool,
		names: &mut LapNames,
		interval: Duration,
		budget: &mut usize,
		pass: &mut Pass,
	) -> Result<TableOutcome> {
		let (ns_name, db_name) =
			match Self::database_names(dbs, lh, canceller, (*ns, *db), names, budget).await? {
				Names::Found(ns_name, db_name) => {
					if !has_work {
						return Ok(TableOutcome::Done);
					}
					(ns_name, db_name)
				}
				Names::Spent => return Ok(TableOutcome::Spent),
				Names::LeaseLost => {
					pass.lease = TaskLeaseState::Elsewhere;
					return Ok(TableOutcome::LeaseLost);
				}
				Names::Gone => {
					Self::forget_table(dbs, *ns, *db, tb).await?;
					return Ok(TableOutcome::Done);
				}
				Names::Unread => {
					if has_work {
						warn!(
							"Table migration of `{tb}` is left: its database is not one this build reads"
						);
					}
					return Ok(TableOutcome::Done);
				}
			};
		let target = Target {
			ns: *ns,
			db: *db,
			tb: tb.clone(),
			ns_name,
			db_name,
		};
		match Self::migrate_table(dbs, lh, canceller, &target, interval, budget).await {
			Ok(TableOutcome::Done) => {
				pass.items += 1;
				Ok(TableOutcome::Done)
			}
			Ok(TableOutcome::LeaseLost) => {
				pass.lease = TaskLeaseState::Elsewhere;
				Ok(TableOutcome::LeaseLost)
			}
			Ok(TableOutcome::Spent) => Ok(TableOutcome::Spent),
			Err(e) => {
				if canceller.is_cancelled() {
					return Err(e);
				}
				// A conflict means another writer committed first; the table is visited again
				// on the next pass.
				if surrealdb_datastore::is_retryable_transaction_conflict(&e) {
					debug!(
						"Table migration of `{}` in {}/{} lost a conflict, retrying on the next pass: {e}",
						target.tb, target.ns_name, target.db_name
					);
					return Ok(TableOutcome::Done);
				}
				warn!(
					"Table migration of `{}` in {}/{} failed: {e}",
					target.tb, target.ns_name, target.db_name
				);
				Ok(TableOutcome::Done)
			}
		}
	}

	/// The names the database `(ns, db)` is addressed by: the names `names` found earlier in
	/// the lap, while the database under them still has the ids, and otherwise those a lookup
	/// reads from the catalog's lists, a page per one of `budget`, continuing the lookup a pass
	/// stopped part-way through for the same database. The lease is renewed before every
	/// page.
	async fn database_names(
		dbs: &Datastore,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		(ns, db): (NamespaceId, DatabaseId),
		names: &mut LapNames,
		budget: &mut usize,
	) -> Result<Names> {
		if names.gone.contains(&(ns, db)) {
			return Ok(Names::Gone);
		}
		if let Some((ns_name, db_name)) = names.found.get(&(ns, db)) {
			let txn = dbs.transaction(TransactionType::Read).await?;
			let current = txn.get_db_by_name(ns_name, db_name, None).await;
			let _ = txn.cancel().await;
			match current {
				Ok(Some(def)) if def.namespace_id == ns && def.database_id == db => {
					return Ok(Names::Found(ns_name.clone(), db_name.clone()));
				}
				Ok(_) => {}
				Err(e) if refused_by_release(&e) => {}
				Err(e) => return Err(e),
			}
			names.found.remove(&(ns, db));
		}
		let mut lookup = match names.pending.take() {
			Some((of, lookup)) if of == (ns, db) => lookup,
			_ => Lookup::Namespace(ListScan::new(NamespacePrefix {}.range()?)),
		};
		loop {
			Self::ensure_not_cancelled(canceller)?;
			if *budget == 0 {
				names.pending = Some(((ns, db), lookup));
				return Ok(Names::Spent);
			}
			*budget -= 1;
			if !lh.try_maintain_lease().await? {
				return Ok(Names::LeaseLost);
			}
			let txn = dbs.transaction(TransactionType::Read).await?;
			let read = match &mut lookup {
				Lookup::Namespace(scan) => scan.next_page(&txn).await.map(|page| {
					page.map(|page| {
						page.into_iter().find(|n| n.namespace_id == ns).map(|n| n.name.to_string())
					})
				}),
				Lookup::Database(_, scan) => scan.next_page(&txn).await.map(|page| {
					page.map(|page| {
						page.into_iter().find(|d| d.database_id == db).map(|d| d.name.to_string())
					})
				}),
			};
			let _ = txn.cancel().await;
			match (read?, lookup) {
				(Some(Some(ns_name)), Lookup::Namespace(_)) => {
					lookup = Lookup::Database(
						ns_name,
						ListScan::new(
							DatabasePrefix {
								ns,
							}
							.range()?,
						),
					);
				}
				(Some(Some(db_name)), Lookup::Database(ns_name, _)) => {
					names.found.insert((ns, db), (ns_name.clone(), db_name.clone()));
					return Ok(Names::Found(ns_name, db_name));
				}
				// The page did not hold it; the next may.
				(Some(None), unfinished) => lookup = unfinished,
				// The list ended without it.
				(None, Lookup::Namespace(mut scan)) => {
					return Ok(Self::not_found(&mut scan, (ns, db), names));
				}
				(None, Lookup::Database(_, mut scan)) => {
					return Ok(Self::not_found(&mut scan, (ns, db), names));
				}
			}
		}
	}

	/// How a lookup whose list `scan` ended without the definition sought ended: the
	/// definition is gone, unless the list held definitions this build skipped because a
	/// newer release wrote them.
	fn not_found<T: crate::key::KVValue<KeyContext = ()>>(
		scan: &mut ListScan<T>,
		id: (NamespaceId, DatabaseId),
		names: &mut LapNames,
	) -> Names {
		if scan.take_refused() > 0 {
			Names::Unread
		} else {
			names.gone.insert(id);
			Names::Gone
		}
	}

	/// Delete the progress of the table `(ns, db, tb)`, whose database is gone, and up to
	/// [`QUEUE_BATCH`] of its requests; a table with requests left is reached again by the
	/// next lap, which deletes the next batch.
	async fn forget_table(
		dbs: &Datastore,
		ns: NamespaceId,
		db: DatabaseId,
		tb: &TableName,
	) -> Result<()> {
		let txn = dbs.transaction(TransactionType::Write).await?;
		let key = TableMigrationKey {
			ns,
			db,
			tb: Cow::Borrowed(tb),
		};
		let deleted = async {
			txn.del_key(&key).await?;
			for (request, _) in txn.tb_migration_requests(ns, db, tb, QUEUE_BATCH).await? {
				txn.del(Key::from(request)).await?;
			}
			Ok::<_, anyhow::Error>(())
		}
		.await;
		match deleted {
			Ok(()) => txn.commit().await,
			Err(e) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Work through `target` in pages until its progress asks for nothing more and it has no
	/// request left, the lease is lost, a verification must wait for older transactions longer
	/// than one interval, or `budget` is spent, each page spending one.
	async fn migrate_table(
		dbs: &Arc<Datastore>,
		lh: &LeaseHandler,
		canceller: &CancellationToken,
		target: &Target,
		interval: Duration,
		budget: &mut usize,
	) -> Result<TableOutcome> {
		let mut page = PAGE_ROWS;
		loop {
			Self::ensure_not_cancelled(canceller)?;
			if *budget == 0 {
				return Ok(TableOutcome::Spent);
			}
			*budget -= 1;
			if !lh.try_maintain_lease().await? {
				return Ok(TableOutcome::LeaseLost);
			}
			let txn = Arc::new(dbs.transaction(TransactionType::Write).await?);
			let progress = txn.get_key(&target.key(), None).await?;
			let requests =
				txn.tb_migration_requests(target.ns, target.db, &target.tb, QUEUE_BATCH).await?;
			// A held entry is visited for its holds, so one whose release was lost is taken out.
			if requests.is_empty()
				&& !progress.as_ref().is_some_and(|state| state.has_work() || state.held())
			{
				let _ = txn.cancel().await;
				return Ok(TableOutcome::Done);
			}
			if txn.get_tb(target.ns, target.db, &target.tb, None).await?.is_none() {
				// The table is gone: nothing is left to rewrite or verify.
				for (request, _) in &requests {
					txn.del(Key::from(request.as_slice())).await?;
				}
				if let Some(progress) = &progress {
					txn.del_compare_key(&target.key(), Some(progress)).await?;
				}
				if Self::page_committed(&txn).await? {
					return Ok(TableOutcome::Done);
				}
				continue;
			}
			if !requests.is_empty() {
				// The requests are folded in a page of their own, so a verification a request
				// asks for is in the progress before the lap waits for the transactions older
				// than it.
				match Self::fold_requests(&txn, target, progress.as_ref(), &requests).await {
					Ok(()) => {
						Self::page_committed(&txn).await?;
					}
					// Another node's job replaced the progress; the next page reads it again.
					Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
						let _ = txn.cancel().await;
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				}
				continue;
			}
			let Some(state) = progress else {
				let _ = txn.cancel().await;
				return Ok(TableOutcome::Done);
			};
			if state.held() {
				// A removal of the table is under way: its rows are left alone until it ends, and
				// the end of the removal's hold, or of the table, brings the job back.
				let stale = catch!(txn, Self::stale_holds(dbs, &txn, &state).await);
				if stale.is_empty() {
					// A verification reads rows and writes none, so it goes on beside the removal.
					if state.rewrite || !state.has_work() {
						let _ = txn.cancel().await;
						return Ok(TableOutcome::Done);
					}
				} else {
					warn!(
						"Table migration of `{}` takes out {} removal hold(s) whose release was lost",
						target.tb,
						stale.len()
					);
					let mut kept = state.clone();
					kept.holds.retain(|hold| !stale.contains(&hold.id));
					let written = if kept.is_spent() {
						txn.del_compare_key(&target.key(), Some(&state)).await
					} else {
						txn.put_compare_key(&target.key(), &kept, Some(&state)).await
					};
					match written {
						Ok(()) => {
							Self::page_committed(&txn).await?;
						}
						// The progress moved meanwhile; the next page reads it again.
						Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
							let _ = txn.cancel().await;
						}
						Err(e) => {
							let _ = txn.cancel().await;
							return Err(e);
						}
					}
					continue;
				}
			}
			if state.verifying() && !state.settled && txn.sole_writer() {
				// The lap judges rows only once no transaction that began before the
				// verification was folded can still commit one behind its cursor. Every row
				// committed afterwards was written by a transaction that saw the new
				// definition, and coerced to it.
				let _ = txn.cancel().await;
				// The page's own transaction is older than the wait: it is counted out only once
				// dropped.
				drop(txn);
				if !Self::older_transactions_ended(dbs, interval).await {
					return Ok(TableOutcome::Done);
				}
				// A request committed during the wait is left for the next page, which folds it
				// into the settled progress: a verification it asks for is unsettled again.
				let txn = dbs.transaction(TransactionType::Write).await?;
				let settled = TableMigrationState {
					settled: true,
					..state.clone()
				};
				match txn.put_compare_key(&target.key(), &settled, Some(&state)).await {
					Ok(()) => {
						Self::page_committed(&txn).await?;
					}
					// The progress moved meanwhile; the next page reads it again.
					Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
						let _ = txn.cancel().await;
					}
					Err(e) => {
						let _ = txn.cancel().await;
						return Err(e);
					}
				}
				continue;
			}
			let table = (target.ns, target.db, target.tb.clone());
			let removals = dbs.commit_triggers().table_removals();
			if state.rewrite && removals.removing(&table) {
				// A transaction open through this datastore is removing the table; its removal
				// wakes the job when it ends, and a lap after that finds the table gone or
				// resumes it.
				let _ = txn.cancel().await;
				return Ok(TableOutcome::Done);
			}
			if !catch!(txn, Self::fence(&txn, target).await) {
				// The database or namespace was removed since the visit began; the next visit
				// finds it gone and deletes the progress.
				let _ = txn.cancel().await;
				return Ok(TableOutcome::Done);
			}
			let outcome = Self::migration_page(dbs, &txn, target, &state, page).await;
			match outcome {
				Ok(more) => {
					match Self::commit_unless_removing(&txn, &state, removals, &table).await {
						Ok(false) => return Ok(TableOutcome::Done),
						Ok(true) => {
							if !more {
								return Ok(TableOutcome::Done);
							}
							page = (page * 2).min(PAGE_ROWS);
						}
						Err(e) if surrealdb_datastore::is_retryable_transaction_conflict(&e) => {
							// A user write of a row the page rewrote, or a change to the definition
							// of the table, its database or its namespace, beat the page; take a
							// fresh snapshot with a smaller page.
							page = (page / 2).max(1);
							common::time::sleep(SETTLE_POLL).await;
						}
						Err(e) => return Err(e),
					}
				}
				// The progress moved under the page: a node that took the lease over replaced it.
				// The next page reads it again.
				Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
					let _ = txn.cancel().await;
				}
				Err(e) => {
					let _ = txn.cancel().await;
					return Err(e);
				}
			}
			yield_now!();
		}
	}

	/// Commit `txn`, a page that writes only the job's own keys, answering whether it applied.
	///
	/// A page that loses a retryable conflict to another writer (a user statement, or a node that
	/// took the lease over) applied nothing; it is answered `false` after a short wait, and the
	/// next page reads the state afresh. The job never makes a writer that beat it fail.
	async fn page_committed(txn: &Transaction) -> Result<bool> {
		match Self::commit_page(txn).await {
			Ok(()) => Ok(true),
			Err(e) if surrealdb_datastore::is_retryable_transaction_conflict(&e) => {
				common::time::sleep(SETTLE_POLL).await;
				Ok(false)
			}
			Err(e) => Err(e),
		}
	}

	/// The ids of the holds in `state` whose release was lost: the removal's node is archived, or
	/// is not in the cluster where this datastore's own node is (a store whose datastores register
	/// no nodes judges by age alone), or the hold has stood, since this datastore first saw it,
	/// longer than any removal's transaction lasts ([`removal_hold_limit`]). A removal that
	/// outlasts the limit is not held off any longer, and loses its commit to a page as it would
	/// with no hold.
	async fn stale_holds(
		dbs: &Datastore,
		txn: &Transaction,
		state: &TableMigrationState,
	) -> Result<Vec<uuid::Uuid>> {
		let limit = removal_hold_limit(dbs.transaction_timeout());
		let mut stale = Vec::new();
		let registered = txn.get_node(dbs.id()).await?.is_some();
		for hold in &state.holds {
			let node_gone = match txn.get_node(hold.node).await? {
				Some(node) => node.is_archived(),
				None => registered,
			};
			if node_gone || dbs.removal_holds_seen.stood(hold.id, limit) {
				stale.push(hold.id);
			}
		}
		dbs.removal_holds_seen.forget_older_than(limit * 2);
		Ok(stale)
	}

	/// Commit `txn`, a page of the lap `state` describes, as [`Self::commit_page`] does. A page of
	/// a rewrite is instead cancelled when a transaction open through this datastore is removing
	/// the table. Answers whether it was committed. No removal of the table is registered while
	/// the commit is under way ([`surrealdb_datastore::triggers::TableRemovals`]).
	async fn commit_unless_removing(
		txn: &Transaction,
		state: &TableMigrationState,
		removals: &surrealdb_datastore::triggers::TableRemovals,
		table: &surrealdb_datastore::triggers::FencedTable,
	) -> Result<bool> {
		if !state.rewrite {
			return Self::commit_page(txn).await.map(|()| true);
		}
		let Some(_gate) = removals.admit(table).await else {
			let _ = txn.cancel().await;
			return Ok(false);
		};
		Self::commit_page(txn).await.map(|()| true)
	}

	/// Take locked reads of the definitions of `target`'s namespace, database and table in `txn`,
	/// so a change to any of them that commits after `txn`'s snapshot fails `txn`'s commit.
	/// Answers whether all three stand under the ids `target` names.
	async fn fence(txn: &Transaction, target: &Target) -> Result<bool> {
		let ns = txn.getu_key(&NamespaceKey::new(Cow::Borrowed(&target.ns_name))).await?;
		let db = txn.getu_key(&DatabaseKey::new(target.ns, Cow::Borrowed(&target.db_name))).await?;
		let tb = txn
			.getu_key_raw(&TableKey {
				ns: target.ns,
				db: target.db,
				tb: Cow::Borrowed(&target.tb),
			})
			.await?;
		Ok(ns.is_some_and(|ns| ns.namespace_id == target.ns)
			&& db.is_some_and(|db| db.database_id == target.db)
			&& tb.is_some())
	}

	/// Fold `requests`, the first of `target`'s in the order made, into `progress`, the progress
	/// `txn` read, and delete them, leaving the writes for the caller to commit. The progress is
	/// written by compare-and-set on `progress`, and deleted when the fold leaves none.
	async fn fold_requests(
		txn: &Transaction,
		target: &Target,
		progress: Option<&TableMigrationState>,
		requests: &[(Vec<u8>, TableMigrationRequest)],
	) -> Result<()> {
		let folded =
			requests.iter().fold(progress.cloned(), |state, (_, request)| request.apply(state));
		for (request, _) in requests {
			txn.del(Key::from(request.as_slice())).await?;
		}
		match (&folded, progress) {
			(Some(folded), _) => txn.put_compare_key(&target.key(), folded, progress).await,
			(None, Some(progress)) => txn.del_compare_key(&target.key(), Some(progress)).await,
			(None, None) => Ok(()),
		}
	}

	/// Commit a page; a page whose progress moved under it commits nothing and is not an error,
	/// since the next page reads the progress again.
	async fn commit_page(txn: &Transaction) -> Result<()> {
		match txn.commit().await {
			Ok(()) => Ok(()),
			Err(e) if crate::kvs::is_conditional_write_conflict(&e) => {
				let _ = txn.cancel().await;
				Ok(())
			}
			Err(e) => {
				let _ = txn.cancel().await;
				Err(e)
			}
		}
	}

	/// Wait, for at most `limit`, until every transaction opened through this datastore before
	/// the call has ended.
	async fn older_transactions_ended(dbs: &Datastore, limit: Duration) -> bool {
		let live = dbs.commit_triggers().live_transactions();
		let deadline = Instant::now() + limit;
		let bucket = loop {
			if let Some(bucket) = live.advance() {
				break bucket;
			}
			if Instant::now() >= deadline {
				return false;
			}
			common::time::sleep(SETTLE_POLL).await;
		};
		while !live.drained(bucket) {
			if Instant::now() >= deadline {
				return false;
			}
			common::time::sleep(SETTLE_POLL).await;
		}
		true
	}

	/// The context a page evaluates migration steps in: the table's database, privileged, with no
	/// session.
	async fn migration_context(
		dbs: &Datastore,
		txn: &Arc<Transaction>,
		target: &Target,
	) -> Result<ExecutionContext> {
		dbs.privileged_database_context(txn, &target.ns_name, &target.db_name).await
	}

	/// The context a privileged evaluation over `txn` runs in: database `db_name` of namespace
	/// `ns_name`, with the root owner's rights, permissions suppressed and no session. Both must
	/// be defined.
	pub(crate) async fn privileged_database_context(
		&self,
		txn: &Arc<Transaction>,
		ns_name: &str,
		db_name: &str,
	) -> Result<ExecutionContext> {
		let mut ctx = self.setup_ctx()?;
		ctx.set_transaction(Arc::clone(txn));
		let ctx = ctx.freeze();
		let ns = txn.expect_ns_by_name(ns_name).await?;
		let db = txn.expect_db_by_name(ns_name, db_name).await?;
		let opt = crate::dbs::Options::new(&self.config.exec)
			.with_ns(Some(ns_name.into()))
			.with_db(Some(db_name.into()))
			.with_auth(Arc::new(Auth::for_root(Role::Owner)));
		Ok(ExecutionContext::privileged_database(ctx, opt, ns, db))
	}

	/// Run one page of `target`'s lap in `txn`, which `state` was read in, leaving the advanced
	/// progress written for the caller to commit. Answers whether the progress still asks for
	/// work.
	async fn migration_page(
		dbs: &Datastore,
		txn: &Arc<Transaction>,
		target: &Target,
		state: &TableMigrationState,
		page: u32,
	) -> Result<bool> {
		let exec = Self::migration_context(dbs, txn, target).await?;
		let layouts = txn.table_layouts(target.ns, target.db, &target.tb, None).await?;
		let replay = Replay::of(&layouts, target.ns, target.db);
		let fields = txn.all_tb_fields(target.ns, target.db, &target.tb, None).await?;
		let mut next = state.clone();
		// The check for undeclared keys judges rows against the names the table declares at the
		// top level now. Rows a lap passed under other names were judged against names that no
		// longer stand, so the lap starts over.
		let declared = crate::exec::migrate::declared_roots(fields.iter());
		if next
			.undeclared
			.as_ref()
			.is_some_and(|check| !check.finished && check.declared != declared)
		{
			if next.cursor.is_some() {
				next.restart();
			}
			if let Some(check) = next.undeclared.as_mut() {
				check.declared = declared;
			}
		}
		if next.lap == 0 {
			next.lap = 1;
		}
		// The lap's target is read on its first page, and only there. `0` is a target too (no
		// step stood when the lap began), so a step minted while the lap is under way leaves the
		// target behind the table's newest step and the lap's end starts another: the rows this
		// lap passed before the step was minted are rewritten by the next.
		if next.cursor.is_none() {
			next.target = newest_step(&layouts);
		}
		// The definitions of the fields under verification, by the position of each in the
		// progress; a field whose definition changed since the pass started is dropped from it, as
		// the definition is no longer the one the pass judges.
		next.verify.retain(|verified| {
			verified.finished
				|| fields.iter().any(|fd| {
					fd.name.to_raw_string() == verified.name
						&& crate::exec::migrate::definition_digest(fd)
							.is_ok_and(|digest| digest == verified.definition)
				})
		});
		// For each field under verification, the key a row holds its value under, what the value
		// is judged against, and whether that is the field's whole definition
		// ([`crate::exec::migrate::verification_test`]). The key is the field's name itself: the
		// canonical text `verified.name` holds backticks a name that would not parse bare, and
		// is the key of no row.
		let schemafull = txn
			.get_tb(target.ns, target.db, &target.tb, None)
			.await?
			.is_some_and(|tb| tb.schemafull);
		let mut tests = Vec::with_capacity(next.verify.len());
		for verified in &next.verify {
			let field = fields.iter().find(|fd| fd.name.to_raw_string() == verified.name);
			tests.push(match field.and_then(|field| Some((field, field.name.field_name()?))) {
				Some((field, key)) if !verified.finished && field.field_kind.is_some() => Some((
					key.to_owned(),
					crate::exec::migrate::verification_test(&exec, &fields, field, schemafull)
						.await
						.map_err(into_error)?,
				)),
				_ => None,
			});
		}
		let frame = crate::exec::migrate::migration_frame(&exec);

		let mut range = RecordPrefix {
			ns: target.ns,
			db: target.db,
			tb: Cow::Borrowed(&target.tb),
		}
		.range()?;
		if let Some(cursor) = &next.cursor {
			range = range.resume_after(cursor, Direction::Forward);
		}
		let rows = txn.scan_raw(range, page.max(1), 0, None).await?;
		if rows.is_empty() {
			let whole: Vec<bool> = tests
				.iter()
				.map(|test| test.as_ref().is_some_and(|(_, (_, whole))| *whole))
				.collect();
			return Self::end_lap(txn, target, state, next, &layouts, &fields, &whole).await;
		}
		let writer = if next.rewrite {
			Some(txn.record_writer(target.ns, target.db, &target.tb).await?)
		} else {
			None
		};
		for (key, value) in &rows {
			next.scanned += 1;
			let decoded = RecordKey::decode_key(key)?;
			let rid = RecordId {
				table: decoded.tb.into_owned(),
				key: decoded.id.into_owned(),
			};
			let (mut record, stamp) = txn
				.decode_stamped_record(target.ns, target.db, &target.tb, value, rid.clone(), None)
				.await?;
			if let Some(replay) = replay.as_ref().filter(|replay| replay.behind(stamp)) {
				let fallbacks = replay
					.apply_counted(&exec, &mut record.data, stamp)
					.await
					.map_err(into_error)?;
				if let Some(writer) = &writer {
					let bytes =
						writer.encode(record.metadata.as_ref(), &record.data, Some(&rid))?;
					txn.set(Key::from(key.as_slice()), bytes).await?;
					txn.forget_record(target.ns, target.db, &target.tb, &rid.key);
					next.rewritten += 1;
					next.fallbacks += fallbacks;
				}
			}
			// A row that is not an object has no fields to judge.
			let Value::Object(object) = &record.data else {
				continue;
			};
			for (verified, test) in next.verify.iter_mut().zip(&tests) {
				if verified.finished {
					continue;
				}
				let Some((key, (test, _))) = test else {
					continue;
				};
				// An object that holds no value at the field is judged as `NONE`: the report
				// lists the rows a later write would refuse, and a field whose type refuses `NONE`
				// states no claim over one.
				let value = object.get(key.as_str()).unwrap_or(&Value::None);
				if !test.conforms(&frame, value).await.map_err(into_error)? {
					verified.failures += 1;
				}
			}
			if let Some(check) = next.undeclared.as_mut().filter(|check| !check.finished)
				&& crate::exec::migrate::holds_undeclared_key(object, &check.declared)
			{
				check.failures += 1;
			}
		}
		next.cursor = rows.last().map(|(key, _)| key.clone());
		txn.put_compare_key(&target.key(), &next, Some(state)).await?;
		Ok(true)
	}

	/// End `target`'s lap in `txn`: stop rewriting when the lap's target is still the newest
	/// step, or start another lap; make the claim of each verified field that conformed
	/// throughout, where the lap could judge every row against the field's whole definition
	/// (`whole`, by the position of each in the progress) and the rewrite is done, and drop a
	/// field that conformed even where the table's claims cannot state it; withdraw the table's
	/// claims when a field that did not conform holds one on the definition the lap judged; drop
	/// the check for undeclared keys when the lap judged every row and counted none, and finish it
	/// with its count otherwise; and delete the progress once it asks for nothing and reports
	/// nothing.
	///
	/// The rewrite is judged done from the generations `txn` read. A step minted by a transaction
	/// that commits after that read comes with a request this page does not delete, so the next
	/// page starts another lap for it.
	#[allow(clippy::too_many_arguments)]
	async fn end_lap(
		txn: &Arc<Transaction>,
		target: &Target,
		state: &TableMigrationState,
		mut next: TableMigrationState,
		layouts: &TableLayouts,
		fields: &[crate::catalog::FieldDefinition],
		whole: &[bool],
	) -> Result<bool> {
		let mut again = false;
		if next.rewrite {
			if newest_step(layouts) == next.target {
				next.rewrite = false;
			} else {
				again = true;
			}
		}
		// The lap judged every row the table can hold: it began after every transaction that
		// could still commit one behind its cursor had ended, and no newer step is left to bring
		// the rows through.
		let judged_all = next.settled && txn.sole_writer() && !again;
		// A claim is stored in the stored grammar, which a store still at the legacy feature level
		// does not write.
		let promotable = judged_all && !txn.write_level().is_legacy();
		let claimable = if promotable && next.verify.iter().any(|f| !f.finished && f.failures == 0)
		{
			claimable_fields(fields)
		} else {
			Vec::new()
		};
		// A row the lap found not to conform refutes any claim on the definition it judged. The
		// claims are withdrawn before this lap's promotions, which a withdrawal would undo.
		if !again {
			let held = txn.table_conformance(target.ns, target.db, &target.tb, None).await?;
			let refuted = held.is_some_and(|held| {
				next.verify.iter().any(|verified| {
					!verified.finished
						&& verified.failures > 0
						&& held
							.claim(&verified.name)
							.is_some_and(|claim| claim.definition == verified.definition)
				})
			});
			if refuted {
				txn.forfeit_conformance(target.ns, target.db, &target.tb).await?;
			}
		}
		let mut kept = Vec::with_capacity(next.verify.len());
		let verify = std::mem::take(&mut next.verify);
		for (at, mut verified) in verify.into_iter().enumerate() {
			if verified.finished || again {
				kept.push(verified);
				continue;
			}
			if promotable && verified.failures == 0 {
				// The lap judged every row and none failed: the field conforms. It states a claim
				// when the lap judged its whole definition and the table's claims can state it;
				// either way nothing is left to report.
				if whole.get(at).copied().unwrap_or(false)
					&& claimable.iter().any(|(name, digest)| {
						*name == verified.name && *digest == verified.definition
					}) {
					match txn
						.promote_conformance(
							target.ns,
							target.db,
							&target.tb,
							&verified.name,
							verified.definition,
							Conformance::Guaranteed,
						)
						.await
					{
						Ok(()) => {}
						// The definition changed after the pass read it: nothing is claimed.
						Err(e)
							if e.downcast_ref::<DatastoreError>().is_some_and(|e| {
								matches!(e, DatastoreError::ConformanceStale { .. })
							}) => {}
						Err(e) => return Err(e),
					}
				}
				continue;
			}
			verified.finished = true;
			kept.push(verified);
		}
		next.verify = kept;
		// The check for undeclared keys states no claim: a lap that judged every row and counted
		// none leaves nothing to report.
		if !again && let Some(check) = next.undeclared.as_mut().filter(|check| !check.finished) {
			check.finished = true;
			if judged_all && check.failures == 0 {
				next.undeclared = None;
			}
		}
		if again {
			next.restart();
		}
		if next.is_spent() {
			txn.del_compare_key(&target.key(), Some(state)).await?;
			return Ok(false);
		}
		if !again {
			// The lap is over; a finished verification keeps its counts for `INFO`, and the
			// cursor starts the next lap from the first record.
			next.cursor = None;
		}
		txn.put_compare_key(&target.key(), &next, Some(state)).await?;
		Ok(next.has_work())
	}
}

/// The newest generation of the table's ring that carries a migration step, as the number the
/// progress records; `0` when none does.
fn newest_step(layouts: &TableLayouts) -> u16 {
	layouts.newest_migration().map_or(0, Generation::get)
}

/// The fields of a table, by name with the digest of their definitions, that the table's claims
/// can state conform ([`birth_claims_of`]): none when a definition cannot be compiled. A
/// component of a named record id is read from the key and has no stored form, so it is not a
/// candidate.
fn claimable_fields(fields: &[crate::catalog::FieldDefinition]) -> Vec<(String, i64)> {
	let stored: Vec<_> = fields
		.iter()
		.filter(|fd| !fd.is_key_part())
		.map(|fd| (fd.name.to_raw_string(), fd.to_stored()))
		.collect();
	let mut compiled = Vec::with_capacity(stored.len());
	for (name, definition) in &stored {
		let Some(candidate) = FieldCandidate::compile(name, definition) else {
			return Vec::new();
		};
		compiled.push(candidate);
	}
	let candidates: Vec<&FieldCandidate> = compiled.iter().collect();
	birth_claims_of(&candidates).into_iter().map(|claim| (claim.field, claim.definition)).collect()
}
