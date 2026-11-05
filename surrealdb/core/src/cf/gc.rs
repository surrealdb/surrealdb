use std::borrow::Cow;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use surrealdb_datastore::tx::refused_by_release;
use surrealdb_kvs::TransactionType::Read;
use tokio_util::sync::CancellationToken;

use crate::catalog::providers::DatabaseProvider;
use crate::catalog::{DatabaseDefinition, DatabaseId, NamespaceId, StoredTableDefinition};
use crate::cf::TableMutations;
use crate::key::TypedRange;
use crate::key::schema::{ChangeFeedPrefix, ChangefeedFenceKey, TablePrefix};
use crate::kvs::paging::{PageCompanion, PagedDelete, delete_sharing_budget};
use crate::kvs::tasklease::LeaseHandler;
use crate::kvs::walk::{CatalogWalk, DatabaseProbe, ListScan, Step};
use crate::kvs::{CHANGEFEED_GC_BATCH_SIZE, Datastore, Transaction};

/// The walk a changefeed-collection sweep makes over the catalog, a page of
/// steps per pass, discovering each database's retention as it reaches it.
///
/// A step is a step of the [`CatalogWalk`] — a namespace entered or a
/// database reached, the latter with the first page of its tables read when the
/// sweep reads retentions (see [`SweepReads`]) — or a further page of the
/// tables of the database being discovered, so a pass
/// that takes a page of steps reads at most a page of each catalog list it
/// moves through, however many namespaces hold no database and however many
/// tables a database holds. A database whose tables span passes is kept in
/// discovery between them.
///
/// A walk that passed a namespace or database definition a newer release
/// wrote ends with a [`DatabaseProbe`] of the keyspace, a step a database
/// subtree, since such a definition hides the id its subtree is addressed by;
/// see [`Self::take_unknown`].
#[derive(Default)]
pub(crate) struct ChangefeedSweep {
	walk: CatalogWalk,
	/// The database whose tables the sweep is reading; `None` when the next
	/// step is the walk's.
	discovery: Option<Discovery>,
	/// The tables skipped, with the database holding them, because a newer
	/// release wrote their definitions, since the count was last taken.
	skipped: usize,
	/// The databases reported by id since they were last taken, for a
	/// collector whose window is the datastore's rather than the catalog's:
	/// each database skipped whole because it holds a table definition a newer
	/// release wrote (see [`Self::discover`]), and each database subtree the
	/// probe reaches, readable or not.
	unknown: Vec<(NamespaceId, DatabaseId)>,
	/// Whether the sweep owes, is making, or has made a probe of the keyspace.
	probe: Probe,
}

/// Where a sweep stands with its probe of the keyspace for database subtrees.
#[derive(Default)]
enum Probe {
	/// The walk has passed no namespace or database definition a newer release
	/// wrote, so every database's id is known to it.
	#[default]
	Unneeded,
	/// The walk has passed such a definition; the probe begins once the walk
	/// ends.
	Owed,
	/// The probe is under way.
	Running(DatabaseProbe),
	/// The probe has reached every database subtree.
	Done,
}

/// A database whose retention a sweep is discovering: the longest retention
/// among its own and its tables' read so far, and the scan of the tables
/// still to read.
struct Discovery {
	database: Discovered,
	tables: ListScan<StoredTableDefinition>,
}

/// What a sweep reads of each database it reaches.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SweepReads {
	/// The database's changefeed retention: the longest of its own and its
	/// tables', whose definitions are read a page a step.
	Retention,
	/// The database alone, for a collector whose window is the datastore's,
	/// on which no table definition bears: a database is discovered the step
	/// that reaches it, whatever its tables.
	Database,
}

/// A database a sweep has reached, with what a pass collects it by.
#[derive(Clone, Debug)]
pub(crate) struct Discovered {
	pub(crate) ns: NamespaceId,
	pub(crate) db: DatabaseId,
	/// The names the database is defined under, which every collection page
	/// resolves it by again.
	ns_name: Arc<str>,
	db_name: Arc<str>,
	/// The database's changefeed-retention fence as the discovery read it, in
	/// the snapshot the retention was read from; `None` while no retention has
	/// ever been written under the database.
	fence: Option<u64>,
	/// The longest changefeed retention of the database and its tables; zero
	/// when none is configured, and so nothing to collect.
	pub(crate) expiry: Duration,
}

impl ChangefeedSweep {
	/// The databases the next `steps` steps of the sweep discover, in catalog
	/// order, each with its retention when `reads` asks for it. A sweep is
	/// paged with the same `reads` throughout.
	///
	/// Reads through `txn`. A database reached is read again by name in the
	/// same snapshot as its fence, rather than taken from the page the walk
	/// read, which an earlier pass may have read under a retention since
	/// written; one removed since the walk read it, or defined again since
	/// under its names, is passed, its entries being reclaimed with it rather
	/// than collected here. An empty page with nothing more to walk means the
	/// sweep has visited every database.
	pub(crate) async fn page(
		&mut self,
		txn: &Transaction,
		steps: usize,
		reads: SweepReads,
	) -> Result<Vec<Discovered>> {
		let mut page = Vec::new();
		let mut left = steps;
		while left > 0 {
			left -= 1;
			if let Some(discovery) = self.discovery.take() {
				self.discover(txn, discovery, &mut page).await?;
				continue;
			}
			if let Probe::Running(probe) = &mut self.probe {
				match probe.next(txn).await? {
					Some(Step::Reached(ids)) => self.unknown.push(ids),
					Some(Step::Passed) => {}
					None => {
						self.probe = Probe::Done;
						break;
					}
				}
				continue;
			}
			let step = self.walk.next(txn).await?;
			// A namespace or database definition the walk passed because a
			// newer release wrote it hides the id its subtree is addressed by,
			// so the sweep owes a probe of the keyspace.
			let refused = self.walk.take_refused();
			if refused > 0 {
				self.skipped += refused;
				if matches!(self.probe, Probe::Unneeded) {
					self.probe = Probe::Owed;
				}
			}
			match step {
				Some(Step::Reached((ns_name, db))) => match Discovery::begin(txn, ns_name, &db)
					.await
				{
					Ok(Some(discovery)) => match reads {
						SweepReads::Retention => self.discover(txn, discovery, &mut page).await?,
						SweepReads::Database => page.push(discovery.database),
					},
					Ok(None) => {}
					// Defined again by a newer release since the walk read it,
					// on an earlier pass's page: skipped and reported by id, as
					// a database holding such a table is.
					Err(e) if refused_by_release(&e) => {
						self.skipped += 1;
						self.unknown.push((db.namespace_id, db.database_id));
					}
					Err(e) => return Err(e),
				},
				Some(Step::Passed) => {}
				None if matches!(self.probe, Probe::Owed) => {
					self.probe = Probe::Running(DatabaseProbe::new()?);
				}
				None => break,
			}
		}
		Ok(page)
	}

	/// One step of a discovery: a page of the database's tables read into its
	/// retention, after which the database is discovered onto `page` if no
	/// table is left to read, and kept in discovery otherwise.
	async fn discover(
		&mut self,
		txn: &Transaction,
		mut discovery: Discovery,
		page: &mut Vec<Discovered>,
	) -> Result<()> {
		if let Some(tables) = discovery.tables.next_page(txn).await? {
			discovery.database.expiry = tables
				.iter()
				.filter_map(|tb| tb.changefeed)
				.map(|cf| Duration::from(cf.expiry))
				.fold(discovery.database.expiry, Duration::max);
		}
		// A table this build cannot read, because a newer release wrote its
		// definition, may keep changes longer than the tables it can read, so
		// the database's retention cannot be known: the database is skipped
		// whole, with every table it holds, since its changefeed entries are
		// shared across its tables and collecting any could remove what the
		// unreadable definition keeps. The entries skipped are counted for the
		// pass to report; the changefeed data is retained, never lost. The
		// database is reported by id all the same, for the live-query
		// collector: its window is the datastore's, which no table definition
		// bears on, so its events are collected whatever the sweep can read.
		let refused = discovery.tables.take_refused();
		if refused > 0 {
			self.skipped += refused;
			self.unknown.push((discovery.database.ns, discovery.database.db));
			return Ok(());
		}
		if discovery.tables.has_more() {
			self.discovery = Some(discovery);
		} else {
			page.push(discovery.database);
		}
		Ok(())
	}

	/// Whether the sweep has a database still to discover, one in discovery,
	/// or a probe owed or under way. A sweep that has not begun has: the
	/// catalog is unread.
	pub(crate) fn has_more(&self) -> bool {
		self.discovery.is_some()
			|| self.walk.has_more()
			|| matches!(self.probe, Probe::Owed | Probe::Running(_))
	}

	/// The catalog entries the sweep skipped since the count was last taken
	/// because a newer release wrote them: namespaces and databases the walk
	/// passed, and the tables of a database skipped whole. Their changefeed
	/// entries are retained, and the pass reports the count once.
	pub(crate) fn take_skipped(&mut self) -> usize {
		std::mem::take(&mut self.skipped)
	}

	/// The databases the sweep reported by id since they were last taken: those
	/// skipped whole because a newer release wrote one of their table
	/// definitions, whose changefeed retention it cannot know, and, once the walk
	/// has passed a namespace or database definition a newer release wrote,
	/// every database subtree the probe reaches, the readable ones among them.
	/// The collector whose window is the datastore's collects them all; a
	/// database also on a page is collected twice in that sweep, which costs a
	/// read and deletes nothing more.
	pub(crate) fn take_unknown(&mut self) -> Vec<(NamespaceId, DatabaseId)> {
		std::mem::take(&mut self.unknown)
	}
}

impl Discovery {
	/// Begin discovering `db`, reached by the walk in the namespace named
	/// `ns_name`, or `None` for a database removed since the walk read it or
	/// defined again since under its names.
	async fn begin(
		txn: &Transaction,
		ns_name: Arc<str>,
		db: &DatabaseDefinition,
	) -> Result<Option<Self>> {
		// The fence and the definition are read in one snapshot: a retention
		// written after the snapshot has bumped the fence the collection
		// pages compare with.
		let fence = txn
			.get_key(
				&ChangefeedFenceKey {
					ns: db.namespace_id,
					db: db.database_id,
				},
				None,
			)
			.await?;
		let Some(current) = txn.get_db_by_name(&ns_name, db.name.as_str(), None).await? else {
			return Ok(None);
		};
		if current.namespace_id != db.namespace_id || current.database_id != db.database_id {
			return Ok(None);
		}
		let tables = ListScan::new(
			TablePrefix {
				ns: db.namespace_id,
				db: db.database_id,
			}
			.range()?,
		);
		Ok(Some(Self {
			database: Discovered {
				ns: db.namespace_id,
				db: db.database_id,
				ns_name,
				db_name: Arc::from(db.name.as_str()),
				fence,
				expiry: current
					.changefeed
					.map(|cf| Duration::from(cf.expiry))
					.unwrap_or(Duration::ZERO),
			},
			tables,
		}))
	}
}

/// One database's stale changefeed entries: everything from the storage engine's
/// earliest timestamp up to, but not including, the database's retention
/// watermark.
struct Target<'a> {
	/// The database, with the fence its retention was discovered under, which
	/// every page revalidates the window against.
	db: &'a Discovered,
	stale: TypedRange<TableMutations>,
}

/// Delete every changefeed entry that is stale as of now from each of `dbs`.
///
/// `dbs` is the page of the catalog a pass collects: the datastore walks the
/// catalog a page of steps per pass, discovering each database's retention as
/// it reaches it, so a pass reads no more of the catalog than its steps and a
/// sweep visits every database once however many there are.
///
/// The backlog behind a database's retention watermark is sized by write
/// throughput, so it is deleted in bounded pages, each committed before the next
/// is scanned (see [`crate::kvs::paging`]). `budget` bounds the whole pass and is
/// shared across the databases it visits, so one large backlog cannot take the
/// whole pass; what a pass does not reach is collected by the next, starting at
/// the head of what survives.
///
/// Every database a pass visits is granted a share of what the budget has left,
/// so each one makes progress on every pass however many there are: the share
/// shrinks with the number of databases rather than the tail of the catalog going
/// uncollected. What a database leaves of its share goes to those that spent
/// theirs with backlog remaining, so the pass ends with backlog only once its
/// budget is spent (see [`delete_sharing_budget`]).
///
/// Reports whether the lease is still held, so a pass that lost it is reported
/// under the holder's standing rather than its own. A lost lease cannot
/// be re-derived from [`LeaseHandler::try_maintain_lease`] here: the check that
/// lost it consumed this maintenance period's one datastore read, so the next
/// call answers `true` from the throttle without asking.
#[cfg_attr(
	feature = "observability",
	tracing::instrument(level = "trace", target = "surrealdb::core::cfs", skip_all)
)]
pub async fn gc_databases(
	ds: &Datastore,
	lh: &LeaseHandler,
	canceller: &CancellationToken,
	budget: &mut u64,
	dbs: &[Discovered],
) -> Result<bool> {
	let targets = targets(ds, dbs).await?;
	// The backlog is collected in key order from a fixed lower bound, so
	// handing the rest to the next lease holder costs nothing: it resumes at
	// the head of what survives.
	delete_sharing_budget(ds, lh, canceller, budget, targets, |target| {
		trace!("Performing changefeed garbage collection on {}:{}", target.db.ns, target.db.db);
		PagedDelete {
			window: target.stale.clone(),
			companion: PageCompanion::ChangefeedRetention {
				ns: target.db.ns,
				db: target.db.db,
				ns_name: &target.db.ns_name,
				db_name: &target.db.db_name,
				fence: target.db.fence,
			},
			expunge: false,
			page: CHANGEFEED_GC_BATCH_SIZE,
		}
	})
	.await
}

/// The databases among `dbs` with a changefeed retention, each with the range
/// its watermark has made stale.
///
/// A database's retention is the longest of its own and those of its tables, as
/// the sweep discovered it, so a table configured to keep changes longer than its
/// database is honoured. Databases with no retention anywhere are omitted: they
/// have nothing to collect.
///
/// The watermarks are taken from one short-lived read transaction, so no snapshot
/// is held across the write transactions the deletes then commit.
async fn targets<'a>(ds: &Datastore, dbs: &'a [Discovered]) -> Result<Vec<Target<'a>>> {
	let txn = ds.transaction(Read).await?;
	let res = async {
		let ts_impl = txn.timestamp_impl();
		let ts = txn.timestamp().await?;
		// The storage engine's earliest timestamp, the fixed lower bound of every
		// database's stale range. A pass interrupted part way needs no cursor
		// against a fixed bound: everything it deleted is committed and gone, so
		// the next scan from here lands on the first entry that survives.
		let mut buf = [0u8; _];
		let earliest = ts_impl.earliest().encode(&mut buf).to_vec();
		let mut targets = Vec::new();
		for db in dbs.iter().filter(|db| !db.expiry.is_zero()) {
			let watermark = ts.sub_checked(db.expiry).unwrap_or_else(|| ts_impl.earliest());
			let mut buf = [0u8; _];
			let end = watermark.encode(&mut buf);
			// Up to, but not including, the watermark: the watermark's own
			// changes are still needed.
			let stale = ChangeFeedPrefix {
				ns: db.ns,
				db: db.db,
			}
			.range_where(Cow::Borrowed(earliest.as_slice())..Cow::Borrowed(end))?;
			targets.push(Target {
				db,
				stale,
			});
		}
		Ok(targets)
	}
	.await;
	let _ = txn.cancel().await;
	res
}

/// Whether the retention that made a database's window stale still stands, and
/// the database with it.
///
/// A window's upper bound is the watermark of the retention the sweep discovered,
/// and that retention is catalog state a `DEFINE DATABASE`, `DEFINE TABLE` or
/// `ALTER TABLE` committed since can extend, which moves the watermark earlier
/// and makes entries the window still names live again — and a deleted changefeed
/// entry cannot be recovered. Every statement that writes a retention bumps the
/// database's fence, so a fence that still reads as the discovery read it means
/// no retention has been written since, at the cost of one key however many
/// tables the database holds. Read inside the transaction that deletes a page,
/// and armed rather than merely read, so the answer covers that page and a
/// retention landing after this read rejects the page at commit on a
/// conflict-serializing backend. A retention withdrawn bumps nothing and
/// withdraws nothing: a shorter retention, or none, retains nothing the window
/// names.
///
/// Reports `false` — stop deleting — for a database removed since, or defined
/// again under its names: its entries are reclaimed with it, by its own
/// tombstone, rather than page by page here, and the new database's are not the
/// window's to name. Reports `false` as well for a database a newer release
/// has defined since, whose retention this build cannot read.
pub(crate) async fn retention_still_reaches(
	txn: &Transaction,
	ns_name: &str,
	db_name: &str,
	ns: NamespaceId,
	db: DatabaseId,
	fence: Option<u64>,
) -> Result<bool> {
	if txn.arm_changefeed_retention_fence(ns, db).await? != fence {
		return Ok(false);
	}
	match txn.get_db_by_name(ns_name, db_name, None).await {
		Ok(current) => Ok(current.is_some_and(|d| d.namespace_id == ns && d.database_id == db)),
		Err(e) if refused_by_release(&e) => Ok(false),
		Err(e) => Err(e),
	}
}
