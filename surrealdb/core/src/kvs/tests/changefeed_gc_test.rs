//! Tests for the changefeed garbage collector's bounds.
//!
//! The collector deletes every changefeed entry behind each database's retention
//! watermark. That backlog is sized by write throughput rather than by the
//! catalog, so it is deleted in bounded pages, each committed before the next is
//! scanned, and one pass spends a fixed key budget shared across the databases it
//! visits. The databases a pass visits are one page of the catalog, so a pass
//! reads no more of the catalog than its page and a sweep visits every database
//! a page per pass. These tests assert those bounds hold: the backlog is charged
//! as writes page by page rather than accumulating in one transaction, no
//! database can spend the whole pass on its own backlog, a pass visits no more
//! than its page of databases, and an interrupted pass leaves what it deleted
//! deleted.

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_observe::TaskLeaseState;
use tokio_util::sync::CancellationToken;
use web_time::Duration;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::gc::{ChangefeedSweep, Discovered, SweepReads, retention_still_reaches};
use crate::dbs::{Capabilities, Session};
use crate::key::schema::{ChangeFeedPrefix, ChangefeedFenceKey};
use crate::key::{AnyRange, KVSubspace, Key, RawRange};
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::tasks::Progress;
use crate::kvs::test_support::{WriteTxSizeObserver, cleanup_sizes};
use crate::kvs::{CHANGEFEED_GC_BATCH_SIZE, Datastore, NORMAL_BATCH_SIZE};
use crate::observe::ExecutionObserver;

/// A retention of one nanosecond puts every entry written before the pass behind
/// the watermark, so a whole backlog is collectable without waiting for it to
/// age.
const STALE: &str = "1ns";

/// A memory datastore whose every write transaction is recorded, and whose
/// background jobs are off so the only transactions observed are the ones the
/// test drives.
async fn observed_ds(observer: Arc<WriteTxSizeObserver>) -> Arc<Datastore> {
	Datastore::builder()
		.with_capabilities(Capabilities::all())
		.without_maintenance_tasks()
		.with_observer(observer as Arc<dyn ExecutionObserver>)
		.build_with_path("memory")
		.await
		.unwrap()
}

/// The whole changefeed keyspace of one database, whatever its entries'
/// timestamps.
async fn feed(ds: &Datastore, ns: &str, db: &str) -> RawRange {
	let txn = ds.transaction(Read).await.unwrap();
	let ns_def = txn.get_ns_by_name(ns, None).await.unwrap().unwrap();
	let db_def = txn.get_db_by_name(ns, db, None).await.unwrap().unwrap();
	let _ = txn.cancel().await;
	ChangeFeedPrefix {
		ns: ns_def.namespace_id,
		db: db_def.database_id,
	}
	.raw_range()
	.unwrap()
}

/// Every database of the catalog, in catalog order, as a sweep discovers them.
async fn databases(ds: &Datastore) -> Vec<Discovered> {
	let txn = ds.transaction(Read).await.unwrap();
	let dbs =
		ChangefeedSweep::default().page(&txn, usize::MAX, SweepReads::Retention).await.unwrap();
	let _ = txn.cancel().await;
	dbs
}

async fn count_range(ds: &Datastore, range: impl AnyRange) -> usize {
	let txn = ds.transaction(Read).await.unwrap();
	let keys = txn.keys_raw(range, u32::MAX, 0, None).await.unwrap();
	let _ = txn.cancel().await;
	keys.len()
}

/// Grow a database's backlog to `count` entries beyond the one its setup wrote.
///
/// The entries are derived from a real changefeed key, so each carries a
/// timestamp the watermark covers — the subspace's own lower bound sorts below
/// the encoded earliest timestamp and so falls outside the collected range. One
/// statement produces one entry however many records it touches, a changefeed key
/// being per transaction, so a backlog of this size is built here rather than by
/// churning records.
async fn grow_backlog(ds: &Datastore, feed: &RawRange, count: usize) {
	let txn = ds.transaction(Read).await.unwrap();
	let base = txn.keys_raw(feed.clone(), 1, 0, None).await.unwrap().remove(0);
	let _ = txn.cancel().await;
	let tx = ds.transaction(Write).await.unwrap();
	for i in 0..count {
		let mut key = base.clone();
		key.extend_from_slice(&(i as u64).to_be_bytes());
		tx.set(Key::from(key), vec![1u8]).await.unwrap();
	}
	tx.commit().await.unwrap();
}

/// Define a database whose changefeed already holds one entry.
async fn with_changefeed(ds: &Datastore, ns: &str, db: &str) {
	let ses = Session::owner().with_ns(ns).with_db(db);
	for res in ds
		.execute(
			&format!(
				"DEFINE DATABASE {db};
				 DEFINE TABLE thing CHANGEFEED {STALE};
				 CREATE thing:1;"
			),
			&ses,
			None,
		)
		.await
		.unwrap()
	{
		res.result.unwrap();
	}
}

/// The whole backlog is deleted, in transactions each bounded to one page.
///
/// The backlog grows with write throughput and has no upper bound, so deleting it
/// in one transaction makes the collector's memory a function of how much was
/// written since the last pass. Paging it also charges every deleted entry to a
/// transaction's write set, which a whole-range delete does not report at all.
#[tokio::test]
async fn changefeed_gc_deletes_the_backlog_in_bounded_committed_pages() {
	let page = CHANGEFEED_GC_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;

	let feed = feed(&ds, "test", "tenant").await;
	// More than two pages of backlog, so the collector has to commit several
	// times.
	grow_backlog(&ds, &feed, page * 2 + page / 2).await;
	let total = count_range(&ds, feed.clone()).await;
	assert!(total > 2 * page, "the backlog must hold more than two pages, holds {total}");

	observer.clear();
	ds.changefeed_process(&Duration::from_secs(1), &CancellationToken::new()).await.unwrap();

	assert_eq!(count_range(&ds, feed).await, 0, "the collector must empty the backlog");
	let sizes = observer.sizes();
	assert!(
		sizes.iter().all(|&n| n <= page as u64),
		"a collection write transaction carried more than one page: {sizes:?}"
	);
	assert!(
		sizes.iter().sum::<u64>() >= total as u64,
		"every collected entry must be charged as a write: {sizes:?}"
	);
	assert!(
		cleanup_sizes(&sizes).len() >= total / page,
		"the backlog must be spread across a transaction per page: {sizes:?}"
	);
}

/// A pass's key budget bounds the pass, not each database it visits.
///
/// Databases are visited in catalog order, so one that spent the whole budget on
/// its own backlog would leave every database behind it uncollected until it
/// finished — however many passes that takes.
#[tokio::test]
async fn changefeed_gc_shares_its_budget_across_databases() {
	let page = CHANGEFEED_GC_BATCH_SIZE as usize;
	let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	// `aaa` is visited first and carries a backlog larger than its share of the
	// pass, so a pass that spent the budget in visit order would never reach
	// `zzz`.
	with_changefeed(&ds, "test", "aaa").await;
	with_changefeed(&ds, "test", "zzz").await;

	let aaa = feed(&ds, "test", "aaa").await;
	let zzz = feed(&ds, "test", "zzz").await;
	grow_backlog(&ds, &aaa, page * 4).await;
	grow_backlog(&ds, &zzz, page / 2).await;
	assert!(count_range(&ds, zzz.clone()).await > 0, "the trailing database needs a backlog");

	// A budget one database's backlog alone would exhaust.
	ds.changefeed_gc_with_budget(
		&Duration::from_secs(1),
		(page * 2) as u64,
		&CancellationToken::new(),
	)
	.await
	.unwrap();

	assert!(
		count_range(&ds, aaa).await > 0,
		"a budget smaller than the leading backlog must leave part of it behind"
	);
	assert_eq!(
		count_range(&ds, zzz).await,
		0,
		"the trailing database must be collected in the same pass"
	);
}

/// What a database leaves of its share goes to one that spent its own with
/// backlog remaining, so a pass ends with backlog only once its budget is spent.
///
/// A pass that ends with budget unspent reports itself done and the next waits
/// a whole interval, so a share a finished database left unspent would idle the
/// backlog it could have collected.
#[tokio::test]
async fn changefeed_gc_spends_what_a_finished_database_leaves() {
	let page = CHANGEFEED_GC_BATCH_SIZE as usize;
	let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "aaa").await;
	with_changefeed(&ds, "test", "zzz").await;

	// `aaa` is visited first with a backlog beyond the whole budget, and `zzz`
	// holds far less than its share.
	let aaa = feed(&ds, "test", "aaa").await;
	let zzz = feed(&ds, "test", "zzz").await;
	grow_backlog(&ds, &aaa, page * 4).await;
	let budget = (page * 2) as u64;

	let pass = ds
		.changefeed_gc_with_budget(&Duration::from_secs(1), budget, &CancellationToken::new())
		.await
		.unwrap();

	assert_eq!(count_range(&ds, zzz).await, 0, "the trailing database is collected");
	assert_eq!(pass.items, budget, "what `zzz` left of its share is spent on `aaa`");
	assert_eq!(pass.progress, Progress::More, "backlog remains, so the next pass is due at once");
	assert!(count_range(&ds, aaa).await > 0);
}

/// An interrupted pass leaves what it deleted deleted, so the next pass resumes
/// at the head of what survives instead of redoing the work.
///
/// The collected range's lower bound is fixed at the storage engine's earliest
/// timestamp, which is what lets a pass resume without a durable cursor:
/// everything below the first surviving entry is already gone.
#[tokio::test]
async fn changefeed_gc_makes_durable_progress_across_passes() {
	let page = CHANGEFEED_GC_BATCH_SIZE as usize;
	let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;

	let feed = feed(&ds, "test", "tenant").await;
	grow_backlog(&ds, &feed, page * 3).await;
	let mut left = count_range(&ds, feed.clone()).await;

	// One page's worth of budget per pass: each must retire a page and no more,
	// and the backlog must shrink monotonically to nothing.
	for _ in 0..10 {
		ds.changefeed_gc_with_budget(
			&Duration::from_secs(1),
			page as u64,
			&CancellationToken::new(),
		)
		.await
		.unwrap();
		let now = count_range(&ds, feed.clone()).await;
		assert!(now < left, "a pass must make progress: {left} then {now}");
		left = now;
		if left == 0 {
			break;
		}
	}
	assert_eq!(left, 0, "repeated bounded passes must drain the backlog");
}

/// The ids of a database, for the revalidation call.
async fn db_ids(ds: &Datastore, ns: &str, db: &str) -> (NamespaceId, DatabaseId) {
	let txn = ds.transaction(Read).await.unwrap();
	let ns_def = txn.get_ns_by_name(ns, None).await.unwrap().unwrap();
	let db_def = txn.get_db_by_name(ns, db, None).await.unwrap().unwrap();
	let _ = txn.cancel().await;
	(ns_def.namespace_id, db_def.database_id)
}

/// A stale range's upper bound only holds while the retention that produced it
/// does.
///
/// The watermark is computed once per sweep from the catalog, but the deletes it
/// authorises are committed in separate transactions over many pages. A
/// `DEFINE TABLE` or `DEFINE DATABASE` extending retention in that window moves
/// the watermark earlier, making entries the range still names live again — and a
/// deleted changefeed entry cannot be recovered. Every such write bumps the
/// database's fence, so the fence is compared inside every page transaction with
/// the count the sweep discovered the retention under, which is what this
/// asserts.
#[tokio::test]
async fn an_extended_retention_withdraws_the_stale_bound() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let (ns, db) = db_ids(&ds, "test", "tenant").await;

	// The fence as a sweep discovering the one-nanosecond retention reads it.
	let discovered = fence(&ds, ns, db).await;

	// Unchanged policy: the fence still reads as discovered, so the delete may
	// proceed.
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		retention_still_reaches(&txn, "test", "tenant", ns, db, discovered).await.unwrap(),
		"an unchanged retention must keep authorising the range it made stale"
	);
	let _ = txn.cancel().await;

	// Extending retention to an hour moves the watermark an hour earlier, so
	// everything the old bound named is inside the window again.
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE TABLE OVERWRITE thing CHANGEFEED 1h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		!retention_still_reaches(&txn, "test", "tenant", ns, db, discovered).await.unwrap(),
		"an extended retention must withdraw the bound a sweep started from"
	);
	let _ = txn.cancel().await;
}

/// A retention withdrawn leaves the bound standing, and a database removed
/// withdraws it.
///
/// A shorter retention, or none, retains nothing the old bound named: the
/// entries behind it are as stale under the new policy as under the old, so the
/// collector goes on deleting them. A removed database is reclaimed by its own
/// tombstone rather than page by page here, and one defined again under the same
/// names is another database, whose entries the removed one's window must not
/// name.
#[tokio::test]
async fn a_removed_database_withdraws_the_stale_bound() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let (ns, db) = db_ids(&ds, "test", "tenant").await;
	let discovered = fence(&ds, ns, db).await;

	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE TABLE OVERWRITE thing;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		retention_still_reaches(&txn, "test", "tenant", ns, db, discovered).await.unwrap(),
		"a retention withdrawn retains nothing the bound named, so the bound stands"
	);
	let _ = txn.cancel().await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap()[0].result.as_ref().unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		!retention_still_reaches(&txn, "test", "tenant", ns, db, discovered).await.unwrap(),
		"a removed database is reclaimed by its tombstone, not collected here"
	);
	let _ = txn.cancel().await;

	ds.execute("DEFINE DATABASE tenant;", &ses, None).await.unwrap()[0].result.as_ref().unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		!retention_still_reaches(&txn, "test", "tenant", ns, db, discovered).await.unwrap(),
		"a database defined again under the names is another database"
	);
	let _ = txn.cancel().await;
}

/// A pass that loses the lease reports it, so the pass is reported under the
/// holder's standing.
///
/// A lost lease cannot be re-derived from the handler: `try_maintain_lease`
/// reads storage at most once per maintenance period, so the very check that
/// lost the lease consumes the read the next check would have made, and that
/// next check answers `true` from the throttle. Without the outcome being
/// propagated, the pass reports a lease it no longer holds.
#[tokio::test]
async fn a_pass_that_loses_the_lease_reports_it() {
	let page = CHANGEFEED_GC_BATCH_SIZE as usize;
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let feed = feed(&ds, "test", "tenant").await;
	grow_backlog(&ds, &feed, page).await;
	let backlog = count_range(&ds, feed.clone()).await;
	assert!(backlog > 0, "there must be a backlog for the pass to page through");

	// Another node takes the changefeed-cleanup lease.
	let held = LeaseHandler::new(
		ds.sequences().clone(),
		uuid::Uuid::now_v7(),
		ds.transaction_factory().clone(),
		TaskLeaseType::ChangeFeedCleanup,
		std::time::Duration::from_secs(60),
	)
	.unwrap();
	assert!(held.has_lease().await.unwrap(), "the other node must take the lease");

	// This node's pass therefore has no lease to maintain.
	let ours = LeaseHandler::new(
		ds.sequences().clone(),
		uuid::Uuid::now_v7(),
		ds.transaction_factory().clone(),
		TaskLeaseType::ChangeFeedCleanup,
		std::time::Duration::from_secs(60),
	)
	.unwrap();
	let dbs = databases(&ds).await;
	let mut budget = u64::MAX;
	let held_after =
		crate::cf::gc_databases(&ds, &ours, &CancellationToken::new(), &mut budget, &dbs)
			.await
			.unwrap();

	assert!(!held_after, "a pass that lost the lease must report the loss to its caller");
	assert_eq!(
		count_range(&ds, feed).await,
		backlog,
		"a pass without the lease must not collect anything"
	);
}

/// A pass that keeps the lease reports that too.
#[tokio::test]
async fn a_completed_pass_reports_the_lease_still_held() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let feed = feed(&ds, "test", "tenant").await;
	grow_backlog(&ds, &feed, CHANGEFEED_GC_BATCH_SIZE as usize).await;

	let ours = LeaseHandler::new(
		ds.sequences().clone(),
		uuid::Uuid::now_v7(),
		ds.transaction_factory().clone(),
		TaskLeaseType::ChangeFeedCleanup,
		std::time::Duration::from_secs(60),
	)
	.unwrap();
	assert!(ours.has_lease().await.unwrap(), "this node must hold the lease");
	let dbs = databases(&ds).await;
	let mut budget = u64::MAX;
	assert!(
		crate::cf::gc_databases(&ds, &ours, &CancellationToken::new(), &mut budget, &dbs)
			.await
			.unwrap(),
		"a pass that kept the lease must report it"
	);
	assert_eq!(count_range(&ds, feed).await, 0, "the pass must collect the backlog");
}

/// The live-query collector reports a lost lease the same way, so its pass is
/// reported under the holder's standing.
#[tokio::test]
async fn a_live_event_collection_that_loses_the_lease_reports_it() {
	let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let ids = [db_ids(&ds, "test", "tenant").await];

	// Another node takes the live-query cleanup lease.
	let held = LeaseHandler::new(
		ds.sequences().clone(),
		uuid::Uuid::now_v7(),
		ds.transaction_factory().clone(),
		TaskLeaseType::LiveQueryCleanup,
		std::time::Duration::from_secs(60),
	)
	.unwrap();
	assert!(held.has_lease().await.unwrap(), "the other node must take the lease");

	// This node's pass therefore has no lease to maintain.
	let ours = LeaseHandler::new(
		ds.sequences().clone(),
		uuid::Uuid::now_v7(),
		ds.transaction_factory().clone(),
		TaskLeaseType::LiveQueryCleanup,
		std::time::Duration::from_secs(60),
	)
	.unwrap();
	let mut budget = u64::MAX;
	let kept = crate::lq::gc::gc_databases(
		&ds,
		&ours,
		Duration::from_secs(1),
		&CancellationToken::new(),
		&mut budget,
		&ids,
	)
	.await
	.unwrap();
	assert!(!kept, "a collection that lost the lease must report the loss to its caller");
}

/// A live-query collection that keeps the lease reports that too.
#[tokio::test]
async fn a_completed_live_event_collection_reports_the_lease_still_held() {
	let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let ids = [db_ids(&ds, "test", "tenant").await];

	let ours = LeaseHandler::new(
		ds.sequences().clone(),
		uuid::Uuid::now_v7(),
		ds.transaction_factory().clone(),
		TaskLeaseType::LiveQueryCleanup,
		std::time::Duration::from_secs(60),
	)
	.unwrap();
	assert!(ours.has_lease().await.unwrap(), "this node must hold the lease");
	let mut budget = u64::MAX;
	let kept = crate::lq::gc::gc_databases(
		&ds,
		&ours,
		Duration::from_secs(1),
		&CancellationToken::new(),
		&mut budget,
		&ids,
	)
	.await
	.unwrap();
	assert!(kept, "a collection that kept the lease must report it");
}

/// Changefeed collection and live-event collection each hold a lease of their
/// own, since each runs on a cadence a node may disable: a node holding either
/// keeps no other node from running the other.
#[tokio::test]
async fn each_collector_runs_while_another_node_holds_the_other_s_lease() {
	let interval = Duration::from_secs(1);
	for (held_elsewhere, runs_here) in [
		(TaskLeaseType::ChangeFeedCleanup, TaskLeaseType::LiveQueryCleanup),
		(TaskLeaseType::LiveQueryCleanup, TaskLeaseType::ChangeFeedCleanup),
	] {
		let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
		ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
		with_changefeed(&ds, "test", "tenant").await;
		let other = LeaseHandler::new(
			ds.sequences().clone(),
			uuid::Uuid::now_v7(),
			ds.transaction_factory().clone(),
			held_elsewhere,
			std::time::Duration::from_secs(60),
		)
		.unwrap();
		assert!(other.has_lease().await.unwrap(), "another node takes {held_elsewhere:?}");

		let canceller = CancellationToken::new();
		let pass = if matches!(runs_here, TaskLeaseType::LiveQueryCleanup) {
			ds.live_query_gc_process(&interval, &canceller).await
		} else {
			ds.changefeed_process(&interval, &canceller).await
		}
		.unwrap();
		assert_eq!(
			pass.lease,
			TaskLeaseState::Held,
			"the collector under {runs_here:?} runs on this node: {pass:?}"
		);
	}
}

/// How many of `feeds` still hold an entry.
async fn uncollected(ds: &Datastore, feeds: &[RawRange]) -> usize {
	let mut n = 0;
	for feed in feeds {
		if count_range(ds, feed.clone()).await > 0 {
			n += 1;
		}
	}
	n
}

/// A pass takes one page of steps through the catalog, and a sweep visits every
/// database a page per pass.
///
/// Discovery read every namespace, database and table of the catalog on every
/// pass, before the key budget applied, so a pass was as long as the catalog in
/// a slice a host driving maintenance itself waits on. A pass now reads the
/// pages of the catalog its steps move through alone, keeps its place in the
/// catalog between passes, and reports `More` while the sweep has databases
/// left.
#[tokio::test]
async fn changefeed_gc_visits_a_page_of_databases_per_pass() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	let mut feeds = Vec::new();
	for db in ["a", "b", "c"] {
		with_changefeed(&ds, "test", db).await;
		let feed = feed(&ds, "test", db).await;
		grow_backlog(&ds, &feed, 8).await;
		feeds.push(feed);
	}
	let per_feed = count_range(&ds, feeds[0].clone()).await;
	assert_eq!(uncollected(&ds, &feeds).await, 3);

	// A page of three steps: the namespace entered, then two of the three
	// databases, each reached with its page of tables in one step, so the pass
	// collects two and stops with the third still to visit.
	let first = ds
		.changefeed_gc_with_page(&Duration::from_secs(1), 3, &CancellationToken::new())
		.await
		.unwrap();
	assert_eq!(first.progress, Progress::More, "a database is still to visit");
	assert_eq!(first.items as usize, 2 * per_feed, "the two databases on the page are collected");
	assert_eq!(uncollected(&ds, &feeds).await, 1, "the third database is not visited");

	let second = ds
		.changefeed_gc_with_page(&Duration::from_secs(1), 3, &CancellationToken::new())
		.await
		.unwrap();
	assert_eq!(second.progress, Progress::Done, "the sweep has visited every database");
	assert_eq!(second.items as usize, per_feed);
	assert_eq!(uncollected(&ds, &feeds).await, 0);

	// The next sweep begins over the catalog afresh.
	for (db, feed) in ["a", "b", "c"].into_iter().zip(&feeds) {
		let ses = Session::owner().with_ns("test").with_db(db);
		ds.execute("CREATE thing:2;", &ses, None).await.unwrap()[0].result.as_ref().unwrap();
		grow_backlog(&ds, feed, 8).await;
	}
	assert_eq!(uncollected(&ds, &feeds).await, 3);
	let third = ds
		.changefeed_gc_with_page(&Duration::from_secs(1), 3, &CancellationToken::new())
		.await
		.unwrap();
	assert_eq!(third.progress, Progress::More);
	assert_eq!(uncollected(&ds, &feeds).await, 1);
	let fourth = ds
		.changefeed_gc_with_page(&Duration::from_secs(1), 3, &CancellationToken::new())
		.await
		.unwrap();
	assert_eq!(fourth.progress, Progress::Done);
	assert_eq!(uncollected(&ds, &feeds).await, 0);
}

/// A database's tables are read a page at a time as the sweep discovers its
/// retention, a page per step, so a pass reads at most a page of a database's
/// tables however many it holds, and a database whose tables span pages spans
/// steps.
///
/// Discovery listed each database's tables whole, before the key budget applied,
/// so a pass over a database of many tables read them all in a slice a host
/// driving maintenance itself waits on.
#[tokio::test]
async fn changefeed_discovery_reads_a_database_s_tables_a_page_at_a_time() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	// A page of tables without a retention ahead, in table order, of the one
	// with the longest: the retention the sweep is after is on the second
	// page.
	let ses = Session::owner().with_ns("test").with_db("tenant");
	let mut sql = String::new();
	for n in 0..NORMAL_BATCH_SIZE {
		sql.push_str(&format!("DEFINE TABLE t{n:04}; "));
	}
	sql.push_str("DEFINE TABLE zzz CHANGEFEED 1h;");
	for res in ds.execute(&sql, &ses, None).await.unwrap() {
		res.result.unwrap();
	}

	let txn = ds.transaction(Read).await.unwrap();
	let mut sweep = ChangefeedSweep::default();
	// Two steps: the namespace entered, and the database reached with the
	// first page of its tables read.
	let page = sweep.page(&txn, 2, SweepReads::Retention).await.unwrap();
	assert!(page.is_empty(), "the tables span a page, so the database is still in discovery");
	assert!(sweep.has_more());
	// One more: the second page, which ends the discovery with the retention
	// found on it.
	let page = sweep.page(&txn, 1, SweepReads::Retention).await.unwrap();
	assert_eq!(page.len(), 1, "the database is discovered");
	assert_eq!(
		page[0].expiry,
		Duration::from_secs(3600),
		"with the longest retention among its tables"
	);
	assert!(!sweep.has_more(), "the sweep has visited every database");
	let _ = txn.cancel().await;
}

/// A sweep for a collector whose window is the datastore's reads no table
/// definition: a database is discovered the step that reaches it, however many
/// tables it holds.
#[tokio::test]
async fn a_database_sweep_discovers_a_database_without_reading_its_tables() {
	let ds = observed_ds(Arc::new(WriteTxSizeObserver::default())).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	let mut sql = String::new();
	for n in 0..NORMAL_BATCH_SIZE {
		sql.push_str(&format!("DEFINE TABLE t{n:04}; "));
	}
	for res in ds.execute(&sql, &ses, None).await.unwrap() {
		res.result.unwrap();
	}

	let txn = ds.transaction(Read).await.unwrap();
	let mut sweep = ChangefeedSweep::default();
	// Two steps: the namespace entered, and the database reached.
	let page = sweep.page(&txn, 2, SweepReads::Database).await.unwrap();
	assert_eq!(page.len(), 1, "the database is discovered the step that reaches it");
	assert!(!sweep.has_more(), "the sweep has visited every database");
	let _ = txn.cancel().await;
}

/// This database's changefeed-retention fence, or `None` while no retention has
/// ever been written.
async fn fence(ds: &Datastore, ns: NamespaceId, db: DatabaseId) -> Option<u64> {
	let txn = ds.transaction(Read).await.unwrap();
	let v = txn
		.get_key(
			&ChangefeedFenceKey {
				ns,
				db,
			},
			None,
		)
		.await
		.unwrap();
	let _ = txn.cancel().await;
	v
}

/// Every statement that writes a changefeed retention bumps the fence.
///
/// The fence is the only key a collection page and a retention change both
/// touch, so a path that sets a retention without bumping it leaves that page
/// free to go on deleting under the watermark the old policy produced. A bump
/// replaces the value rather than counting it up, so each is checked to differ
/// from the last. Dropping a retention is not bumped: it can only lower the
/// maximum, which strands nothing.
#[tokio::test]
async fn every_retention_write_bumps_the_fence() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();

	// DEFINE DATABASE with a retention.
	ds.execute("DEFINE DATABASE tenant CHANGEFEED 1h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let (ns, db) = db_ids(&ds, "test", "tenant").await;
	let after_db = fence(&ds, ns, db).await.expect("DEFINE DATABASE must bump the fence");

	// DEFINE TABLE with a retention.
	ds.execute("DEFINE TABLE thing CHANGEFEED 1h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let after_tb = fence(&ds, ns, db).await.unwrap();
	assert_ne!(after_tb, after_db, "DEFINE TABLE must bump the fence: {after_db} -> {after_tb}");

	// ALTER TABLE setting a retention, on a table that already had one.
	ds.execute("ALTER TABLE thing CHANGEFEED 2h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let after_alter = fence(&ds, ns, db).await.unwrap();
	assert_ne!(
		after_alter, after_tb,
		"ALTER TABLE must bump the fence: {after_tb} -> {after_alter}"
	);

	// ALTER TABLE setting a retention on a table that had none — raises the
	// database's maximum just as much, and is a different branch from the replace
	// above.
	ds.execute("DEFINE TABLE other;", &ses, None).await.unwrap()[0].result.as_ref().unwrap();
	let before_fresh = fence(&ds, ns, db).await.unwrap();
	ds.execute("ALTER TABLE other CHANGEFEED 3h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let after_fresh = fence(&ds, ns, db).await.unwrap();
	assert_ne!(
		after_fresh, before_fresh,
		"setting a retention on a table that had none must bump the fence: \
		 {before_fresh} -> {after_fresh}"
	);

	// Dropping one does not: it can only lower the maximum.
	ds.execute("ALTER TABLE other DROP CHANGEFEED;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	assert_eq!(
		fence(&ds, ns, db).await.unwrap(),
		after_fresh,
		"dropping a retention needs no fence bump: it strands nothing"
	);
}

/// Every statement that writes a changefeed retention leaves the fence at a
/// nonce, not at the successor of the value it found.
///
/// The fence is seeded to a known value before each statement. A statement
/// that read the fence and wrote the next count — which two statements from
/// one snapshot would both write — leaves exactly the seed's successor; a
/// nonce leaves anything but the seed and, barring a one-in-2^64 draw, its
/// successor. The three statements that write a retention each lower the
/// write separately, so each is driven.
#[tokio::test]
async fn every_retention_write_leaves_a_nonce_not_the_next_count() {
	let ds = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	for r in ds.execute("DEFINE DATABASE tenant; DEFINE TABLE thing;", &ses, None).await.unwrap() {
		r.result.unwrap();
	}
	let (ns, db) = db_ids(&ds, "test", "tenant").await;
	const SEED: u64 = 1_000;
	for stmt in [
		"DEFINE DATABASE OVERWRITE tenant CHANGEFEED 1h;",
		"DEFINE TABLE OVERWRITE thing CHANGEFEED 1h;",
		"ALTER TABLE thing CHANGEFEED 2h;",
	] {
		let txn = ds.transaction(Write).await.unwrap();
		txn.set_key(
			&ChangefeedFenceKey {
				ns,
				db,
			},
			&SEED,
		)
		.await
		.unwrap();
		txn.commit().await.unwrap();
		assert_eq!(fence(&ds, ns, db).await, Some(SEED), "the seed is in place for {stmt}");
		ds.execute(stmt, &ses, None).await.unwrap()[0].result.as_ref().unwrap();
		let after = fence(&ds, ns, db).await.unwrap();
		assert!(
			after != SEED && after != SEED + 1,
			"{stmt} must leave a nonce rather than the next count: {SEED} -> {after}"
		);
	}
}

/// Two retention writes from one snapshot leave the fence at two values.
///
/// The fence is a nonce, not a count. Two writers counting from the same
/// snapshot would both write the same next value, and on a backend that lets
/// two blind writes of one key both commit — the last-writer-wins ones, which
/// validate no read set — a collector that discovered the retention under the
/// first could not tell the second had landed, and would go on deleting under
/// a retention the second had already lengthened. The value each writer leaves
/// is read back in its own transaction: whether both can commit is the
/// backend's, and this holds on every backend.
#[tokio::test]
async fn racing_retention_writes_leave_the_fence_at_two_values() {
	let ds = Datastore::new("memory").await.unwrap();
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	ds.execute("DEFINE DATABASE tenant CHANGEFEED 1h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let (ns, db) = db_ids(&ds, "test", "tenant").await;
	let discovered = fence(&ds, ns, db).await;
	let key = ChangefeedFenceKey {
		ns,
		db,
	};

	// Two writers from the same snapshot, as two statements on different tables
	// of the database are when neither has committed.
	let first = ds.transaction(Write).await.unwrap();
	let second = ds.transaction(Write).await.unwrap();
	first.bump_changefeed_retention_fence(ns, db).await.unwrap();
	second.bump_changefeed_retention_fence(ns, db).await.unwrap();
	let left_by_first = first.get_key(&key, None).await.unwrap();
	let left_by_second = second.get_key(&key, None).await.unwrap();
	first.commit().await.unwrap();
	let _ = second.cancel().await;

	assert_ne!(left_by_first, discovered, "the first write must move the fence");
	assert_ne!(left_by_second, discovered, "the second write must move the fence");
	assert_ne!(
		left_by_first, left_by_second,
		"two writes from one snapshot must leave the fence at two values"
	);
}

/// A collection page arms against the fence, so a retention change committed
/// while the page is open rejects the page instead of being ignored.
///
/// This is what a plain catalog read cannot do. The page reads catalog keys and
/// writes changefeed keys, so on a last-writer-wins backend the two transactions
/// share no key and the page commits regardless; arming the fence gives them one.
#[tokio::test]
async fn a_retention_change_rejects_an_open_collection_page() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let (ns, db) = db_ids(&ds, "test", "tenant").await;
	let discovered = fence(&ds, ns, db).await;

	// A page transaction that has revalidated the bound and holds the fence.
	let page = ds.transaction(Write).await.unwrap();
	assert!(
		retention_still_reaches(&page, "test", "tenant", ns, db, discovered).await.unwrap(),
		"the retention must still authorise the range before the change lands"
	);

	// The retention changes while that page is open.
	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("ALTER TABLE thing CHANGEFEED 1h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();

	// The page must not be allowed to land: it would delete entries the committed
	// policy retains.
	page.set(Key::from(b"unused".to_vec()), vec![1u8]).await.unwrap();
	page.commit()
		.await
		.expect_err("a page open across a retention change must be rejected at commit");
}
