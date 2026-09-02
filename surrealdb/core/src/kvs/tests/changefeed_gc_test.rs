//! Tests for the changefeed garbage collector's bounds.
//!
//! The collector deletes every changefeed entry behind each database's retention
//! watermark. That backlog is sized by write throughput rather than by the
//! catalog, so it is deleted in bounded pages, each committed before the next is
//! scanned, and one pass spends a fixed key budget shared across the databases it
//! visits. These tests assert those bounds hold: the backlog is charged as writes
//! page by page rather than accumulating in one transaction, no database can
//! spend the whole pass on its own backlog, and an interrupted pass leaves what
//! it deleted deleted.

use std::sync::Arc;

use surrealdb_kvs::TransactionType::{Read, Write};
use tokio_util::sync::CancellationToken;
use web_time::Duration;

use crate::catalog::providers::{DatabaseProvider, NamespaceProvider};
use crate::catalog::{DatabaseId, NamespaceId};
use crate::cf::gc::retention_still_reaches;
use crate::dbs::{Capabilities, Session};
use crate::key::schema::{ChangeFeedPrefix, ChangefeedFenceKey};
use crate::key::{AnyRange, KVSubspace, Key, RawRange};
use crate::kvs::tasklease::{LeaseHandler, TaskLeaseType};
use crate::kvs::test_support::{WriteTxSizeObserver, cleanup_sizes};
use crate::kvs::{CHANGEFEED_GC_BATCH_SIZE, Datastore};
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

/// The encoded watermark a pass would build right now for `expiry`.
async fn watermark_now(ds: &Datastore, expiry: std::time::Duration) -> Vec<u8> {
	let txn = ds.transaction(Read).await.unwrap();
	let ts_impl = txn.timestamp_impl();
	let ts = txn.timestamp().await.unwrap();
	let _ = txn.cancel().await;
	let watermark = ts.sub_checked(expiry).unwrap_or_else(|| ts_impl.earliest());
	let mut buf = [0u8; _];
	watermark.encode(&mut buf).to_vec()
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
/// The watermark is computed once per pass from the catalog, but the deletes it
/// authorises are committed in separate transactions over many pages. A
/// `DEFINE TABLE` or `DEFINE DATABASE` extending retention in that window moves
/// the watermark earlier, making entries the range still names live again — and a
/// deleted changefeed entry cannot be recovered. The bound is therefore
/// revalidated inside every page transaction, which is what this asserts.
#[tokio::test]
async fn an_extended_retention_withdraws_the_stale_bound() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let (ns, db) = db_ids(&ds, "test", "tenant").await;

	// The bound a pass would start from under the one-nanosecond retention.
	let bound = watermark_now(&ds, std::time::Duration::from_nanos(1)).await;

	// Unchanged policy: the watermark a page recomputes still reaches the bound,
	// so the delete may proceed.
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		retention_still_reaches(&txn, ns, db, &bound).await.unwrap(),
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
		!retention_still_reaches(&txn, ns, db, &bound).await.unwrap(),
		"an extended retention must withdraw the bound a pass started from"
	);
	let _ = txn.cancel().await;
}

/// A database that loses its retention, or goes away entirely, withdraws the
/// bound too: the first has nothing left to collect, and the second is reclaimed
/// by its own tombstone rather than page by page here.
#[tokio::test]
async fn a_removed_retention_withdraws_the_stale_bound() {
	let observer = Arc::new(WriteTxSizeObserver::default());
	let ds = observed_ds(Arc::clone(&observer)).await;
	ds.execute("DEFINE NAMESPACE test;", &Session::owner(), None).await.unwrap();
	with_changefeed(&ds, "test", "tenant").await;
	let (ns, db) = db_ids(&ds, "test", "tenant").await;
	let bound = watermark_now(&ds, std::time::Duration::from_nanos(1)).await;

	let ses = Session::owner().with_ns("test").with_db("tenant");
	ds.execute("DEFINE TABLE OVERWRITE thing;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		!retention_still_reaches(&txn, ns, db, &bound).await.unwrap(),
		"a database with no retention left has nothing this collector may delete"
	);
	let _ = txn.cancel().await;

	ds.execute("REMOVE DATABASE tenant;", &ses, None).await.unwrap()[0].result.as_ref().unwrap();
	let txn = ds.transaction(Write).await.unwrap();
	assert!(
		!retention_still_reaches(&txn, ns, db, &bound).await.unwrap(),
		"a removed database is reclaimed by its tombstone, not collected here"
	);
	let _ = txn.cancel().await;
}

/// A pass that loses the lease reports it, so the caller does not start the
/// live-query collector on the same lease.
///
/// The two collectors share one lease, and a lost lease cannot be re-derived
/// from the handler: `try_maintain_lease` reads storage at most once per
/// maintenance period, so the very check that lost the lease consumes the read
/// the next check would have made, and that next check answers `true` from the
/// throttle. Without the outcome being propagated, the former holder collects
/// alongside the new one.
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
	let mut budget = u64::MAX;
	let held_after =
		crate::cf::gc_all_at(&ds, &ours, &CancellationToken::new(), &mut budget).await.unwrap();

	assert!(!held_after, "a pass that lost the lease must report the loss to its caller");
	assert_eq!(
		count_range(&ds, feed).await,
		backlog,
		"a pass without the lease must not collect anything"
	);
}

/// A pass that keeps the lease reports that too, so the caller goes on to the
/// live-query collector.
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
	let mut budget = u64::MAX;
	assert!(
		crate::cf::gc_all_at(&ds, &ours, &CancellationToken::new(), &mut budget).await.unwrap(),
		"a pass that kept the lease must report it, so the caller runs the next collector"
	);
	assert_eq!(count_range(&ds, feed).await, 0, "the pass must collect the backlog");
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
/// free to go on deleting under the watermark the old policy produced. Dropping a
/// retention is not bumped: it can only lower the maximum, which strands nothing.
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
	assert!(after_tb > after_db, "DEFINE TABLE must bump the fence: {after_db} -> {after_tb}");

	// ALTER TABLE setting a retention, on a table that already had one.
	ds.execute("ALTER TABLE thing CHANGEFEED 2h;", &ses, None).await.unwrap()[0]
		.result
		.as_ref()
		.unwrap();
	let after_alter = fence(&ds, ns, db).await.unwrap();
	assert!(after_alter > after_tb, "ALTER TABLE must bump the fence: {after_tb} -> {after_alter}");

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
	assert!(
		after_fresh > before_fresh,
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
	let bound = watermark_now(&ds, std::time::Duration::from_nanos(1)).await;

	// A page transaction that has revalidated the bound and holds the fence.
	let page = ds.transaction(Write).await.unwrap();
	assert!(
		retention_still_reaches(&page, ns, db, &bound).await.unwrap(),
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
