//! Changefeed retention and the keys that back it.
//!
//! A database's `!ts` keys correlate a wall clock second with the versionstamp
//! current at that moment, and its `#` keys are the change entries themselves.
//! Both live inside `crate::key::database::all`, and both exist only to serve
//! changefeed retention: the timestamp writer maintains them for a database
//! that keeps a changefeed, and garbage collection clears them for one that
//! does not.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::{CreateDs, Kvs};
use crate::dbs::node::Timestamp;
use crate::dbs::Session;
use crate::kvs::clock::{FakeClock, SizedClock};
use crate::kvs::{Datastore, Key, KeyEncode, LockType::*, TransactionType::*};

/// Every key currently stored under a prefix, in key order.
async fn keys_between(ds: &Datastore, beg: Vec<u8>, end: Vec<u8>) -> Vec<Key> {
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let res = txn.keys(beg..end, u32::MAX, None).await.unwrap();
	txn.cancel().await.unwrap();
	res
}

/// The timestamp keys of a database.
async fn ts_keys(ds: &Datastore, ns: &str, db: &str) -> Vec<Key> {
	let beg = crate::key::database::ts::prefix(ns, db).unwrap();
	let end = crate::key::database::ts::suffix(ns, db).unwrap();
	keys_between(ds, beg, end).await
}

/// The ranges garbage collection would drain at a timestamp.
async fn plan_at(ds: &Datastore, ts: u64) -> Vec<(std::ops::Range<Vec<u8>>, crate::cf::Fence)> {
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let plan = crate::cf::gc_all_at(&txn, ts).await.unwrap();
	txn.cancel().await.unwrap();
	plan
}

/// The changefeed entries of a database.
async fn change_keys(ds: &Datastore, ns: &str, db: &str) -> Vec<Key> {
	let mut beg = crate::key::database::all::new(ns, db).encode().unwrap();
	beg.push(b'#');
	let mut end = beg.clone();
	end.push(0xff);
	keys_between(ds, beg, end).await
}

/// A timestamp key is only maintained for a database which keeps a changefeed.
///
/// The writer runs on a timer for every database on the instance and writes
/// inside `crate::key::database::all`, so writing for a database that has no
/// changefeed puts a periodic write into a prefix nothing needs it in.
pub async fn versionstamp_timestamps_follow_changefeed_retention(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("f1a2b3c4-d5e6-4708-9a1b-2c3d4e5f6071").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// One database with a changefeed, one with a table-level changefeed, and one
	// with neither
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE feed CHANGEFEED 1h;
		 USE NS ns; DEFINE DATABASE tbfeed; USE DB tbfeed; DEFINE TABLE person CHANGEFEED 1h;
		 USE NS ns; DEFINE DATABASE plain; USE DB plain; DEFINE TABLE person",
		&root,
		None,
	)
	.await
	.unwrap();
	// One tick of the timestamp writer
	ds.changefeed_versionstamp(1).await.unwrap();
	// Only the databases which keep a changefeed have a timeline
	assert_eq!(ts_keys(&ds, "ns", "feed").await.len(), 1);
	assert_eq!(ts_keys(&ds, "ns", "tbfeed").await.len(), 1);
	assert!(ts_keys(&ds, "ns", "plain").await.is_empty());
}

/// Dropping the last changefeed of a database collects what it left behind.
///
/// A database with no retention keeps no change entries and has no timeline, so
/// the entries and timestamps written while it did keep one are stale the
/// moment the retention goes, and nothing else will ever write another.
pub async fn dropping_a_changefeed_collects_what_it_left_behind(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("0a1b2c3d-4e5f-4071-82a3-b4c5d6e7f809").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database with a changefeed, and one which keeps its own
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person; CREATE person:1, person:2;
		 USE NS ns; DEFINE DATABASE keep CHANGEFEED 1h; USE DB keep;
		 DEFINE TABLE person; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	ds.changefeed_versionstamp(1).await.unwrap();
	assert!(!change_keys(&ds, "ns", "db").await.is_empty());
	assert!(!ts_keys(&ds, "ns", "db").await.is_empty());
	let kept_changes = change_keys(&ds, "ns", "keep").await;
	let kept_ts = ts_keys(&ds, "ns", "keep").await;
	assert!(!kept_changes.is_empty());
	assert!(!kept_ts.is_empty());
	// Drop the changefeed, leaving the database with no retention at all
	ds.execute("DEFINE DATABASE OVERWRITE db", &Session::owner().with_ns("ns"), None)
		.await
		.unwrap();
	// A collection pass clears both ranges of the database with no retention
	ds.changefeed_cleanup(2).await.unwrap();
	assert!(
		change_keys(&ds, "ns", "db").await.is_empty(),
		"changefeed entries survived the loss of their retention"
	);
	assert!(
		ts_keys(&ds, "ns", "db").await.is_empty(),
		"versionstamp timestamps survived the loss of their retention"
	);
	// The database which still keeps a changefeed is inside its window
	assert_eq!(change_keys(&ds, "ns", "keep").await, kept_changes);
	assert_eq!(ts_keys(&ds, "ns", "keep").await, kept_ts);
}

/// A backlog larger than one page is collected, in pages.
///
/// A `!ts` key is written for every database that keeps a changefeed on every
/// tick and nothing else ever deletes them, so the range a database that drops
/// its changefeed leaves behind is bounded by how long it kept one rather than
/// by anything the catalog holds. Collecting it in one transaction would put
/// years of accumulation into a single write set.
pub async fn a_backlog_larger_than_one_page_is_collected(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("2c3d4e5f-6071-4823-9b4c-5d6e7f8091a2").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database which keeps a changefeed
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	// More ticks than one collected page holds
	const TICKS: u64 = 2_500;
	for ts in 1..=TICKS {
		ds.changefeed_versionstamp(ts).await.unwrap();
	}
	assert_eq!(ts_keys(&ds, "ns", "db").await.len(), TICKS as usize);
	// Drop the changefeed, leaving the whole backlog stale
	ds.execute("DEFINE DATABASE OVERWRITE db", &Session::owner().with_ns("ns"), None)
		.await
		.unwrap();
	// One collection pass drains all of it
	ds.changefeed_cleanup(TICKS + 1).await.unwrap();
	assert!(ts_keys(&ds, "ns", "db").await.is_empty(), "the backlog was not collected");
	assert!(change_keys(&ds, "ns", "db").await.is_empty());
}

/// An empty range is not planned for collection.
///
/// A planned range is drained in write transactions of its own, each of which
/// re-reads the catalog before it deletes anything. A database which defines no
/// changefeed — the default — has nothing to collect and would otherwise plan
/// two such ranges on every tick, on every node, for as long as the deployment
/// runs. Nothing takes a lease for this pass, so that cost is paid per node.
pub async fn an_empty_range_is_not_planned(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("93a4b5c6-d7e8-49f0-a1b2-c3d4e5f60718").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database with data but no changefeed
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db; USE DB db;
		 DEFINE TABLE person; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	let plan = plan_at(&ds, 1_577_836_800).await;
	assert!(
		plan.is_empty(),
		"{} ranges were planned for a database which holds nothing to collect",
		plan.len()
	);
	// A changefeed which records entries plans the range that holds them
	let sess = Session::owner().with_ns("ns").with_db("db");
	ds.execute("DEFINE TABLE other CHANGEFEED 0s; CREATE other:1", &sess, None).await.unwrap();
	assert!(!change_keys(&ds, "ns", "db").await.is_empty(), "no change was recorded");
	let plan = plan_at(&ds, 1_577_836_800).await;
	assert!(!plan.is_empty(), "the range holding the changes was not planned");
}

/// `SHOW CHANGES SINCE <datetime>` earlier than the timeline reads from its
/// start rather than failing.
///
/// The lookup finds the greatest recorded timestamp at or before the one asked
/// for, so it finds nothing exactly when the request predates every timestamp
/// the database has recorded — which is what a changefeed defined less than one
/// collection tick ago looks like. The changes from that point on are the whole
/// of what is retained, and they are plainly there to be returned.
pub async fn changes_since_before_the_timeline_read_from_its_start(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("4e5f6071-8293-44a5-b6c7-d8e9f0a1b2c3").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A changefeed with entries in it, and no tick of the timestamp writer
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	assert!(ts_keys(&ds, "ns", "db").await.is_empty(), "the timeline should still be empty");
	assert!(!change_keys(&ds, "ns", "db").await.is_empty());
	// A datetime before anything the database recorded reads the retained changes
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds
		.execute(
			"SHOW CHANGES FOR TABLE person SINCE d\"2020-01-01T00:00:00Z\" LIMIT 10",
			&sess,
			None,
		)
		.await
		.unwrap();
	let val = res[0].result.as_ref().expect("a datetime before the timeline must not error");
	assert!(!val.is_empty_array(), "the retained changes were not returned: {val}");
}

/// `SHOW CHANGES SINCE <datetime>` starts at the recorded timestamp at or
/// before it, not at the datetime itself.
///
/// A database records a timestamp once per collection tick, so that is the
/// resolution the whole statement has: a request lands on the tick before it
/// and returns the changes from there, including ones that happened before the
/// request. This is what bounds the answer for a database that has recorded no
/// timestamp at all, where the scan starts at the beginning of the range: such
/// a database is younger than one tick, so the changes before the request that
/// it returns span the same tick every other answer may reach back over.
pub async fn changes_since_a_datetime_start_at_the_tick_before_it(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("718293a4-b5c6-47d8-e9f0-a1b2c3d4e5f6").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, kvs) = new_ds.create_ds(node_id, clock).await;
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	// A tick at 2020-01-01T00:00:00Z, then a change after it
	ds.changefeed_versionstamp(1_577_836_800).await.unwrap();
	let sess = Session::owner().with_ns("ns").with_db("db");
	ds.execute("CREATE person:2", &sess, None).await.unwrap();
	// A request half a minute after that tick still reads from the tick
	let res = ds
		.execute(
			"SHOW CHANGES FOR TABLE person SINCE d\"2020-01-01T00:00:30Z\" LIMIT 10",
			&sess,
			None,
		)
		.await
		.unwrap();
	let val = res[0].result.as_ref().expect("the request must not error").to_string();
	assert!(
		val.contains("person:2"),
		"a change made after the tick the request lands on was not returned: {val}"
	);
	// What a recorded timestamp is worth is not the same on every backend.
	// FoundationDB records the read version its transaction was given, while a
	// change key takes the commit version assigned to it afterwards, so a
	// timestamp names a point before changes that were already committed when
	// it was written, and the tick does not bound where a request starts. That
	// gap is in the mapping rather than in this statement.
	if !matches!(kvs, Kvs::Fdb) {
		assert!(
			!val.contains("person:1"),
			"the request read from before the tick it lands on: {val}"
		);
	}
}

/// A changefeed of no retention keeps a timeline like any other.
///
/// `CHANGEFEED 0s` records a change entry per write and keeps it until the
/// next collection, so a request against it is placed on the timeline the same
/// way. Recording a timestamp only where a retention window is non-zero would
/// leave such a database with no timeline at all, and every
/// `SHOW CHANGES SINCE <datetime>` against it would answer with the whole of
/// what is retained rather than with the window asked for.
pub async fn changes_since_a_datetime_are_placed_without_retention(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("8293a4b5-c6d7-48e9-f0a1-b2c3d4e5f607").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, kvs) = new_ds.create_ds(node_id, clock).await;
	// A table whose changefeed keeps nothing, and a change under it
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db; USE DB db;
		 DEFINE TABLE person CHANGEFEED 0s; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	assert!(!change_keys(&ds, "ns", "db").await.is_empty(), "no change was recorded");
	// A tick at 2020-01-01T00:00:00Z records the timeline, then a change after it
	ds.changefeed_versionstamp(1_577_836_800).await.unwrap();
	assert!(
		!ts_keys(&ds, "ns", "db").await.is_empty(),
		"a changefeed of no retention was left off the timeline"
	);
	let sess = Session::owner().with_ns("ns").with_db("db");
	ds.execute("CREATE person:2", &sess, None).await.unwrap();
	// A request after that tick reads from the tick, not from the whole feed
	let res = ds
		.execute(
			"SHOW CHANGES FOR TABLE person SINCE d\"2020-01-01T00:00:30Z\" LIMIT 10",
			&sess,
			None,
		)
		.await
		.unwrap();
	let val = res[0].result.as_ref().expect("the request must not error").to_string();
	assert!(
		val.contains("person:2"),
		"a change made after the tick the request lands on was not returned: {val}"
	);
	// FoundationDB records the read version its transaction was given rather than
	// the commit version a change key takes, so a timestamp there names a point
	// before changes that were already committed when it was written
	if !matches!(kvs, Kvs::Fdb) {
		assert!(
			!val.contains("person:1"),
			"the request read from before the tick it lands on: {val}"
		);
	}
}

/// `SHOW CHANGES SINCE <datetime>` later than anything that can have happened
/// reads nothing.
///
/// A database which has recorded no timestamp at all places no request on its
/// timeline, so the lookup answers the same way for a request from before its
/// changes and for one from after them. Reading from the start of the range in
/// both cases would return the whole feed to a request no change can yet
/// satisfy.
pub async fn changes_since_after_an_empty_timeline_read_nothing(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("5f607182-93a4-45b6-c7d8-e9f0a1b2c3d4").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A changefeed with entries in it, and no tick of the timestamp writer
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	assert!(ts_keys(&ds, "ns", "db").await.is_empty(), "the timeline should still be empty");
	assert!(!change_keys(&ds, "ns", "db").await.is_empty());
	// A datetime nothing recorded can be at or after returns no changes
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds
		.execute(
			"SHOW CHANGES FOR TABLE person SINCE d\"2999-01-01T00:00:00Z\" LIMIT 10",
			&sess,
			None,
		)
		.await
		.unwrap();
	let val = res[0].result.as_ref().expect("a datetime after the timeline must not error");
	assert!(val.is_empty_array(), "changes from before the request were returned: {val}");
}

/// A collection page stops when the retention it was planned under has moved.
///
/// A range is planned in a transaction that is cancelled before any of it is
/// drained. A `DEFINE ... CHANGEFEED` committing in between puts entries the
/// database now retains inside a range already planned for deletion — whether
/// the window went from nothing to something, or merely got longer — so every
/// page re-reads the retention in the transaction that deletes.
pub async fn collection_stops_when_the_retention_moves(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("6071a2b3-c4d5-46e7-a8b9-c0d1e2f30415").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database which keeps a changefeed and has entries in it
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	ds.changefeed_versionstamp(1).await.unwrap();
	let changes = change_keys(&ds, "ns", "db").await;
	let ts = ts_keys(&ds, "ns", "db").await;
	assert!(!changes.is_empty());
	assert!(!ts.is_empty());
	// Drain the ranges a zero-retention plan would have produced. This is the
	// state left behind when the retention comes back after the plan.
	let fence = crate::cf::Fence {
		ns: "ns".to_owned(),
		db: "db".to_owned(),
		retention: Duration::ZERO,
		id: None,
		watermark: None,
	};
	let rng = crate::cf::change_range("ns", "db", crate::vs::VersionStamp::MAX).unwrap();
	ds.changefeed_collect(rng, fence.clone()).await.unwrap();
	let rng = crate::cf::timestamp_range("ns", "db").unwrap();
	ds.changefeed_collect(rng, fence).await.unwrap();
	// Nothing was collected, because the database keeps a changefeed again
	assert_eq!(change_keys(&ds, "ns", "db").await, changes, "live changes were collected");
	assert_eq!(ts_keys(&ds, "ns", "db").await, ts, "a live timeline was collected");
	// Now the same with a window that merely got longer. The plan below was made
	// under a one hour retention, and the database keeps a day.
	ds.execute("DEFINE DATABASE OVERWRITE db CHANGEFEED 1d", &Session::owner().with_ns("ns"), None)
		.await
		.unwrap();
	let fence = crate::cf::Fence {
		ns: "ns".to_owned(),
		db: "db".to_owned(),
		retention: Duration::from_secs(3600),
		id: None,
		watermark: None,
	};
	let rng = crate::cf::change_range("ns", "db", crate::vs::VersionStamp::MAX).unwrap();
	ds.changefeed_collect(rng, fence).await.unwrap();
	assert_eq!(
		change_keys(&ds, "ns", "db").await,
		changes,
		"changes a longer window retains were collected"
	);
}

/// A plan made against one database is not drained against its replacement.
///
/// Names are reused on this line, so a database removed and defined again
/// answers to the same name. Everything else the fence reads can be reproduced:
/// the replacement can carry the same retention, and because its versionstamps
/// restart it can write the same timeline entry inside the second a watermark
/// names. Its catalog id cannot be reproduced, because ids are drawn from a
/// sequence nothing ever returns one to.
pub async fn a_plan_is_not_drained_against_a_replacement(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("a4f4e7e9-3a09-4c62-b5d8-192002d81920").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database which keeps a changefeed and has entries in it
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	ds.changefeed_versionstamp(1).await.unwrap();
	// The id the plan is made against, and one that is not it
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let id = txn.get_db("ns", "db").await.unwrap().id.expect("the database carries no id");
	txn.cancel().await.unwrap();
	let changes = change_keys(&ds, "ns", "db").await;
	let ts = ts_keys(&ds, "ns", "db").await;
	assert!(!changes.is_empty());
	assert!(!ts.is_empty());
	// A plan carrying the id of some other generation of the name, with the
	// retention the database really has, so only the identity check can stop it
	let fence = crate::cf::Fence {
		ns: "ns".to_owned(),
		db: "db".to_owned(),
		retention: Duration::from_secs(3600),
		id: Some(id.wrapping_add(1)),
		watermark: None,
	};
	let rng = crate::cf::change_range("ns", "db", crate::vs::VersionStamp::MAX).unwrap();
	ds.changefeed_collect(rng, fence.clone()).await.unwrap();
	let rng = crate::cf::timestamp_range("ns", "db").unwrap();
	ds.changefeed_collect(rng, fence).await.unwrap();
	// Nothing was collected
	assert_eq!(change_keys(&ds, "ns", "db").await, changes, "another database's changes went");
	assert_eq!(ts_keys(&ds, "ns", "db").await, ts, "another database's timeline went");
	// The same plan against the database it was made for drains it
	let fence = crate::cf::Fence {
		ns: "ns".to_owned(),
		db: "db".to_owned(),
		retention: Duration::from_secs(3600),
		id: Some(id),
		watermark: None,
	};
	let rng = crate::cf::change_range("ns", "db", crate::vs::VersionStamp::MAX).unwrap();
	ds.changefeed_collect(rng, fence.clone()).await.unwrap();
	let rng = crate::cf::timestamp_range("ns", "db").unwrap();
	ds.changefeed_collect(rng, fence).await.unwrap();
	assert!(change_keys(&ds, "ns", "db").await.is_empty(), "the planned changes stayed");
	assert!(ts_keys(&ds, "ns", "db").await.is_empty(), "the planned timeline stayed");
}

/// A planned range names its database the way the catalog is keyed.
///
/// A database record carries its name as an `Ident`, whose `Display` escapes
/// anything that would not parse bare, so a name like `test-db` renders with
/// backticks. A page re-reads the retention by the names its fence carries, so
/// a fence built from the displayed form looks up a database that does not
/// exist, reads no retention for it, and stops — collecting nothing and
/// reporting success.
pub async fn a_plan_names_the_database_the_catalog_is_keyed_by(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("8293a4b5-c6d7-48e9-9a0b-1c2d3e4f5061").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Names which do not survive a round trip through `Ident`'s `Display`
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE `test-ns`;
		 USE NS `test-ns`; DEFINE DATABASE `test-db` CHANGEFEED 1h; USE DB `test-db`;
		 DEFINE TABLE person; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	ds.changefeed_versionstamp(1).await.unwrap();
	// Drop the changefeed, so that what it left behind is planned for collection
	ds.execute("DEFINE DATABASE OVERWRITE `test-db`", &Session::owner().with_ns("test-ns"), None)
		.await
		.unwrap();
	// Plan a pass, and read the names the pages would look the database up by
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let plan = crate::cf::gc_all_at(&txn, 2).await.unwrap();
	txn.cancel().await.unwrap();
	assert!(!plan.is_empty(), "nothing was planned for a database which retains nothing");
	for (_, fence) in &plan {
		assert_eq!(
			(fence.ns.as_str(), fence.db.as_str()),
			("test-ns", "test-db"),
			"a planned range names a database no lookup will find"
		);
	}
	// And the pass those ranges belong to collects them
	ds.changefeed_cleanup(2).await.unwrap();
	assert!(change_keys(&ds, "test-ns", "test-db").await.is_empty());
	assert!(ts_keys(&ds, "test-ns", "test-db").await.is_empty());
}

/// A collection page stops when the database it planned for was replaced.
///
/// A database removed and defined again under the same name and retention
/// answers the retention check as though it were the one planned. Its
/// versionstamps restart at the bottom of the range the plan cut, so the events
/// it writes next sit inside a range chosen to delete the events of an object
/// that no longer exists. What tells them apart is the timeline the watermark
/// came from, which was destroyed with the rest of the predecessor's prefix.
pub async fn collection_stops_when_the_database_was_replaced(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("a4b5c6d7-e8f9-4a0b-9c1d-2e3f40516273").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database which keeps a changefeed, with a timeline to cut against
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:1",
		&root,
		None,
	)
	.await
	.unwrap();
	ds.changefeed_versionstamp(1).await.unwrap();
	// Plan a pass past the retention window
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let plan = crate::cf::gc_all_at(&txn, 3601).await.unwrap();
	txn.cancel().await.unwrap();
	let (rng, fence) = plan
		.into_iter()
		.find(|(_, f)| f.watermark.is_some())
		.expect("no range was cut at a watermark");
	// Remove the database and define it again, the same in every way the
	// catalog can report
	let ns = Session::owner().with_ns("ns");
	ds.execute("REMOVE DATABASE db", &ns, None).await.unwrap();
	ds.execute(
		"DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
		 DEFINE TABLE person CHANGEFEED 1h; CREATE person:2",
		&ns,
		None,
	)
	.await
	.unwrap();
	let changes = change_keys(&ds, "ns", "db").await;
	assert!(!changes.is_empty(), "the replacement wrote no changes to lose");
	// Drain the range that was cut for the database which is gone
	ds.changefeed_collect(rng, fence).await.unwrap();
	assert_eq!(
		change_keys(&ds, "ns", "db").await,
		changes,
		"a plan for the predecessor collected the replacement's changes"
	);
}

/// One collection of a range stops when it has deleted what a pass may.
///
/// A changefeed defined with no retention keeps nothing, so its range is
/// planned open at the top — and it still records a change for every write, so
/// that top moves for as long as the writes continue. A collection that only
/// stopped when the range was drained would never stop against a database
/// written to at the rate it deletes, would never reach the databases planned
/// behind it, and would hold the task that awaits it past a shutdown.
pub async fn one_collection_stops_at_what_a_pass_may_delete(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b5c6d7e8-f901-4a2b-8c3d-4e5f60718293").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A changefeed which retains nothing, and writes under it
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns;
		 USE NS ns; DEFINE DATABASE db; USE DB db;
		 DEFINE TABLE person CHANGEFEED 0s",
		&root,
		None,
	)
	.await
	.unwrap();
	// A change key is named by the versionstamp it was written at, so each
	// write needs a transaction of its own to leave a key of its own
	let sess = Session::owner().with_ns("ns").with_db("db");
	for i in 1..=20 {
		ds.execute(&format!("CREATE person:{i}"), &sess, None).await.unwrap();
	}
	let before = change_keys(&ds, "ns", "db").await.len();
	assert!(before > 8, "a zero retention changefeed recorded too little to bound: {before}");
	// The range planned for it is open at the top
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let plan = crate::cf::gc_all_at(&txn, 1).await.unwrap();
	txn.cancel().await.unwrap();
	let (rng, fence) = plan
		.into_iter()
		.find(|(_, f)| f.retention.is_zero() && f.db == "db")
		.expect("no range was planned for the database with no retention");
	// A collection allowed fewer keys than it holds returns having spent them
	ds.changefeed_collect_within(rng, fence, 4).await.unwrap();
	let after = change_keys(&ds, "ns", "db").await.len();
	assert!(
		after >= before - 4,
		"one collection deleted {} keys against a budget of 4",
		before - after
	);
	assert!(after > 0, "the collection did not stop at its budget");
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn versionstamp_timestamps_follow_changefeed_retention() {
			super::changefeed_gc::versionstamp_timestamps_follow_changefeed_retention($new_ds)
				.await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_plan_is_not_drained_against_a_replacement() {
			super::changefeed_gc::a_plan_is_not_drained_against_a_replacement($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn dropping_a_changefeed_collects_what_it_left_behind() {
			super::changefeed_gc::dropping_a_changefeed_collects_what_it_left_behind($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_backlog_larger_than_one_page_is_collected() {
			super::changefeed_gc::a_backlog_larger_than_one_page_is_collected($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn changes_since_before_the_timeline_read_from_its_start() {
			super::changefeed_gc::changes_since_before_the_timeline_read_from_its_start($new_ds)
				.await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn an_empty_range_is_not_planned() {
			super::changefeed_gc::an_empty_range_is_not_planned($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn changes_since_after_an_empty_timeline_read_nothing() {
			super::changefeed_gc::changes_since_after_an_empty_timeline_read_nothing($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn changes_since_a_datetime_are_placed_without_retention() {
			super::changefeed_gc::changes_since_a_datetime_are_placed_without_retention($new_ds)
				.await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn changes_since_a_datetime_start_at_the_tick_before_it() {
			super::changefeed_gc::changes_since_a_datetime_start_at_the_tick_before_it($new_ds)
				.await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn collection_stops_when_the_retention_moves() {
			super::changefeed_gc::collection_stops_when_the_retention_moves($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_plan_names_the_database_the_catalog_is_keyed_by() {
			super::changefeed_gc::a_plan_names_the_database_the_catalog_is_keyed_by($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn collection_stops_when_the_database_was_replaced() {
			super::changefeed_gc::collection_stops_when_the_database_was_replaced($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn one_collection_stops_at_what_a_pass_may_delete() {
			super::changefeed_gc::one_collection_stops_at_what_a_pass_may_delete($new_ds).await;
		}
	};
}
pub(crate) use define_tests;

/// Changefeed collection against a store which retains versions.
///
/// The shared harness builds a `surrealkv:` datastore, which keeps none, so
/// this builds its own `surrealkv+versioned:` one. CI's workspace test enables
/// only `storage-mem`, so it runs under `--features kv-surrealkv`.
#[cfg(feature = "kv-surrealkv")]
mod versioned {
	use crate::dbs::Session;
	use crate::kvs::{Datastore, Key, KeyEncode, LockType::*, TransactionType::*};
	use temp_dir::TempDir;

	/// Every version a database's changefeed keys hold.
	async fn versions_under(ds: &Datastore, prefix: &Key) -> usize {
		let mut end = prefix.clone();
		end.push(0xff);
		let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
		let res = match tx.scan_all_versions(prefix.clone()..end, u32::MAX).await {
			Ok(rows) => rows.len(),
			Err(crate::err::Error::UnsupportedVersionedQueries) => 0,
			Err(e) => panic!("scanning versions failed: {e}"),
		};
		tx.cancel().await.unwrap();
		res
	}

	/// Collecting a changefeed destroys the versions of what it collects.
	///
	/// A timestamp key is named by the second it records and a change key by
	/// the versionstamp it was written at, so neither is ever written twice.
	/// Deleting one visibly leaves every version it ever held on disk, and
	/// nothing will overwrite it, so a database that drops its changefeed after
	/// years of ticks would keep all of them: the collection would report the
	/// range clear and reclaim nothing.
	#[tokio::test]
	#[serial_test::serial]
	async fn collection_destroys_the_versions_of_what_it_collects() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		// A database with a changefeed and a backlog of ticks behind it
		let root = Session::owner();
		ds.execute(
			"DEFINE NAMESPACE ns;
			 USE NS ns; DEFINE DATABASE db CHANGEFEED 1h; USE DB db;
			 DEFINE TABLE person; CREATE person:1, person:2",
			&root,
			None,
		)
		.await
		.unwrap();
		for ts in 1..=8 {
			ds.changefeed_versionstamp(ts).await.unwrap();
		}
		let prefix: Key = crate::key::database::all::new("ns", "db").encode().unwrap();
		let before = versions_under(&ds, &prefix).await;
		assert!(before > 0, "the store retained no history, so this proves nothing");
		// Drop the changefeed, leaving the whole range stale
		ds.execute("DEFINE DATABASE OVERWRITE db", &Session::owner().with_ns("ns"), None)
			.await
			.unwrap();
		ds.changefeed_cleanup(9).await.unwrap();
		// What the collection took is gone from disk, not merely hidden
		let ts_prefix: Key = crate::key::database::ts::prefix("ns", "db").unwrap();
		assert_eq!(
			versions_under(&ds, &ts_prefix).await,
			0,
			"the timestamps were tombstoned rather than destroyed"
		);
	}
}
