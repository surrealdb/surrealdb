//! Deferred reclaim of removed namespaces and databases.
//!
//! `REMOVE NAMESPACE` and `REMOVE DATABASE` delete only the catalog definition
//! and enqueue the orphaned data prefix at `/!rc`. The background reclaim task
//! destroys that prefix out of band, in bounded resumable pages.
//!
//! These tests drive the statements through `Datastore::execute` and then read
//! the raw keys, because the point of the design is precisely the difference
//! between what the catalog reports and what is still on disk.

use std::sync::Arc;
use std::time::Duration;

use uuid::Uuid;

use super::{CreateDs, Kvs};
use crate::ctx::MutableContext;
use crate::dbs::node::Timestamp;
use crate::dbs::{Options, Session};
use crate::iam::{Auth, Role};
use crate::key::index::iu::IndexCountKey;
use crate::key::root::ic::IndexCompactionKey;
use crate::key::root::rc::{ReclaimKey, ReclaimKind, ReclaimState};
use crate::kvs::clock::{FakeClock, SizedClock};
use crate::kvs::{Datastore, Key, KeyEncode, LockType::*, TransactionType::*};
use crate::sql::statements::RemoveDatabaseStatement;
use crate::sql::Ident;

/// A reclaim interval long enough that a test never loses the task lease it
/// takes on its first pass.
const INTERVAL: Duration = Duration::from_secs(60);

/// Runs a query as the owner of `ns`, failing the test if any statement errored.
async fn exec(ds: &Datastore, ns: &str, db: Option<&str>, sql: &str) {
	let mut sess = Session::owner().with_ns(ns);
	if let Some(db) = db {
		sess = sess.with_db(db);
	}
	let res = ds.execute(sql, &sess, None).await.unwrap();
	for (n, res) in res.into_iter().enumerate() {
		if let Err(e) = res.result {
			panic!("statement {n} of `{sql}` failed: {e}");
		}
	}
}

/// The key prefix holding every byte of a database's data.
fn db_prefix(ns: &str, db: &str) -> Key {
	crate::key::database::all::new(ns, db).encode().unwrap()
}

/// The key prefix holding every byte of a namespace's data.
fn ns_prefix(ns: &str) -> Key {
	crate::key::namespace::all::new(ns).encode().unwrap()
}

/// Every key currently stored under a prefix, in key order.
async fn keys_under(ds: &Datastore, prefix: &[u8]) -> Vec<Key> {
	let mut end = prefix.to_vec();
	end.push(0xff);
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let res = txn.keys(prefix.to_vec()..end, u32::MAX, None).await.unwrap();
	txn.cancel().await.unwrap();
	res
}

/// How many stored versions a prefix still holds, counting keys whose only
/// versions are deletions.
///
/// A backend which keeps no versions reports none, which is the right answer
/// there: its visible keys are all it holds.
#[cfg(feature = "kv-surrealkv")]
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

/// Enqueues `n` index compaction entries naming one database.
///
/// The entries are what a count index writes when it is updated; nothing here
/// reads their values, only their keys.
async fn seed_index_compaction(ds: &Datastore, ns: &str, db: &str, n: u32) {
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	for i in 0..n {
		let key = IndexCompactionKey::new(
			ns.into(),
			db.into(),
			"person".into(),
			"idx".into(),
			Uuid::from_u128(1),
			Uuid::from_u128(u128::from(i) + 2),
		);
		txn.set(key, vec![0u8], None).await.unwrap();
	}
	txn.commit().await.unwrap();
}

/// Every entry currently in the index compaction queue.
async fn index_compaction_keys(ds: &Datastore) -> Vec<Key> {
	let (beg, end) = IndexCompactionKey::range();
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let res = txn.keys(beg..end, u32::MAX, None).await.unwrap();
	txn.cancel().await.unwrap();
	res
}

/// How many entries a count index holds, and the total they sum to.
///
/// The count of the index is the sum of everything under its range, whether it
/// has been folded into the aggregate entry or is still a delta of its own.
async fn count_index_entries(
	ds: &Datastore,
	ns: &str,
	db: &str,
	tb: &str,
	ix: &str,
) -> (usize, i64) {
	let rng = IndexCountKey::range(ns, db, tb, ix).unwrap();
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let keys = txn.keys(rng, u32::MAX, None).await.unwrap();
	txn.cancel().await.unwrap();
	let mut total = 0i64;
	for k in &keys {
		let iu = IndexCountKey::decode_key(k).unwrap();
		if iu.pos {
			total += iu.count as i64;
		} else {
			total -= iu.count as i64;
		}
	}
	(keys.len(), total)
}

/// How many stored versions a count index's entries still hold.
///
/// A backend which keeps no versions reports none, which is the right answer
/// there: its visible entries are all it holds.
#[cfg(feature = "kv-surrealkv")]
async fn count_index_versions(ds: &Datastore, ns: &str, db: &str, tb: &str, ix: &str) -> usize {
	let rng = IndexCountKey::range(ns, db, tb, ix).unwrap();
	let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
	let res = match tx.scan_all_versions(rng, u32::MAX).await {
		Ok(rows) => rows.len(),
		Err(crate::err::Error::UnsupportedVersionedQueries) => 0,
		Err(e) => panic!("scanning versions failed: {e}"),
	};
	tx.cancel().await.unwrap();
	res
}

/// Every entry currently in the reclaim queue, decoded.
async fn jobs(ds: &Datastore) -> Vec<(ReclaimKey<'static>, ReclaimState)> {
	let (beg, end) = ReclaimKey::range();
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let res = txn.scan(beg..end, u32::MAX, None).await.unwrap();
	txn.cancel().await.unwrap();
	res.iter()
		.map(|(k, v)| {
			let key = ReclaimKey::decode_key(k).unwrap();
			// Detach the decoded key from the scan buffer so that callers may
			// hold it across further transactions
			let key = match key.kind {
				ReclaimKind::Namespace => {
					ReclaimKey::namespace(key.ns.into_owned().into(), key.uid)
				}
				ReclaimKind::Database => ReclaimKey::database(
					key.ns.into_owned().into(),
					key.db.unwrap().into_owned().into(),
					key.uid,
				),
			};
			(key, revision::from_slice(v).unwrap())
		})
		.collect()
}

/// One tick of the changefeed timestamp writer, which correlates a wall clock
/// second with the current versionstamp. It rewrites the database's
/// versionstamp key and adds a timestamp key, both inside the database prefix.
async fn tick(ds: &Datastore, ns: &str, db: &str, ts: u64) {
	let mut tx = ds.transaction(Write, Optimistic).await.unwrap().inner();
	tx.set_timestamp_for_versionstamp(ts, ns, db).await.unwrap();
	tx.commit().await.unwrap();
}

/// Runs reclaim passes with the production bounds until the queue is empty,
/// returning how many keys were destroyed in total.
async fn drain(ds: &Datastore) -> u64 {
	let mut destroyed = 0;
	// Every pass makes progress or the queue is already empty, and the prefixes
	// under test are far smaller than one pass's budget
	for _ in 0..8 {
		let (visited, errors, deleted) =
			ds.reclaim_tombstones_bounded(INTERVAL, 1_000, 100_000).await.unwrap();
		assert_eq!(errors, 0);
		destroyed += deleted;
		if visited == 0 {
			return destroyed;
		}
	}
	panic!("the reclaim queue did not drain");
}

/// A removed database disappears from the catalog the moment its removal
/// commits, while every byte it held is still on disk.
///
/// This is the whole point of the deferred path: the removal's write set covers
/// the catalog and the queue, never the data. A removal that deleted the data
/// prefix inline would fail the byte-for-byte comparison below.
pub async fn remove_database_defers_its_data(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("f0f4a0d5-6d1e-4b0a-9a4c-1d2e3f405162").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, and a sibling which must be left alone
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1, person:2, person:3").await;
	exec(&ds, "ns", Some("keep"), "DEFINE TABLE person; CREATE person:1").await;
	let before = keys_under(&ds, &db_prefix("ns", "db")).await;
	let kept = keys_under(&ds, &db_prefix("ns", "keep")).await;
	assert!(!before.is_empty());
	assert!(!kept.is_empty());
	// Remove the database
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// The catalog no longer knows about it
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let dbs = txn.all_db("ns").await.unwrap();
	assert_eq!(
		dbs.iter().map(|x| x.name.to_string()).collect::<Vec<_>>(),
		vec!["keep".to_string()]
	);
	assert!(matches!(txn.get_db("ns", "db").await, Err(crate::err::Error::DbNotFound { .. })));
	txn.cancel().await.unwrap();
	// Not one byte of its data was touched
	assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await, before);
	assert_eq!(keys_under(&ds, &db_prefix("ns", "keep")).await, kept);
	// Exactly one job names the removed database, unobserved and with no progress
	let jobs = jobs(&ds).await;
	assert_eq!(jobs.len(), 1);
	assert_eq!(jobs[0].0.kind, ReclaimKind::Database);
	assert_eq!(jobs[0].0.ns, "ns");
	assert_eq!(jobs[0].0.db.as_deref(), Some("db"));
	assert_eq!(jobs[0].1, ReclaimState::enqueued());
}

/// The reclaim task destroys the deferred prefix and then removes the job.
pub async fn reclaim_destroys_the_deferred_prefix(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("2a6c8e11-3b4d-4f5a-8c7b-9d0e1f2a3b44").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, and a sibling which must survive the reclaim
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1, person:2, person:3").await;
	exec(&ds, "ns", Some("keep"), "DEFINE TABLE person; CREATE person:1").await;
	let kept = keys_under(&ds, &db_prefix("ns", "keep")).await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(!orphaned.is_empty());
	// The first pass only stamps the job, so nothing is destroyed by it
	let (visited, errors, deleted) =
		ds.reclaim_tombstones_bounded(INTERVAL, 1_000, 100_000).await.unwrap();
	assert_eq!((visited, errors, deleted), (1, 0, 0));
	assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await, orphaned);
	// Draining the queue destroys the prefix exactly once over
	assert_eq!(drain(&ds).await, orphaned.len() as u64);
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
	// The sibling database is untouched, and still readable
	assert_eq!(keys_under(&ds, &db_prefix("ns", "keep")).await, kept);
	exec(&ds, "ns", Some("keep"), "SELECT * FROM person").await;
}

/// A prefix larger than one pass's budget is destroyed over several passes,
/// each of which does a bounded amount of work.
pub async fn reclaim_drains_a_prefix_across_bounded_passes(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("5b7d9f22-4c5e-4061-9d8c-ae1f2b3c4d55").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database holding many more keys than a pass below is allowed to destroy
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE |person:60|").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(orphaned.len() > 16, "expected a prefix worth paging, got {}", orphaned.len());
	// Stamp the job
	ds.reclaim_tombstones_bounded(INTERVAL, 4, 8).await.unwrap();
	// Each pass destroys at most its budget, and the prefix shrinks by exactly
	// that much until it is drained
	let mut remaining = orphaned.len() as u64;
	let mut passes = 0;
	while remaining > 0 {
		let (visited, errors, deleted) =
			ds.reclaim_tombstones_bounded(INTERVAL, 4, 8).await.unwrap();
		assert_eq!((visited, errors), (1, 0));
		assert!(deleted <= 8, "a pass destroyed {deleted} keys, over its budget of 8");
		remaining -= deleted;
		assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await.len() as u64, remaining);
		passes += 1;
		assert!(passes < 64, "the reclaim made no progress");
	}
	// The prefix is gone and the job with it
	assert!(passes > 1, "the prefix was not paged across passes");
	assert!(jobs(&ds).await.is_empty());
}

/// A pass interrupted part-way through a prefix resumes at the cursor it
/// committed, rather than rescanning the prefix from its start.
pub async fn reclaim_resumes_after_an_interrupted_pass(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("8c0e1a33-5d6f-4172-ae9d-bf203c4d5e66").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed and stamped so that it is eligible
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE |person:40|").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(orphaned.len() > 6, "expected a prefix worth paging, got {}", orphaned.len());
	ds.reclaim_tombstones_bounded(INTERVAL, 3, 6).await.unwrap();
	// A pass which spends its budget mid-prefix leaves the rest in place, and
	// commits a cursor naming the last key it destroyed
	let (_, errors, deleted) = ds.reclaim_tombstones_bounded(INTERVAL, 3, 6).await.unwrap();
	assert_eq!((errors, deleted), (0, 6));
	assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await, orphaned[6..].to_vec());
	let job = jobs(&ds).await;
	assert_eq!(job.len(), 1);
	assert_eq!(job[0].1.cursor.as_deref(), Some(orphaned[5].as_slice()));
	// Put a key back below that cursor. While the prefix still holds keys the
	// walk has not reached, the cursor is what keeps the pass off ground it has
	// already destroyed, so this key stands in for every key a restart would
	// re-read.
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.set(orphaned[0].clone(), vec![0u8], None).await.unwrap();
	txn.commit().await.unwrap();
	// The next bounded pass resumes above the cursor rather than restarting, so
	// the key below it is still there while there is still ground above
	let (_, errors, deleted) = ds.reclaim_tombstones_bounded(INTERVAL, 3, 3).await.unwrap();
	assert_eq!((errors, deleted), (0, 3));
	let remaining = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(
		remaining.contains(&orphaned[0]),
		"the pass restarted the prefix instead of resuming above its cursor"
	);
	// Finish the reclaim
	drain(&ds).await;
	// The job is retired only once nothing is left under the prefix, above the
	// cursor or below it
	assert!(
		keys_under(&ds, &db_prefix("ns", "db")).await.is_empty(),
		"a key below the cursor outlived the job that named its prefix"
	);
	assert!(jobs(&ds).await.is_empty());
}

/// Removing a namespace defers the whole namespace prefix, so the databases
/// inside it are reclaimed with it.
pub async fn remove_namespace_covers_its_databases(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("4e5a6b55-7f81-4394-a0bf-d1425e6f7088").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Two populated databases in the namespace to remove, and a namespace which
	// must survive it
	exec(&ds, "ns", Some("one"), "DEFINE TABLE person; CREATE person:1, person:2").await;
	exec(&ds, "ns", Some("two"), "DEFINE TABLE person; CREATE person:1").await;
	exec(&ds, "keep", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	let kept = keys_under(&ds, &ns_prefix("keep")).await;
	assert!(!keys_under(&ds, &db_prefix("ns", "one")).await.is_empty());
	assert!(!keys_under(&ds, &db_prefix("ns", "two")).await.is_empty());
	// Remove the namespace
	exec(&ds, "keep", None, "REMOVE NAMESPACE ns").await;
	// The catalog no longer knows about it, while its data is still on disk
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let all = txn.all_ns().await.unwrap();
	assert_eq!(
		all.iter().map(|x| x.name.to_string()).collect::<Vec<_>>(),
		vec!["keep".to_string()]
	);
	assert!(matches!(txn.get_ns("ns").await, Err(crate::err::Error::NsNotFound { .. })));
	txn.cancel().await.unwrap();
	let orphaned = keys_under(&ds, &ns_prefix("ns")).await;
	assert!(!orphaned.is_empty());
	// One job names the whole namespace prefix, both databases included
	let job = jobs(&ds).await;
	assert_eq!(job.len(), 1);
	assert_eq!(job[0].0.kind, ReclaimKind::Namespace);
	assert_eq!(job[0].0.ns, "ns");
	assert_eq!(job[0].0.db, None);
	// Draining it destroys both databases
	assert_eq!(drain(&ds).await, orphaned.len() as u64);
	assert!(keys_under(&ds, &db_prefix("ns", "one")).await.is_empty());
	assert!(keys_under(&ds, &db_prefix("ns", "two")).await.is_empty());
	assert!(keys_under(&ds, &ns_prefix("ns")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
	// The other namespace is untouched, and still readable
	assert_eq!(keys_under(&ds, &ns_prefix("keep")).await, kept);
	exec(&ds, "keep", Some("db"), "SELECT * FROM person").await;
}

/// A cancelled removal leaves the database in place and enqueues nothing, so a
/// rolled back statement can never orphan data.
pub async fn remove_database_rolled_back_leaves_no_job(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("7a8b9c66-8092-44a5-b1c0-e2536f708199").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1, person:2").await;
	let before = keys_under(&ds, &db_prefix("ns", "db")).await;
	// Remove it inside a transaction which is then cancelled
	let sess = Session::owner().with_ns("ns");
	ds.execute("BEGIN; REMOVE DATABASE db; CANCEL;", &sess, None).await.unwrap();
	// The database is still in the catalog, its data still on disk, and no job
	// names a prefix that is still reachable
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(txn.get_db("ns", "db").await.is_ok());
	txn.cancel().await.unwrap();
	assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await, before);
	assert!(jobs(&ds).await.is_empty());
	// The database is still usable
	exec(&ds, "ns", Some("db"), "SELECT * FROM person").await;
}

/// Reusing the name of a removed database destroys what the pending reclaim
/// held for it, so the new database starts empty.
///
/// A database addresses its data by name, so a job still naming one describes
/// bytes the new database would otherwise read as its own, and that the reclaim
/// task would later destroy out from under it.
pub async fn reusing_a_database_name_finishes_its_reclaim(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("3c4d5e88-a2b4-46c7-83e2-04758192a30b").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	assert_eq!(jobs(&ds).await.len(), 1);
	// Using the name again finds nothing of what it held
	exec(&ds, "ns", None, "DEFINE DATABASE db").await;
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds.execute("INFO FOR DB; SELECT * FROM person", &sess, None).await.unwrap();
	let info = res[0].result.as_ref().unwrap().to_string();
	assert!(
		info.contains("tables: {  }"),
		"the recreated database inherited the removed database's tables: {info}"
	);
	assert_eq!(
		res[1].result.as_ref().unwrap().to_string(),
		"[]",
		"the recreated database inherited the removed database's records"
	);
	// Nothing is left queued to destroy the new database's prefix
	assert!(jobs(&ds).await.is_empty());
	// The new database keeps what is written to it, across a reclaim pass
	exec(&ds, "ns", Some("db"), "CREATE person:2 SET name = 'new'").await;
	drain(&ds).await;
	let res = ds.execute("SELECT VALUE name FROM person", &sess, None).await.unwrap();
	assert_eq!(res[0].result.as_ref().unwrap().to_string(), "['new']");
}

/// A removed database's rows are unreachable through its name from the moment
/// the removal commits, whether or not a reclaim has run.
///
/// Data is addressed by name, so the deferred prefix is only out of reach
/// because every path to it resolves the database through the catalog first.
/// This is the contract that makes deferring the destruction sound at all, and
/// it is asserted here across a read, a count, a record lookup, a range, and
/// the table list.
pub async fn a_removed_databases_rows_are_unreachable(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("81920a99-c4d6-48e9-94f4-2697a4b5c62d").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// The data is still on disk, which is what makes the assertions below mean
	// something: they are about reachability, not about physical presence
	assert!(!keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	// No shape of read reaches what the removal took away
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds
		.execute(
			"SELECT * FROM person;
			 SELECT * FROM person:1;
			 SELECT * FROM person:1..9;
			 SELECT count() FROM person GROUP ALL;
			 INFO FOR DB",
			&sess,
			None,
		)
		.await
		.unwrap();
	let out = |n: usize| res[n].result.as_ref().unwrap().to_string();
	assert_eq!(out(0), "[]", "a table scan reached the removed database's records");
	assert_eq!(out(1), "[]", "a record lookup reached the removed database's records");
	assert_eq!(out(2), "[]", "a range scan reached the removed database's records");
	assert_eq!(out(3), "[{ count: 0 }]", "a count reached the removed database's records");
	assert!(
		out(4).contains("tables: {  }"),
		"the removed database's tables are still listed: {}",
		out(4)
	);
}

/// A removed database's users and access methods stop resolving with its
/// removal, even though their definitions are still on disk.
///
/// Credentials live inside the database prefix, so revoking access by removing
/// the database has to take effect at the removal, not at the reclaim.
pub async fn a_removed_databases_credentials_stop_resolving(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("c4d6a099-8192-4f4e-9726-a4b5c62d8193").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database with a user and an access method of its own
	exec(
		&ds,
		"ns",
		Some("db"),
		"DEFINE USER auditor ON DATABASE PASSWORD 'secret' ROLES VIEWER;
		 DEFINE ACCESS api ON DATABASE TYPE JWT ALGORITHM HS512 KEY 'secret'",
	)
	.await;
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(txn.get_db_user("ns", "db", "auditor").await.is_ok());
	assert!(txn.get_db_access("ns", "db", "api").await.is_ok());
	assert_eq!(txn.all_db_users("ns", "db").await.unwrap().len(), 1);
	txn.cancel().await.unwrap();
	// Remove the database
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// The definitions are still on disk, and no longer resolve
	assert!(!keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(matches!(
		txn.get_db_user("ns", "db", "auditor").await,
		Err(crate::err::Error::UserDbNotFound { .. })
	));
	assert!(matches!(
		txn.get_db_access("ns", "db", "api").await,
		Err(crate::err::Error::AccessDbNotFound { .. })
	));
	assert!(txn.all_db_users("ns", "db").await.unwrap().is_empty());
	assert!(txn.all_db_accesses("ns", "db").await.unwrap().is_empty());
	txn.cancel().await.unwrap();
}

/// A namespace user stops resolving with the removal of its namespace.
pub async fn a_removed_namespaces_credentials_stop_resolving(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("d68193a0-99c4-4e94-b5c6-2d8192a4f47e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A namespace with a user of its own, and one which must be left alone
	let root = Session::owner();
	ds.execute(
		"DEFINE NAMESPACE ns; DEFINE NAMESPACE keep;
		 USE NS ns; DEFINE USER auditor ON NAMESPACE PASSWORD 'secret' ROLES VIEWER;
		 USE NS keep; DEFINE USER auditor ON NAMESPACE PASSWORD 'secret' ROLES VIEWER",
		&root,
		None,
	)
	.await
	.unwrap();
	ds.execute("REMOVE NAMESPACE ns", &root, None).await.unwrap();
	// The definition is still on disk, and no longer resolves
	assert!(!keys_under(&ds, &ns_prefix("ns")).await.is_empty());
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(matches!(
		txn.get_ns_user("ns", "auditor").await,
		Err(crate::err::Error::UserNsNotFound { .. })
	));
	assert!(txn.get_ns_user("keep", "auditor").await.is_ok());
	txn.cancel().await.unwrap();
}

/// A write through the name of a removed database creates the database again,
/// and what it writes survives the reclaim.
///
/// The write path resolves the database through the catalog before it builds a
/// data key, so the pending reclaim is finished before the record is written
/// rather than after it.
pub async fn writes_through_a_removed_name_survive_the_reclaim(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("2d819200-a4f4-4e7e-93a0-99c4b5c62d81").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// A write through the removed name
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds.execute("CREATE person:99 SET name = 'new'", &sess, None).await.unwrap();
	assert!(res[0].result.is_ok(), "a write through the removed name failed: {:?}", res[0].result);
	// The database exists again, nothing is queued against it, and the removed
	// database's records did not come back with it
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(txn.get_db("ns", "db").await.is_ok());
	txn.cancel().await.unwrap();
	assert!(jobs(&ds).await.is_empty());
	// The write is still there after the reclaim task has run
	drain(&ds).await;
	let res = ds.execute("SELECT VALUE name FROM person", &sess, None).await.unwrap();
	assert_eq!(
		res[0].result.as_ref().unwrap().to_string(),
		"['new']",
		"the reclaim destroyed a record written after the removal"
	);
}

/// A write through the name of a removed namespace creates the namespace again,
/// and what it writes survives the reclaim.
///
/// `REMOVE NAMESPACE` deletes only the root-level definition, so the database
/// definitions inside the namespace prefix outlive it. Reading one back is not
/// evidence that the database exists, and a write that trusted one would land
/// in a prefix the reclaim task is about to destroy.
pub async fn writes_through_a_removed_namespace_survive_the_reclaim(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("8192a4f4-7e2d-4093-99c4-b5c62d819200").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated namespace, removed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	let root = Session::owner();
	ds.execute("REMOVE NAMESPACE ns", &root, None).await.unwrap();
	// The database definition inside it is still on disk, and no longer resolves
	assert!(!keys_under(&ds, &ns_prefix("ns")).await.is_empty());
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(matches!(txn.get_db("ns", "db").await, Err(crate::err::Error::DbNotFound { .. })));
	txn.cancel().await.unwrap();
	// A write through the removed namespace
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds.execute("CREATE person:99 SET name = 'new'", &sess, None).await.unwrap();
	assert!(res[0].result.is_ok(), "a write through the removed name failed: {:?}", res[0].result);
	// The namespace exists again and nothing is queued against it
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(txn.ns_exists("ns").await.unwrap());
	txn.cancel().await.unwrap();
	assert!(jobs(&ds).await.is_empty());
	// The write is still there after the reclaim task has run
	drain(&ds).await;
	let res = ds.execute("SELECT VALUE name FROM person", &sess, None).await.unwrap();
	assert_eq!(
		res[0].result.as_ref().unwrap().to_string(),
		"['new']",
		"the reclaim destroyed a record written after the removal"
	);
}

/// A database inside a removed namespace is absent, and defining it says so.
///
/// `DEFINE DATABASE` reads the catalog to decide whether the name is taken, and
/// the row it reads lives inside the namespace prefix that outlives the removal.
/// Trusting that row would report a database that already exists, or report the
/// success of an `IF NOT EXISTS` which created nothing.
pub async fn defining_a_database_inside_a_removed_namespace_creates_it(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("00819200-a4f4-4e7e-93a0-99c4b5c62d82").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated namespace, removed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	let root = Session::owner();
	ds.execute("REMOVE NAMESPACE ns", &root, None).await.unwrap();
	// Defining the namespace and the database again succeeds, and both are real
	exec(&ds, "ns", None, "DEFINE DATABASE IF NOT EXISTS db").await;
	let res = ds.execute("INFO FOR ROOT", &root, None).await.unwrap();
	let info = res[0].result.as_ref().unwrap().to_string();
	assert!(info.contains("ns"), "the namespace was not created: {info}");
	let sess = Session::owner().with_ns("ns");
	let res = ds.execute("INFO FOR NS", &sess, None).await.unwrap();
	let info = res[0].result.as_ref().unwrap().to_string();
	assert!(info.contains("db"), "the database was not created: {info}");
	// And the records of the removed namespace did not come with it
	let sess = sess.with_db("db");
	let res = ds.execute("SELECT * FROM person", &sess, None).await.unwrap();
	assert_eq!(res[0].result.as_ref().unwrap().to_string(), "[]");
}

/// A deferred removal marks its transaction, and the cache is dropped after it.
///
/// The datastore cache holds database and table definitions under keys carrying
/// no version of their own, so nothing invalidates them but a clear, and every
/// write resolves its database through one of them before it reaches the
/// catalog. A removal clears the cache as it runs, but a query running alongside
/// it resolves from a snapshot taken before the removal and puts what it found
/// back, and a write hitting such an entry lands in a prefix the reclaim task is
/// about to destroy. The mark is what tells the caller committing the removal to
/// clear the cache again once it has.
pub async fn a_deferred_removal_marks_its_transaction(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("6f708192-a3b4-4c5d-9e6f-708192a3b4c5").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	// An ordinary write leaves the mark alone
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.set(b"/test".to_vec(), b"v".to_vec(), None).await.unwrap();
	assert!(!txn.deferred_reclaim(), "an ordinary write marked its transaction");
	txn.cancel().await.unwrap();
	// Deferring a database marks it
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.del_db_deferred("ns", "db").await.unwrap();
	assert!(txn.deferred_reclaim(), "a deferred database removal left its transaction unmarked");
	txn.cancel().await.unwrap();
	// And so does deferring a namespace
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.del_ns_deferred("ns").await.unwrap();
	assert!(txn.deferred_reclaim(), "a deferred namespace removal left its transaction unmarked");
	txn.cancel().await.unwrap();
	// A removal run the ordinary way leaves nothing cached under the name
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	let cache = ds.get_cache();
	assert!(
		cache.get(&crate::kvs::cache::ds::Lookup::Db("ns", "db")).is_none(),
		"the removed database was still cached"
	);
	assert!(
		cache.get(&crate::kvs::cache::ds::Lookup::Tb("ns", "db", "person")).is_none(),
		"a table of the removed database was still cached"
	);
}

/// A cached definition does not let a write through a removed name skip the
/// reclaim.
///
/// The datastore cache outlives the transaction that filled it. A deferred
/// removal clears it, but a reader whose snapshot predates the removal can put
/// an entry straight back, and the cache is consulted ahead of the catalog. A
/// write that trusted such an entry would take the removed database's table
/// definition, land in the prefix the reclaim task is about to destroy, and
/// never finish the job that would have saved it.
pub async fn a_cached_definition_does_not_bypass_the_reclaim(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("5c62d819-a4f4-4e7e-93a0-99b52d817e4e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, and the definitions a reader of it would hold
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	let db_def = txn.get_db("ns", "db").await.unwrap();
	let tb_def = txn.get_tb("ns", "db", "person").await.unwrap();
	txn.cancel().await.unwrap();
	// Remove it, which clears the cache
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// Put the definitions back, as a reader holding a snapshot from before the
	// removal does when it resolves the table after the removal has committed
	let cache = ds.get_cache();
	cache.insert(
		crate::kvs::cache::ds::Lookup::Db("ns", "db"),
		crate::kvs::cache::ds::Entry::Any(db_def),
	);
	cache.insert(
		crate::kvs::cache::ds::Lookup::Tb("ns", "db", "person"),
		crate::kvs::cache::ds::Entry::Any(tb_def),
	);
	// A write through the removed name
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds.execute("CREATE person:99 SET name = 'new'", &sess, None).await.unwrap();
	assert!(res[0].result.is_ok(), "a write through the removed name failed: {:?}", res[0].result);
	// The database exists again and nothing is queued against it
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(txn.get_db("ns", "db").await.is_ok(), "the write did not define the database again");
	txn.cancel().await.unwrap();
	assert!(jobs(&ds).await.is_empty(), "a write through a cached definition left the job behind");
	// The write is still there after the reclaim task has run, and the removed
	// database's records did not come back with it
	drain(&ds).await;
	let res = ds.execute("SELECT VALUE name FROM person", &sess, None).await.unwrap();
	assert_eq!(
		res[0].result.as_ref().unwrap().to_string(),
		"['new']",
		"a write which trusted a cached definition was destroyed by the reclaim"
	);
}

/// A write committed behind the reclaim's cursor is still destroyed.
///
/// The cursor makes each page resume strictly after the last key the page
/// before it deleted, so a page reports the range drained when the window
/// *after the cursor* comes back short. That says nothing about what is behind
/// it. A removal writes only the catalog row and the queue entry, so a
/// transaction holding a snapshot from before the removal does not conflict
/// with it and can commit a key anywhere in the prefix, including behind a
/// cursor the walk has already passed. Retiring the job on the strength of the
/// suffix alone would leave that key for whatever takes the name next.
pub async fn a_write_behind_the_cursor_is_not_left_behind(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("7e4e2d81-9200-4a4f-93a0-99c4b5c62d81").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, with more keys than one page below will delete
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person").await;
	for i in 0..8 {
		exec(&ds, "ns", Some("db"), &format!("CREATE person:{i} SET name = 'old'")).await;
	}
	let prefix = db_prefix("ns", "db");
	assert!(!keys_under(&ds, &prefix).await.is_empty());
	// A transaction whose snapshot predates the removal. It has already resolved
	// the catalog, which is what every write does before it writes.
	let late = ds.transaction(Write, Optimistic).await.unwrap();
	// Remove the database
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// The first pass only stamps the entry it has just met; deletion starts on
	// the pass after it
	let (_, errors, _) = ds.reclaim_tombstones_within(INTERVAL, 2, 2, 100, 100).await.unwrap();
	assert_eq!(errors, 0);
	// One page, bounded so that it stops partway and leaves a cursor behind it
	let (_, errors, deleted) =
		ds.reclaim_tombstones_within(INTERVAL, 2, 2, 100, 100).await.unwrap();
	assert_eq!(errors, 0);
	assert!(deleted > 0, "the bounded page destroyed nothing, so there is no cursor to get behind");
	assert!(
		!keys_under(&ds, &prefix).await.is_empty(),
		"the bounded page drained the whole prefix, so nothing is behind its cursor"
	);
	// The late write lands at the very start of the prefix, behind the cursor
	let mut key = prefix.clone();
	key.extend_from_slice(b"\x00late");
	late.set(key.clone(), b"survivor".to_vec(), None).await.unwrap();
	late.commit().await.unwrap();
	// The reclaim runs to completion and retires the job
	drain(&ds).await;
	assert!(jobs(&ds).await.is_empty(), "the reclaim job was not retired");
	// Nothing of the removed database is left for the next name to inherit
	assert!(
		keys_under(&ds, &prefix).await.is_empty(),
		"a write committed behind the cursor survived the reclaim"
	);
}

/// A write that lands in a prefix after its reclaim job is retired is not
/// inherited by the next object of the same name.
///
/// An empty sweep proves the prefix empty as of the commit that retires the
/// job, not that nothing may still write into it. A removal writes only the
/// catalog row and the queue entry, and the backends on this line validate
/// write sets only, so a transaction holding a snapshot from before the removal
/// conflicts with neither the removal nor the sweep, and may commit into the
/// prefix after both. Nothing names that key by then, so the guard that has to
/// catch it is the one on the other side: taking a name destroys the prefix it
/// addresses, whether or not a job still names it.
pub async fn a_write_after_the_job_is_retired_is_not_inherited(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("3f8c1d20-5a47-4b93-8c1d-205a474b9301").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	let prefix = db_prefix("ns", "db");
	assert!(!keys_under(&ds, &prefix).await.is_empty());
	// A transaction whose snapshot predates the removal, which is what every
	// write that has already resolved the catalog holds
	let late = ds.transaction(Write, Optimistic).await.unwrap();
	// Remove the database, and let the reclaim run to completion against a
	// prefix that is empty every time it looks
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	drain(&ds).await;
	assert!(jobs(&ds).await.is_empty(), "the reclaim job was not retired");
	assert!(keys_under(&ds, &prefix).await.is_empty(), "the reclaim left the prefix behind");
	// The late write commits into a prefix nothing names any more
	let mut key = prefix.clone();
	key.extend_from_slice(b"\x00late");
	late.set(key.clone(), b"survivor".to_vec(), None).await.unwrap();
	late.commit().await.unwrap();
	assert!(
		keys_under(&ds, &prefix).await.contains(&key),
		"the late write did not land in the orphaned prefix, so nothing is being tested"
	);
	// A database taking the name again must not inherit it
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person").await;
	assert!(
		!keys_under(&ds, &prefix).await.contains(&key),
		"a database inherited a write that committed after its reclaim job was retired"
	);
}

/// Redefining a name the catalog still holds destroys nothing.
///
/// `DEFINE NAMESPACE OVERWRITE` and `DEFINE DATABASE OVERWRITE` replace the
/// definition of an object that exists, and reach the same path that empties an
/// orphaned prefix before a name is taken. The catalog is what tells the two
/// apart: a prefix whose object is still defined is live data, not bytes left
/// by a removal.
pub async fn redefining_a_live_name_destroys_nothing(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("5c1e9b74-2d30-4a86-9b74-2d304a865c1e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, and nothing queued for reclaim
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'live'").await;
	assert!(jobs(&ds).await.is_empty());
	let before = keys_under(&ds, &ns_prefix("ns")).await;
	assert!(!before.is_empty());
	// Redefining either name leaves the data under it alone
	exec(&ds, "ns", None, "DEFINE NAMESPACE OVERWRITE ns").await;
	exec(&ds, "ns", None, "DEFINE DATABASE OVERWRITE db").await;
	assert_eq!(
		keys_under(&ds, &ns_prefix("ns")).await,
		before,
		"redefining a name the catalog still holds destroyed the data under it"
	);
	let res = ds
		.execute(
			"SELECT VALUE name FROM person",
			&Session::owner().with_ns("ns").with_db("db"),
			None,
		)
		.await
		.unwrap();
	assert_eq!(res[0].result.as_ref().unwrap().to_string(), "['live']");
}

/// A reclaim entry naming an object that is live again is left alone, rather
/// than destroying the object's data when its name is taken.
///
/// The catalog is the authority on whether an entry's prefix is orphaned, as it
/// is for the reclaim task, which leaves such an entry where it is. A namespace
/// entry's prefix covers every database in it, so acting on one that outlived
/// its namespace's return would destroy the whole live namespace, and defining
/// a database inside it is enough to reach that path.
pub async fn a_job_naming_a_live_object_destroys_nothing(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b7042f19-6c5a-4d38-9f19-6c5a4d38b704").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated namespace, and an entry naming it while it is still live,
	// which is the state a reuse would leave behind if it did not finish one
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1 SET name = 'live'").await;
	let before = keys_under(&ds, &ns_prefix("ns")).await;
	assert!(!before.is_empty());
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	let job = ReclaimKey::namespace(std::borrow::Cow::Borrowed("ns"), Uuid::now_v7());
	txn.set(job.encode().unwrap(), revision::to_vec(&ReclaimState::enqueued()).unwrap(), None)
		.await
		.unwrap();
	txn.commit().await.unwrap();
	assert_eq!(jobs(&ds).await.len(), 1);
	// Defining a database inside the namespace reaches the entry through the
	// namespace range, and taking the namespace's own name reaches it directly
	exec(&ds, "ns", None, "DEFINE DATABASE other").await;
	exec(&ds, "ns", None, "DEFINE NAMESPACE OVERWRITE ns").await;
	assert!(
		keys_under(&ds, &ns_prefix("ns")).await.starts_with(&before[..1]),
		"an entry naming a live namespace destroyed the start of its prefix"
	);
	for key in &before {
		assert!(
			keys_under(&ds, &ns_prefix("ns")).await.contains(key),
			"an entry naming a live namespace destroyed a key under it"
		);
	}
	let res = ds
		.execute(
			"SELECT VALUE name FROM person",
			&Session::owner().with_ns("ns").with_db("db"),
			None,
		)
		.await
		.unwrap();
	assert_eq!(
		res[0].result.as_ref().unwrap().to_string(),
		"['live']",
		"an entry naming a live namespace destroyed its data when a name was taken"
	);
	// The entry is still there, for the reclaim task to report as it does
	assert_eq!(jobs(&ds).await.len(), 1, "an entry naming a live object was retired");
}

/// A reuse whose prefix is exactly the size of its budget is allowed.
///
/// A page that exactly drains a range spends its whole limit, so it cannot be
/// told from a full one by its size alone. Refusing on that would reject a reuse
/// that had already destroyed everything it was asked to.
pub async fn reusing_a_name_at_exactly_the_budget_is_allowed(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("d8192a4f-47e2-4d81-92a4-f47e2d819200").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE |person:20|").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(!orphaned.is_empty());
	// A budget of exactly what the prefix holds is enough
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.finish_pending_reclaim_within("ns", Some("db"), orphaned.len() as u64)
		.await
		.expect("a reuse that fits its budget exactly was refused");
	txn.commit().await.unwrap();
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
}

/// A refused reuse destroys nothing and spends nothing.
///
/// This is reached from the implicit creation every write does outside strict
/// mode, so a name whose prefix is too large is met by every write against it,
/// and by every retry, for as long as the reclaim task takes to drain it. A
/// refusal that had first destroyed everything it was allowed would roll all of
/// that back and charge the next reuse in the same transaction for it.
pub async fn a_refused_reuse_spends_nothing(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("192a3b4c-5d6e-4f70-8192-a3b4c5d6e7f8").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Two removed databases, one far larger than the other
	exec(&ds, "ns", Some("big"), "DEFINE TABLE person; CREATE |person:20|").await;
	exec(&ds, "ns", Some("small"), "DEFINE TABLE person; CREATE person:1").await;
	exec(&ds, "ns", None, "REMOVE DATABASE big; REMOVE DATABASE small").await;
	let big = keys_under(&ds, &db_prefix("ns", "big")).await;
	let small = keys_under(&ds, &db_prefix("ns", "small")).await;
	assert!(big.len() > small.len(), "the two prefixes are the same size");
	// A budget that covers the smaller one exactly, spent first on the larger
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	let res = txn.finish_pending_reclaim_within("ns", Some("big"), small.len() as u64).await;
	assert!(
		matches!(res, Err(crate::err::Error::ReclaimPending { .. })),
		"an over-budget reuse did not refuse: {res:?}"
	);
	// The refusal left the allowance alone, so the reuse that fits still fits
	txn.finish_pending_reclaim("ns", Some("small"))
		.await
		.expect("a reuse that fits was charged for a refusal before it");
	txn.commit().await.unwrap();
	assert!(keys_under(&ds, &db_prefix("ns", "small")).await.is_empty());
	assert_eq!(
		keys_under(&ds, &db_prefix("ns", "big")).await,
		big,
		"the refused reuse destroyed part of the prefix it refused"
	);
}

/// One oversized prefix does not take every page of every pass.
///
/// The queue is keyed by kind and name, so serving it in key order would make
/// the entries that happen to sort lowest the only ones ever served while they
/// still hold data. A pass divides its key budget between the entries that have
/// waited longest instead.
pub async fn a_large_prefix_does_not_starve_the_queue(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("47e2d819-2a4f-4d81-92a4-f47e2d819201").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Two removed databases, the one that sorts first far larger than the other
	exec(&ds, "ns", Some("aaa"), "DEFINE TABLE person; CREATE |person:200|").await;
	exec(&ds, "ns", Some("zzz"), "DEFINE TABLE person; CREATE |person:4|").await;
	exec(&ds, "ns", None, "REMOVE DATABASE aaa; REMOVE DATABASE zzz").await;
	let big = keys_under(&ds, &db_prefix("ns", "aaa")).await.len();
	let small = keys_under(&ds, &db_prefix("ns", "zzz")).await.len();
	assert!(big > small * 4);
	// The first pass only stamps the two jobs
	let (visited, errors, deleted) = ds.reclaim_tombstones_bounded(INTERVAL, 2, 4).await.unwrap();
	assert_eq!((visited, errors, deleted), (2, 0, 0));
	// The next spends its budget on both of them, not on the first alone
	let (_, errors, deleted) = ds.reclaim_tombstones_bounded(INTERVAL, 2, 4).await.unwrap();
	assert_eq!(errors, 0);
	assert!(deleted > 0);
	assert!(
		keys_under(&ds, &db_prefix("ns", "zzz")).await.len() < small,
		"the smaller prefix was never served"
	);
	assert!(keys_under(&ds, &db_prefix("ns", "aaa")).await.len() < big);
}

/// The index compaction queue is drained in committed pages.
///
/// Its length tracks write traffic against count indexes rather than the size of
/// the catalog, and the range that reaches it has been matching no entry at all,
/// so an upgraded store can meet the task with a backlog. Neither the entries a
/// pass holds nor the deletions it issues may be bounded by the queue itself.
pub async fn index_compaction_drains_a_backlog_in_pages(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("2d8192a4-f47e-4d81-92a4-f47e2d819202").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database with a count index, and more queued entries than one page holds
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; DEFINE INDEX idx ON person FIELDS name")
		.await;
	seed_index_compaction(&ds, "ns", "db", 2_500).await;
	assert_eq!(index_compaction_keys(&ds).await.len(), 2_500);
	// One run drains all of them
	ds.index_compaction(INTERVAL).await.unwrap();
	assert!(index_compaction_keys(&ds).await.is_empty(), "the backlog was not drained");
}

/// The compaction entries of a name that exists again are left alone.
///
/// A compaction entry is addressed by name, so the entries under a reused name
/// belong to the object that took it. Dropping them would leave that object's
/// count-index deltas uncompacted until another write enqueued a trigger.
pub async fn purging_compaction_entries_leaves_a_live_object_alone(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("192a4f47-e2d8-4d81-92a4-f47e2d819203").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed but not yet observed
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	assert_eq!(jobs(&ds).await.len(), 1);
	// Put the catalog definition back without touching the queue, which is the
	// state a reuse racing the purge would leave behind, and queue compaction
	// entries the way the recreated database's own writes would
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	let def = crate::sql::statements::DefineDatabaseStatement {
		name: Ident::from("db"),
		..Default::default()
	};
	txn.set(crate::key::namespace::db::new("ns", "db"), revision::to_vec(&def).unwrap(), None)
		.await
		.unwrap();
	txn.commit().await.unwrap();
	seed_index_compaction(&ds, "ns", "db", 4).await;
	let queued = index_compaction_keys(&ds).await;
	assert_eq!(queued.len(), 4);
	// A pass which observes the job must not take them with it
	let (visited, errors, deleted) =
		ds.reclaim_tombstones_bounded(INTERVAL, 1_000, 100_000).await.unwrap();
	assert_eq!((visited, errors, deleted), (1, 0, 0));
	assert_eq!(
		index_compaction_keys(&ds).await,
		queued,
		"the compaction entries of a live database were purged"
	);
}

/// An index defined through the name of a removed database is built over no
/// records, whatever the removal left on disk.
///
/// The build reads the table in transactions of its own, which cannot see the
/// destruction the defining statement has not committed yet, so it resolves the
/// database through the catalog before it reads a single record.
pub async fn defining_an_index_through_a_removed_name_ignores_its_records(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b5c62d81-92a4-4f47-8e2d-8192a4f47e2d").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database holding records which a unique index could not cover
	exec(
		&ds,
		"ns",
		Some("db"),
		"DEFINE TABLE user;
		 CREATE user:1 SET email = 'one@example.com';
		 CREATE user:2 SET email = 'one@example.com'",
	)
	.await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	assert!(!keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	// The index covers nothing, so the duplicate the removal took away cannot
	// make defining it fail
	exec(&ds, "ns", Some("db"), "DEFINE INDEX email ON user FIELDS email UNIQUE").await;
	let sess = Session::owner().with_ns("ns").with_db("db");
	let res = ds.execute("SELECT * FROM user", &sess, None).await.unwrap();
	assert_eq!(res[0].result.as_ref().unwrap().to_string(), "[]");
	// The recreated database enforces the index on what is written to it now
	exec(&ds, "ns", Some("db"), "CREATE user:3 SET email = 'one@example.com'").await;
	let res = ds.execute("CREATE user:4 SET email = 'one@example.com'", &sess, None).await.unwrap();
	assert!(res[0].result.is_err(), "the unique index did not cover the recreated database");
	// And keeps what it holds across a reclaim pass
	drain(&ds).await;
	let res = ds.execute("SELECT VALUE id FROM user", &sess, None).await.unwrap();
	assert_eq!(res[0].result.as_ref().unwrap().to_string(), "[user:3]");
}

/// Reusing a name costs the caller's transaction the size of what was removed,
/// so it is capped and refused rather than allowed to grow without limit.
pub async fn reusing_a_name_over_the_budget_is_refused(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("a4f47e2d-8192-4093-99c4-b5c62d8192a4").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A database holding more keys than the budget below allows
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE |person:20|").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(orphaned.len() > 4);
	// A reuse which cannot finish the job inside its budget refuses, and leaves
	// the job and its data exactly where they were
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	let res = txn.finish_pending_reclaim_within("ns", Some("db"), 4).await;
	assert!(
		matches!(res, Err(crate::err::Error::ReclaimPending { .. })),
		"an over-budget reuse did not refuse: {res:?}"
	);
	txn.cancel().await.unwrap();
	assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await, orphaned);
	assert_eq!(jobs(&ds).await.len(), 1);
	// The background task drains what the reuse would not, and the name is free
	drain(&ds).await;
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
	exec(&ds, "ns", None, "DEFINE DATABASE db").await;
}

/// The reuses of one transaction share one key budget.
///
/// The cap exists to bound the write set of the transaction a reuse destroys
/// in, and one transaction can reach many names: a single statement resolves a
/// namespace and a database, and a transaction holds as many statements as the
/// client sends. An allowance granted per reuse would let the number of names
/// reused decide how large that write set grows, which is the whole of what the
/// cap is here to stop.
pub async fn the_reuses_of_one_transaction_share_one_budget(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("2d8192a4-f47e-4c93-b5c6-2d8192a4f47f").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Two removed databases, each holding the same amount
	exec(&ds, "ns", Some("one"), "DEFINE TABLE person; CREATE |person:20|").await;
	exec(&ds, "ns", Some("two"), "DEFINE TABLE person; CREATE |person:20|").await;
	exec(&ds, "ns", None, "REMOVE DATABASE one; REMOVE DATABASE two").await;
	let one = keys_under(&ds, &db_prefix("ns", "one")).await;
	let two = keys_under(&ds, &db_prefix("ns", "two")).await;
	assert!(!one.is_empty() && !two.is_empty());
	// A budget which covers the first exactly leaves the second nothing
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	txn.finish_pending_reclaim_within("ns", Some("one"), one.len() as u64)
		.await
		.expect("a reuse that fits the budget exactly was refused");
	let res = txn.finish_pending_reclaim("ns", Some("two")).await;
	assert!(
		matches!(res, Err(crate::err::Error::ReclaimPending { .. })),
		"a second reuse in the same transaction was granted an allowance of its own: {res:?}"
	);
	txn.cancel().await.unwrap();
	// Neither destruction is durable, because they shared a transaction
	assert_eq!(keys_under(&ds, &db_prefix("ns", "one")).await, one);
	assert_eq!(keys_under(&ds, &db_prefix("ns", "two")).await, two);
	assert_eq!(jobs(&ds).await.len(), 2);
}

/// A job naming an object the catalog holds again is left alone.
///
/// A name is only free to reuse once the job naming it is finished, and that
/// happens in the transaction which reuses it. The reaper checks the catalog
/// inside the transaction that would destroy the prefix anyway, so that a job
/// which somehow outlives its object's recreation destroys nothing.
pub async fn reclaim_leaves_a_job_naming_a_live_object(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("7e2d8192-a4f4-4c93-b5c6-2d8192a4f47e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, removed and stamped so that it is eligible
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1, person:2").await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	ds.reclaim_tombstones_bounded(INTERVAL, 1_000, 100_000).await.unwrap();
	assert!(jobs(&ds).await[0].1.is_observed());
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(!orphaned.is_empty());
	// Put the catalog definition back without touching the queue, which is the
	// state a reuse would leave behind if it did not finish the job
	let txn = ds.transaction(Write, Optimistic).await.unwrap();
	let def = crate::sql::statements::DefineDatabaseStatement {
		name: Ident::from("db"),
		..Default::default()
	};
	txn.set(crate::key::namespace::db::new("ns", "db"), revision::to_vec(&def).unwrap(), None)
		.await
		.unwrap();
	txn.commit().await.unwrap();
	// The reclaim destroys nothing, and the job stays where it is
	let (visited, errors, deleted) =
		ds.reclaim_tombstones_bounded(INTERVAL, 1_000, 100_000).await.unwrap();
	assert_eq!((visited, errors, deleted), (1, 0, 0));
	assert_eq!(keys_under(&ds, &db_prefix("ns", "db")).await, orphaned);
	assert_eq!(jobs(&ds).await.len(), 1);
}

/// `AND EXPUNGE` is deferred like any other removal, so the removal stays
/// bounded by the catalog. It selects nothing beyond that: every reclaim
/// destroys every version, because names are reused on this line.
pub async fn remove_database_and_expunge_defers_a_versioned_destroy(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("92a4f47e-2d81-4093-99c4-b5c62d8192a4").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated database, and a sibling which must be left alone
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1, person:2").await;
	exec(&ds, "ns", Some("keep"), "DEFINE TABLE person; CREATE person:1").await;
	let kept = keys_under(&ds, &db_prefix("ns", "keep")).await;
	assert!(!keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	// Remove and expunge it
	exec(&ds, "ns", None, "REMOVE DATABASE AND EXPUNGE db").await;
	// The statement queued the destroy and left the data for the reclaim task
	let orphaned = keys_under(&ds, &db_prefix("ns", "db")).await;
	assert!(!orphaned.is_empty());
	assert_eq!(jobs(&ds).await.len(), 1);
	// Draining the queue destroys the prefix
	assert_eq!(drain(&ds).await, orphaned.len() as u64);
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
	assert_eq!(keys_under(&ds, &db_prefix("ns", "keep")).await, kept);
}

/// `REMOVE NAMESPACE ... AND EXPUNGE` defers its destroy on the same terms.
pub async fn remove_namespace_and_expunge_defers_a_versioned_destroy(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("f47e2d81-92a4-4093-b5c6-2d8192a4f47e").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A populated namespace, and one which must be left alone
	exec(&ds, "ns", Some("one"), "DEFINE TABLE person; CREATE person:1").await;
	exec(&ds, "keep", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	let kept = keys_under(&ds, &ns_prefix("keep")).await;
	assert!(!keys_under(&ds, &ns_prefix("ns")).await.is_empty());
	// Remove and expunge it
	let root = Session::owner();
	ds.execute("REMOVE NAMESPACE AND EXPUNGE ns", &root, None).await.unwrap();
	// The statement queued the destroy and left the data for the reclaim task
	let orphaned = keys_under(&ds, &ns_prefix("ns")).await;
	assert!(!orphaned.is_empty());
	assert_eq!(jobs(&ds).await.len(), 1);
	// Draining the queue destroys the prefix
	assert_eq!(drain(&ds).await, orphaned.len() as u64);
	assert!(keys_under(&ds, &ns_prefix("ns")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
	assert_eq!(keys_under(&ds, &ns_prefix("keep")).await, kept);
}

/// Reusing the name of a removed namespace finishes the job naming it, whose
/// prefix covers every database it contained.
///
/// A job naming one of those databases is not finished with it: it addresses
/// the database, which does not exist however many times the namespace is
/// recreated, so the reclaim task still owns it.
pub async fn reusing_a_namespace_name_finishes_its_reclaims(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("6f708199-b3c5-47d8-a4f3-158693a4b51c").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A namespace with one database removed inside it and then removed itself,
	// so that two jobs name overlapping prefixes
	exec(&ds, "ns", Some("one"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	exec(&ds, "ns", Some("two"), "DEFINE TABLE person; CREATE person:1 SET name = 'old'").await;
	exec(&ds, "ns", None, "REMOVE DATABASE one").await;
	let root = Session::owner();
	ds.execute("REMOVE NAMESPACE ns", &root, None).await.unwrap();
	assert_eq!(jobs(&ds).await.len(), 2);
	// Using the namespace name again destroys everything it held, so nothing of
	// the removed namespace is left for the recreated one to inherit
	ds.execute("DEFINE NAMESPACE ns", &root, None).await.unwrap();
	assert!(keys_under(&ds, &ns_prefix("ns")).await.is_empty());
	let sess = Session::owner().with_ns("ns").with_db("two");
	let res = ds.execute("SELECT * FROM person", &sess, None).await.unwrap();
	assert_eq!(
		res[0].result.as_ref().unwrap().to_string(),
		"[]",
		"the recreated namespace inherited the removed namespace's records"
	);
	// The job naming the database is all that is left, and the reclaim task
	// drops it once it finds nothing to destroy
	let job = jobs(&ds).await;
	assert_eq!(job.len(), 1);
	assert_eq!(job[0].0.db.as_deref(), Some("one"));
	assert_eq!(drain(&ds).await, 0);
	assert!(jobs(&ds).await.is_empty());
	// Which took nothing of the recreated namespace with it
	exec(&ds, "ns", Some("one"), "CREATE person:1 SET name = 'new'").await;
	drain(&ds).await;
	let sess = Session::owner().with_ns("ns").with_db("one");
	let res = ds.execute("SELECT VALUE name FROM person", &sess, None).await.unwrap();
	assert_eq!(res[0].result.as_ref().unwrap().to_string(), "['new']");
}

/// A removal commits even though the changefeed timestamp writer commits into
/// the same database prefix while the removal is open.
///
/// The timestamp writer runs on every instance on a timer and writes inside
/// `crate::key::database::all` for every database that keeps a changefeed. A
/// removal must therefore not write anywhere in that prefix, or the two share
/// keys and one of them loses its commit. The control arm asserts that the
/// backend under test can still exhibit that conflict when a removal does write
/// there, so a passing main arm means something.
pub async fn remove_database_commits_against_a_concurrent_changefeed_write(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("9b0c1d77-91a3-45b6-c2d1-f3647081920a").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, kvs) = new_ds.create_ds(node_id, clock).await;
	// A populated database with a changefeed, and one tick of its timeline
	// already recorded, so that the versionstamp key the next tick rewrites
	// already exists
	exec(
		&ds,
		"ns",
		Some("db"),
		"DEFINE DATABASE db CHANGEFEED 1h; DEFINE TABLE person; CREATE person:1, person:2",
	)
	.await;
	tick(&ds, "ns", "db", 1).await;
	// Control, on a database of its own so that its outcome cannot disturb the
	// arm below. Only backends which reject two transactions writing the same
	// key can show the conflict at all; the ones exercised by
	// `multiwriter_same_keys_allow` let the later write win instead.
	if matches!(kvs, Kvs::Mem | Kvs::Rocksdb | Kvs::SurrealKV) {
		exec(&ds, "ns", Some("ctl"), "DEFINE DATABASE ctl CHANGEFEED 1h; CREATE person:1").await;
		tick(&ds, "ns", "ctl", 1).await;
		// The removal shape which deletes the data prefix inline shares keys
		// with the timestamp writer, and loses its commit to it
		let inline = ds.transaction(Write, Optimistic).await.unwrap();
		inline.del(crate::key::namespace::db::new("ns", "ctl")).await.unwrap();
		inline.delp(crate::key::database::all::new("ns", "ctl")).await.unwrap();
		tick(&ds, "ns", "ctl", 2).await;
		assert!(
			matches!(inline.commit().await, Err(crate::err::Error::TxRetryable)),
			"an inline prefix delete no longer conflicts with the changefeed timestamp writer, so this test proves nothing"
		);
	}
	// The removal statement, run inside a transaction the test holds open so
	// that the timestamp writer can commit underneath it
	let txn = ds.transaction(Write, Optimistic).await.unwrap().enclose();
	let mut ctx = MutableContext::default();
	ctx.set_transaction(txn.clone());
	let ctx = ctx.freeze();
	let opt = Options::default()
		.with_auth(Arc::new(Auth::for_root(Role::Owner)))
		.with_ns(Some("ns".into()));
	let stmt = RemoveDatabaseStatement {
		name: Ident::from("db"),
		if_exists: false,
		expunge: false,
	};
	stmt.compute(&ctx, &opt).await.unwrap();
	tick(&ds, "ns", "db", 2).await;
	// The removal writes only the catalog and the queue, so the concurrent
	// write into the database prefix cannot take its commit
	txn.commit().await.unwrap();
	// The removal took effect, and the reclaim destroys the prefix the
	// timestamp writer has been writing into
	let txn = ds.transaction(Read, Optimistic).await.unwrap();
	assert!(matches!(txn.get_db("ns", "db").await, Err(crate::err::Error::DbNotFound { .. })));
	txn.cancel().await.unwrap();
	drain(&ds).await;
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty());
	assert!(jobs(&ds).await.is_empty());
}

/// A count index with a larger backlog than one page holds is folded in pages.
///
/// The queue entry that triggers a compaction says nothing about how much the
/// index has accumulated: that is set by the write traffic against it, and a
/// store which has never compacted meets the first trigger with all of it.
/// Folding it in one transaction would put every one of those deletions into a
/// single write set.
pub async fn compacting_a_count_index_folds_a_backlog_in_pages(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("c6d7e8f9-0a1b-4c2d-9e3f-405162738495").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A count index with more deltas behind it than one page folds
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; DEFINE INDEX cnt ON person COUNT").await;
	exec(&ds, "ns", Some("db"), "CREATE |person:2500|").await;
	let (before, total) = count_index_entries(&ds, "ns", "db", "person", "cnt").await;
	assert!(before > 1_000, "only {before} entries accumulated, so this fits in one page");
	assert_eq!(total, 2_500, "the deltas did not count the records");
	// One run folds all of it, and the count it arrives at is the same
	ds.index_compaction(INTERVAL).await.unwrap();
	let (after, total) = count_index_entries(&ds, "ns", "db", "person", "cnt").await;
	assert_eq!(after, 1, "{after} entries survived the compaction rather than one aggregate");
	assert_eq!(total, 2_500, "folding the backlog changed the count");
	assert!(index_compaction_keys(&ds).await.is_empty(), "the queue was not drained");
}

/// Entries naming an index the catalog no longer holds are dropped.
///
/// Nothing folds deltas for an index that is not there and nothing writes more
/// of them, so such an entry has nothing left to ask for. Keeping it would stop
/// the queue draining past it, which is what happens to an entry whose index
/// could not be read for any other reason.
pub async fn compaction_entries_for_a_missing_index_are_dropped(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("d7e8f9a0-1b2c-4d3e-9f40-516273849506").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A table with no index of that name, and a queue naming one
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person").await;
	seed_index_compaction(&ds, "ns", "db", 4).await;
	assert_eq!(index_compaction_keys(&ds).await.len(), 4);
	// The pass drops them rather than stopping on them
	ds.index_compaction(INTERVAL).await.unwrap();
	assert!(
		index_compaction_keys(&ds).await.is_empty(),
		"the queue did not drain past an index the catalog does not hold"
	);
}

/// One pass reads at most the queue entries its bound allows.
///
/// Every entry carries a fresh uid, so an index still being written to keeps
/// placing entries above the cursor of a walk already under way: a pass that
/// followed them would end only when the writes stopped, and the task awaits
/// the pass before it can see a shutdown. What the bound leaves stays queued.
pub async fn one_compaction_pass_stops_at_what_it_may_read(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("f9a0b1c2-3d4e-4f50-a162-738495061728").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Entries a pass drops on sight, so only its bound can stop it
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person").await;
	seed_index_compaction(&ds, "ns", "db", 8).await;
	assert_eq!(index_compaction_keys(&ds).await.len(), 8);
	// A bound of half of them leaves the other half for the next tick
	ds.index_compaction_within(INTERVAL, 1_000, 4, 100_000).await.unwrap();
	let left = index_compaction_keys(&ds).await.len();
	assert_eq!(left, 4, "one pass read {} of 8 entries against a bound of 4", 8 - left);
}

/// One call folds at most one drain of an index, and keeps its queue entries.
///
/// An index still being written to accepts new deltas while its old ones are
/// folded, so draining one to exhaustion could hold the queue on it for as long
/// as the writes continue: every later index would wait on it and the lease
/// would go unrenewed. The budget is per drain, so an index met again on a
/// later page of the queue must not begin another, or a backlog large enough to
/// span pages would fold without limit anyway. The count it arrives at is
/// correct wherever it stops.
pub async fn compacting_a_count_index_stops_at_its_budget(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("e8f9a0b1-2c3d-4e4f-a051-627384950617").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A count index with more entries behind it than one page folds
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; DEFINE INDEX cnt ON person COUNT").await;
	exec(&ds, "ns", Some("db"), "CREATE |person:2500|").await;
	let (before, total) = count_index_entries(&ds, "ns", "db", "person", "cnt").await;
	assert!(before > 1_000, "only {before} entries accumulated, so one page drains it");
	assert_eq!(total, 2_500);
	// Its queue entries span several pages, so meeting the index again on a
	// later page must not start a second drain for it
	assert!(
		index_compaction_keys(&ds).await.len() > 1_000,
		"the queue fits in one page, so a later page cannot start a second drain"
	);
	ds.index_compaction_bounded(INTERVAL, 1_000).await.unwrap();
	assert!(
		!index_compaction_keys(&ds).await.is_empty(),
		"the queue entries were dropped although their index was not drained"
	);
	let (after, total) = count_index_entries(&ds, "ns", "db", "person", "cnt").await;
	assert!(
		after < before,
		"the budget bought no progress: {before} entries before, {after} after"
	);
	assert!(after > 1, "the whole index was folded despite the budget");
	assert_eq!(total, 2_500, "stopping at the budget changed the count");
}

/// One pass folds what its allowance leaves, across every index it meets.
///
/// The per-index cap bounds one drain, but how many indexes a pass meets is set
/// by the catalog and by how many of them are being written to. Granting each
/// of them a drain of its own would let the number of count indexes decide how
/// much a pass folds, and on a store meeting the queue for the first time that
/// is the whole accumulated backlog in one tick.
pub async fn one_compaction_pass_shares_one_fold_allowance(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("a0b1c2d3-4e5f-4061-8273-849506172839").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Two count indexes, the one that sorts first holding more than one page
	exec(
		&ds,
		"ns",
		Some("db"),
		"DEFINE TABLE aaa; DEFINE INDEX cnt ON aaa COUNT;
		 DEFINE TABLE zzz; DEFINE INDEX cnt ON zzz COUNT",
	)
	.await;
	exec(&ds, "ns", Some("db"), "CREATE |aaa:2500|; CREATE |zzz:20|").await;
	let (big, big_total) = count_index_entries(&ds, "ns", "db", "aaa", "cnt").await;
	let (small, small_total) = count_index_entries(&ds, "ns", "db", "zzz", "cnt").await;
	assert!(big > 1_000, "only {big} entries accumulated, so one page drains the first index");
	assert!(small > 1, "the second index has nothing to fold");
	// An allowance of one page is spent on the first index, and the second
	// keeps its entries for the pass after this one
	ds.index_compaction_within(INTERVAL, 1_000, 100_000, 1_000).await.unwrap();
	let (after_big, _) = count_index_entries(&ds, "ns", "db", "aaa", "cnt").await;
	let (after_small, _) = count_index_entries(&ds, "ns", "db", "zzz", "cnt").await;
	assert!(after_big < big, "the allowance bought no progress on the first index");
	assert_eq!(
		after_small, small,
		"the second index was folded too, so each index was given an allowance of its own"
	);
	// A pass with the allowance to reach it folds the second index as well
	ds.index_compaction(INTERVAL).await.unwrap();
	let (after_small, total) = count_index_entries(&ds, "ns", "db", "zzz", "cnt").await;
	assert!(after_small < small, "the second index was never reached");
	assert_eq!(total, small_total, "folding the second index changed its count");
	let (_, total) = count_index_entries(&ds, "ns", "db", "aaa", "cnt").await;
	assert_eq!(total, big_total, "folding the first index changed its count");
}

/// A compaction pass that stops short resumes where it stopped.
///
/// An index whose drain has spent its budget keeps every entry that asked for
/// it, and the entries of one index are contiguous. A walk that always started
/// at the beginning would therefore read the same entries of the same hot index
/// every pass once it held more of them than one pass may read, and the indexes
/// behind it would never be compacted at all.
pub async fn a_bounded_compaction_walk_resumes_where_it_stopped(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("4c5d6e7f-8091-42a3-b4c5-d6e7f8091a2b").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A hot count index which sorts first and holds more entries than one pass
	// reads, and a second one behind it
	exec(
		&ds,
		"ns",
		Some("db"),
		"DEFINE TABLE aaa; DEFINE INDEX cnt ON aaa COUNT;
		 DEFINE TABLE zzz; DEFINE INDEX cnt ON zzz COUNT",
	)
	.await;
	exec(&ds, "ns", Some("db"), "CREATE |aaa:2500|; CREATE |zzz:20|").await;
	assert!(
		index_compaction_keys(&ds).await.len() > 2_000,
		"the queue is shorter than one pass reads, so a pass would reach both"
	);
	let (small, small_total) = count_index_entries(&ds, "ns", "db", "zzz", "cnt").await;
	assert!(small > 1, "the second index has nothing to fold");
	// A drain too small to finish the first index, so it keeps its entries, and
	// a pass that reads fewer entries than it holds
	ds.index_compaction_within(INTERVAL, 1_000, 2_000, 100_000).await.unwrap();
	let (after, _) = count_index_entries(&ds, "ns", "db", "zzz", "cnt").await;
	assert_eq!(after, small, "the first pass reached the second index already");
	// The pass after it carries on past the entries the first one read
	ds.index_compaction_within(INTERVAL, 1_000, 2_000, 100_000).await.unwrap();
	let (after, total) = count_index_entries(&ds, "ns", "db", "zzz", "cnt").await;
	assert!(
		after < small,
		"the second index was never reached, so the walk read the first one again"
	);
	assert_eq!(total, small_total, "folding the second index changed its count");
}

/// A compaction queue too large for one purge keeps the job that names it.
///
/// The entries are root-level keys rather than part of the prefix the reclaim
/// destroys, so nothing else would name them once the job is retired. A purge
/// that stops at its cap therefore has to be finished before the job goes,
/// rather than left to whatever happens to run afterwards.
pub async fn a_large_compaction_queue_keeps_its_reclaim_job(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("f9a0b1c2-3d4e-4f50-a617-283949506172").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A removed database which left more queue entries than one purge drops
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	seed_index_compaction(&ds, "ns", "db", 12).await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// The pass which observes the job purges its share
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 5, 100_000).await.unwrap();
	// The pass which destroys the data purges another, and the queue still
	// holds entries, so the job stays for the pass after it
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 5, 100_000).await.unwrap();
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty(), "the data was not destroyed");
	assert!(!index_compaction_keys(&ds).await.is_empty());
	assert_eq!(
		jobs(&ds).await.len(),
		1,
		"the job was retired while the queue still held entries under its name"
	);
	// Passes after it finish both
	drain(&ds).await;
	assert!(jobs(&ds).await.is_empty(), "the job outlived the queue it named");
	assert!(index_compaction_keys(&ds).await.is_empty(), "the queue was not purged");
}

/// A purge which empties the queue on its last allowed page retires the job.
///
/// A bounded page reports a range drained only when it comes back short, so a
/// page which exactly consumes what it was allowed cannot tell an emptied queue
/// from one with more behind it. Left at that, a purge which finished the queue
/// on that page would keep the job for another interval and warn about entries
/// that no longer exist.
pub async fn a_purge_which_exactly_spends_its_budget_retires_the_job(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("c4d5e6f7-0819-4a2b-b3c4-d5e6f7081920").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// A removed database leaving exactly two purges worth of queue entries
	exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
	seed_index_compaction(&ds, "ns", "db", 10).await;
	exec(&ds, "ns", None, "REMOVE DATABASE db").await;
	// The pass which observes the job purges half of them
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 5, 100_000).await.unwrap();
	// The pass which destroys the data takes the rest in one full page, which
	// leaves nothing for a third pass to find
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 5, 100_000).await.unwrap();
	assert!(keys_under(&ds, &db_prefix("ns", "db")).await.is_empty(), "the data was not destroyed");
	assert!(index_compaction_keys(&ds).await.is_empty(), "the queue was not purged");
	assert!(jobs(&ds).await.is_empty(), "the job outlived the queue it named");
}

/// One pass reads at most the queue entries its bound allows.
///
/// Every entry carries a uid of its own, so a removal committed while a pass is
/// under way lands anywhere in the queue, including above the cursor of the walk
/// already reading it. A pass that followed them would return only when the
/// removals stopped, and the entries it had already found due would wait on it
/// for that whole time, because their data is destroyed only once the walk
/// returns. What the bound leaves is read by the next pass.
pub async fn one_reclaim_pass_stops_at_what_it_may_read(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b2c3d4e5-6f70-4819-a2a3-b4c5d6e7f809").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Eight removed databases, so eight queue entries to be sighted
	for i in 0..8 {
		let db = format!("db{i}");
		exec(&ds, "ns", Some(&db), "DEFINE TABLE person; CREATE person:1").await;
		exec(&ds, "ns", None, &format!("REMOVE DATABASE {db}")).await;
	}
	assert_eq!(jobs(&ds).await.len(), 8, "the queue was not seeded");
	// A bound of half of them leaves the rest unsighted, and a first sighting is
	// the only work an entry gets on the pass that finds it
	let (visited, errors, _) =
		ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 10_000, 4).await.unwrap();
	assert_eq!(errors, 0);
	assert_eq!(visited, 4, "one pass visited {visited} of 8 entries against a bound of 4");
	let unobserved = jobs(&ds).await.iter().filter(|(_, s)| !s.is_observed()).count();
	assert_eq!(unobserved, 4, "{unobserved} entries were left unsighted rather than 4");
	// The passes after it reach the rest
	drain(&ds).await;
	assert!(jobs(&ds).await.is_empty(), "the queue outlived the data it named");
	for i in 0..8 {
		let prefix = db_prefix("ns", &format!("db{i}"));
		assert!(keys_under(&ds, &prefix).await.is_empty(), "db{i} kept its data");
	}
}

/// One pass destroying data for many removals spends one allowance between them.
///
/// The entries a pass destroys data for each leave a compaction queue of their
/// own, and how many of them reach that point is set by how many removals are
/// waiting. Granting each a cap of its own would multiply the allowance by the
/// quota, and a pass would open a transaction per page of every one of them.
/// What an entry is not given is purged by the pass after it, which is what
/// keeps its job standing in the meantime.
pub async fn destroying_removals_shares_one_compaction_budget(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("b1c2d3e4-5f60-4172-8394-950617283940").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Two removed databases, observed before either left a compaction queue
	exec(&ds, "ns", Some("one"), "DEFINE TABLE person; CREATE person:1").await;
	exec(&ds, "ns", Some("two"), "DEFINE TABLE person; CREATE person:1").await;
	exec(&ds, "ns", None, "REMOVE DATABASE one; REMOVE DATABASE two").await;
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 10, 100_000).await.unwrap();
	assert_eq!(jobs(&ds).await.len(), 2, "both removals should be waiting");
	seed_index_compaction(&ds, "ns", "one", 10).await;
	seed_index_compaction(&ds, "ns", "two", 10).await;
	assert_eq!(index_compaction_keys(&ds).await.len(), 20);
	// An allowance covering one of the two queues is not granted to both
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 10, 100_000).await.unwrap();
	assert_eq!(
		index_compaction_keys(&ds).await.len(),
		10,
		"both queues were purged, so each entry was given an allowance of its own"
	);
	assert_eq!(
		jobs(&ds).await.len(),
		1,
		"a job was retired while the queue under its name still held entries"
	);
	// The passes after it finish what this one left
	drain(&ds).await;
	assert!(jobs(&ds).await.is_empty(), "a job outlived the queue it named");
	assert!(index_compaction_keys(&ds).await.is_empty(), "a queue was not purged");
}

/// A pass that stops short resumes where it stopped, not at the beginning.
///
/// An entry is stamped on the pass that first meets it and destroyed by a later
/// one, so it stays in the queue across passes. A walk that always started at
/// the beginning would therefore read the same entries every pass once the queue
/// was longer than one pass may read, and the removals behind them would not
/// even be stamped, which is what puts them in line for a budget at all.
pub async fn a_bounded_reclaim_walk_resumes_where_it_stopped(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("3b4c5d6e-7f80-4192-a3b4-c5d6e7f80192").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Eight removed databases, so eight queue entries to be sighted
	for i in 0..8 {
		let db = format!("db{i}");
		exec(&ds, "ns", Some(&db), "DEFINE TABLE person; CREATE person:1").await;
		exec(&ds, "ns", None, &format!("REMOVE DATABASE {db}")).await;
	}
	assert_eq!(jobs(&ds).await.len(), 8, "the queue was not seeded");
	// A first sighting only stamps, so every entry stays where it is and a walk
	// starting again at the beginning would find the same ones
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 10_000, 4).await.unwrap();
	let unobserved = jobs(&ds).await.iter().filter(|(_, s)| !s.is_observed()).count();
	assert_eq!(unobserved, 4, "the first pass sighted {} entries rather than 4", 8 - unobserved);
	// The pass after it reaches the four the first one did not
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 10_000, 4).await.unwrap();
	let unobserved = jobs(&ds).await.iter().filter(|(_, s)| !s.is_observed()).count();
	assert_eq!(
		unobserved, 0,
		"{unobserved} entries were still unsighted, so the second pass read the first four again"
	);
}

/// One pass observing many removals spends one allowance between them.
///
/// A first sighting drops what the removed object left in the index compaction
/// queue, and every job the queue holds is sighted in the same pass. Granting
/// each of them an allowance of its own would let the number of removals
/// waiting decide how much a pass deletes, which is what a budget is here to
/// stop: the queue's length follows write traffic against count indexes, so a
/// backlog of removed databases would multiply one bound by the other.
pub async fn observing_removals_shares_one_compaction_budget(new_ds: impl CreateDs) {
	// Create a new datastore
	let node_id = Uuid::parse_str("a1b2c3d4-5e6f-4708-9192-a3b4c5d6e7f8").unwrap();
	let clock = Arc::new(SizedClock::Fake(FakeClock::new(Timestamp::default())));
	let (ds, _) = new_ds.create_ds(node_id, clock).await;
	// Three removed databases, each leaving entries in the queue
	for db in ["a", "b", "c"] {
		exec(&ds, "ns", Some(db), "DEFINE TABLE person; CREATE person:1").await;
		seed_index_compaction(&ds, "ns", db, 10).await;
		exec(&ds, "ns", None, &format!("REMOVE DATABASE {db}")).await;
	}
	let before = index_compaction_keys(&ds).await.len();
	assert_eq!(before, 30, "the queue was not seeded");
	// The pass that observes all three is allowed fewer entries than they hold
	// between them
	ds.reclaim_tombstones_within(INTERVAL, 1_000, 100_000, 12, 100_000).await.unwrap();
	let dropped = before - index_compaction_keys(&ds).await.len();
	assert!(dropped <= 12, "one pass dropped {dropped} entries against an allowance of 12");
	// What it could not reach is finished by the passes that destroy the data
	drain(&ds).await;
	assert!(index_compaction_keys(&ds).await.is_empty(), "the queue was not purged");
	assert!(jobs(&ds).await.is_empty(), "a job outlived the queue it named");
}

macro_rules! define_tests {
	($new_ds:ident) => {
		#[tokio::test]
		#[serial_test::serial]
		async fn remove_database_defers_its_data() {
			super::reclaim::remove_database_defers_its_data($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_destroys_the_deferred_prefix() {
			super::reclaim::reclaim_destroys_the_deferred_prefix($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_drains_a_prefix_across_bounded_passes() {
			super::reclaim::reclaim_drains_a_prefix_across_bounded_passes($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_resumes_after_an_interrupted_pass() {
			super::reclaim::reclaim_resumes_after_an_interrupted_pass($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn remove_namespace_covers_its_databases() {
			super::reclaim::remove_namespace_covers_its_databases($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn remove_database_rolled_back_leaves_no_job() {
			super::reclaim::remove_database_rolled_back_leaves_no_job($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reusing_a_database_name_finishes_its_reclaim() {
			super::reclaim::reusing_a_database_name_finishes_its_reclaim($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_removed_databases_rows_are_unreachable() {
			super::reclaim::a_removed_databases_rows_are_unreachable($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_removed_databases_credentials_stop_resolving() {
			super::reclaim::a_removed_databases_credentials_stop_resolving($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_removed_namespaces_credentials_stop_resolving() {
			super::reclaim::a_removed_namespaces_credentials_stop_resolving($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn writes_through_a_removed_name_survive_the_reclaim() {
			super::reclaim::writes_through_a_removed_name_survive_the_reclaim($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_write_behind_the_cursor_is_not_left_behind() {
			super::reclaim::a_write_behind_the_cursor_is_not_left_behind($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_write_after_the_job_is_retired_is_not_inherited() {
			super::reclaim::a_write_after_the_job_is_retired_is_not_inherited($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn redefining_a_live_name_destroys_nothing() {
			super::reclaim::redefining_a_live_name_destroys_nothing($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_job_naming_a_live_object_destroys_nothing() {
			super::reclaim::a_job_naming_a_live_object_destroys_nothing($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_cached_definition_does_not_bypass_the_reclaim() {
			super::reclaim::a_cached_definition_does_not_bypass_the_reclaim($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn writes_through_a_removed_namespace_survive_the_reclaim() {
			super::reclaim::writes_through_a_removed_namespace_survive_the_reclaim($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn defining_a_database_inside_a_removed_namespace_creates_it() {
			super::reclaim::defining_a_database_inside_a_removed_namespace_creates_it($new_ds)
				.await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reusing_a_name_at_exactly_the_budget_is_allowed() {
			super::reclaim::reusing_a_name_at_exactly_the_budget_is_allowed($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_deferred_removal_marks_its_transaction() {
			super::reclaim::a_deferred_removal_marks_its_transaction($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_large_prefix_does_not_starve_the_queue() {
			super::reclaim::a_large_prefix_does_not_starve_the_queue($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_refused_reuse_spends_nothing() {
			super::reclaim::a_refused_reuse_spends_nothing($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn index_compaction_drains_a_backlog_in_pages() {
			super::reclaim::index_compaction_drains_a_backlog_in_pages($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn compacting_a_count_index_folds_a_backlog_in_pages() {
			super::reclaim::compacting_a_count_index_folds_a_backlog_in_pages($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn compaction_entries_for_a_missing_index_are_dropped() {
			super::reclaim::compaction_entries_for_a_missing_index_are_dropped($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn one_compaction_pass_stops_at_what_it_may_read() {
			super::reclaim::one_compaction_pass_stops_at_what_it_may_read($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn compacting_a_count_index_stops_at_its_budget() {
			super::reclaim::compacting_a_count_index_stops_at_its_budget($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn purging_compaction_entries_leaves_a_live_object_alone() {
			super::reclaim::purging_compaction_entries_leaves_a_live_object_alone($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_large_compaction_queue_keeps_its_reclaim_job() {
			super::reclaim::a_large_compaction_queue_keeps_its_reclaim_job($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_bounded_compaction_walk_resumes_where_it_stopped() {
			super::reclaim::a_bounded_compaction_walk_resumes_where_it_stopped($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn one_compaction_pass_shares_one_fold_allowance() {
			super::reclaim::one_compaction_pass_shares_one_fold_allowance($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn one_reclaim_pass_stops_at_what_it_may_read() {
			super::reclaim::one_reclaim_pass_stops_at_what_it_may_read($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_purge_which_exactly_spends_its_budget_retires_the_job() {
			super::reclaim::a_purge_which_exactly_spends_its_budget_retires_the_job($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn observing_removals_shares_one_compaction_budget() {
			super::reclaim::observing_removals_shares_one_compaction_budget($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn a_bounded_reclaim_walk_resumes_where_it_stopped() {
			super::reclaim::a_bounded_reclaim_walk_resumes_where_it_stopped($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn destroying_removals_shares_one_compaction_budget() {
			super::reclaim::destroying_removals_shares_one_compaction_budget($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn defining_an_index_through_a_removed_name_ignores_its_records() {
			super::reclaim::defining_an_index_through_a_removed_name_ignores_its_records($new_ds)
				.await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reusing_a_name_over_the_budget_is_refused() {
			super::reclaim::reusing_a_name_over_the_budget_is_refused($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reclaim_leaves_a_job_naming_a_live_object() {
			super::reclaim::reclaim_leaves_a_job_naming_a_live_object($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn the_reuses_of_one_transaction_share_one_budget() {
			super::reclaim::the_reuses_of_one_transaction_share_one_budget($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn remove_database_and_expunge_defers_a_versioned_destroy() {
			super::reclaim::remove_database_and_expunge_defers_a_versioned_destroy($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn remove_namespace_and_expunge_defers_a_versioned_destroy() {
			super::reclaim::remove_namespace_and_expunge_defers_a_versioned_destroy($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn reusing_a_namespace_name_finishes_its_reclaims() {
			super::reclaim::reusing_a_namespace_name_finishes_its_reclaims($new_ds).await;
		}

		#[tokio::test]
		#[serial_test::serial]
		async fn remove_database_commits_against_a_concurrent_changefeed_write() {
			super::reclaim::remove_database_commits_against_a_concurrent_changefeed_write($new_ds)
				.await;
		}
	};
}
pub(crate) use define_tests;

/// Reclaim against a store which retains versions.
///
/// The shared harness builds a `surrealkv:` datastore, which keeps none, so
/// these build their own `surrealkv+versioned:` one. CI's workspace test only
/// enables `storage-mem`, so they run under `--features kv-surrealkv`.
#[cfg(feature = "kv-surrealkv")]
mod versioned {
	use super::{
		count_index_entries, count_index_versions, db_prefix, drain, exec, index_compaction_keys,
		jobs, keys_under, seed_index_compaction, versions_under, INTERVAL,
	};
	use crate::kvs::{Datastore, Key, LockType::*, TransactionType::*};
	use std::time::Duration;
	use temp_dir::TempDir;

	/// How many times the reclaim task's lease has been written.
	async fn lease_versions(ds: &Datastore) -> usize {
		let key =
			crate::key::root::tl::Tl::new(&crate::kvs::tasklease::TaskLeaseType::ReclaimTombstones);
		let beg = crate::kvs::KeyEncode::encode_owned(key).unwrap();
		let mut end = beg.clone();
		end.push(0xff);
		let mut tx = ds.transaction(Read, Optimistic).await.unwrap().inner();
		let rows = tx.scan_all_versions(beg..end, u32::MAX).await.unwrap();
		tx.cancel().await.unwrap();
		rows.len()
	}

	/// A pass renews the lease it holds while it walks the queue.
	///
	/// The walk writes: an unobserved entry is stamped, and what it left in the
	/// index compaction queue is dropped before that. A backlog of removals is long
	/// enough to outlive a lease granted for twice the interval, and a second node
	/// taking it would destroy the same prefixes alongside this one.
	#[tokio::test]
	#[serial_test::serial]
	async fn a_reclaim_pass_renews_its_lease() {
		// A datastore which retains versions, so each write of the lease is counted
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		// A backlog of removals, each of which leaves a compaction queue
		for i in 0..4 {
			let db = format!("db{i}");
			exec(&ds, "ns", Some(&db), "DEFINE TABLE person; CREATE person:1").await;
			seed_index_compaction(&ds, "ns", &db, 4).await;
			exec(&ds, "ns", None, &format!("REMOVE DATABASE {db}")).await;
		}
		// A lease of no duration, so every check of it is a renewal
		ds.reclaim_tombstones_within(Duration::ZERO, 1_000, 100_000, 10_000, 100_000)
			.await
			.unwrap();
		let writes = lease_versions(&ds).await;
		assert!(
			writes > 1,
			"the lease was written {writes} time(s), so it was taken and never renewed"
		);
	}

	/// Folding a count index destroys the versions of the deltas it folds.
	///
	/// Both the total and the uid are part of a count entry's key, so nothing a
	/// fold consumes is ever written over: a delta carries a uid of its own,
	/// and an aggregate carrying a different total is a different key. A
	/// tombstone would therefore leave every entry the index has accumulated on
	/// disk for good, and one is written per update.
	#[tokio::test]
	#[serial_test::serial]
	async fn compacting_count_deltas_destroys_their_versions() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		exec(&ds, "ns", Some("db"), "DEFINE TABLE person; DEFINE INDEX cnt ON person COUNT").await;
		exec(&ds, "ns", Some("db"), "CREATE |person:20|").await;
		// A backlog of deltas, each of them on disk
		let (before, total) = count_index_entries(&ds, "ns", "db", "person", "cnt").await;
		assert!(before > 1, "only {before} entries accumulated, so there is nothing to fold");
		assert_eq!(total, 20);
		let held = count_index_versions(&ds, "ns", "db", "person", "cnt").await;
		assert!(held >= before, "the store holds {held} versions of {before} entries");
		// The fold collapses them into the aggregate alone
		ds.index_compaction(INTERVAL).await.unwrap();
		let (after, total) = count_index_entries(&ds, "ns", "db", "person", "cnt").await;
		assert_eq!(after, 1, "the fold left {after} entries rather than the aggregate alone");
		assert_eq!(total, 20, "the fold changed the count");
		// And took the deltas off disk rather than tombstoning them
		let left = count_index_versions(&ds, "ns", "db", "person", "cnt").await;
		assert_eq!(left, after, "the fold left {left} versions for {after} visible entries");
	}

	/// A read opened before a removal still resolves the database it names.
	///
	/// A query which started before the removal resolves its database through the
	/// catalog like any other, so what it can still read is decided by whether the
	/// cleared definition is absent from the snapshot it holds. It is not: a
	/// reader keeps the snapshot it began with, and neither an ordinary delete nor
	/// a destruction of every version reaches one that is already open.
	#[tokio::test]
	#[serial_test::serial]
	async fn a_read_opened_before_a_removal_still_resolves_the_database() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
		// A read which has resolved nothing yet, opened before the removal
		let before = ds.transaction(Read, Optimistic).await.unwrap();
		exec(&ds, "ns", None, "REMOVE DATABASE db").await;
		// The removal took effect for everything opened after it
		let after = ds.transaction(Read, Optimistic).await.unwrap();
		let gone = !after.db_exists("ns", "db").await.unwrap();
		after.cancel().await.unwrap();
		assert!(gone, "the removal did not take effect, so this proves nothing");
		// The older read reaches its first catalog check after that commit
		let exists = before.db_exists("ns", "db").await.unwrap();
		// And the records it was left are still readable through it
		let prefix = db_prefix("ns", "db");
		let mut end = prefix.clone();
		end.push(0xff);
		let held = before.keys(prefix..end, 10, None).await.unwrap().len();
		before.cancel().await.unwrap();
		assert!(exists, "the reader lost the database it opened against");
		assert!(held > 0, "the reader could not read the data it was left");
	}

	/// A key deleted before its database was removed is destroyed with the rest.
	///
	/// Such a key has no visible entry and still holds every version it ever
	/// had, so enumerating the prefix's visible keys walks straight past it and
	/// then reports the prefix drained. Recreating the name would inherit those
	/// versions, and a versioned read of the new database would return the old
	/// one's record.
	#[tokio::test]
	#[serial_test::serial]
	async fn reclaim_destroys_tombstoned_versions() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		// A database holding one live record and one already deleted
		exec(
			&ds,
			"ns",
			Some("db"),
			"DEFINE TABLE person; CREATE person:1, person:2; DELETE person:1",
		)
		.await;
		let prefix = db_prefix("ns", "db");
		let visible = keys_under(&ds, &prefix).await.len();
		let held = versions_under(&ds, &prefix).await;
		assert!(
			held > visible,
			"the store retained no history ({held} versions over {visible} keys), so this \
			 proves nothing"
		);
		// Remove it and let the reclaim task run to completion
		exec(&ds, "ns", None, "REMOVE DATABASE db").await;
		drain(&ds).await;
		assert!(keys_under(&ds, &prefix).await.is_empty());
		assert!(jobs(&ds).await.is_empty());
		assert_eq!(
			versions_under(&ds, &prefix).await,
			0,
			"the prefix held {held} versions before the reclaim and still holds some after it"
		);
	}

	/// A reuse whose budget runs out over tombstoned keys is refused.
	///
	/// The budget is spent on the keys a page destroys, and a page destroys
	/// versions, so a prefix can hold more of them than a visible scan reports.
	/// Asking whether anything is left with a visible scan answers no while the
	/// prefix still holds every version of every key deleted before the removal,
	/// and the name is then handed to a new object that inherits them.
	#[tokio::test]
	#[serial_test::serial]
	async fn a_budget_spent_over_tombstoned_keys_refuses_the_reuse() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		// A database whose every record has been deleted, so that what it holds
		// beyond its catalog is invisible to a scan of its visible keys
		exec(
			&ds,
			"ns",
			Some("db"),
			"DEFINE TABLE person; CREATE person:1, person:2, person:3;
			 DELETE person:1, person:2, person:3",
		)
		.await;
		exec(&ds, "ns", None, "REMOVE DATABASE db").await;
		let prefix = db_prefix("ns", "db");
		// The catalog is all that is visible, and it sorts before the records
		let visible = keys_under(&ds, &prefix).await.len();
		let held = versions_under(&ds, &prefix).await;
		assert!(
			held > visible,
			"the store retained no history ({held} versions over {visible} keys), so this \
			 proves nothing"
		);
		// A budget of exactly the visible keys is spent before the tombstoned
		// records are reached, so the reuse cannot finish inside its transaction
		let txn = ds.transaction(Write, Optimistic).await.unwrap();
		let res = txn.finish_pending_reclaim_within("ns", Some("db"), visible as u64).await;
		assert!(
			matches!(res, Err(crate::err::Error::ReclaimPending { .. })),
			"a reuse which left tombstoned versions behind was allowed: {res:?}"
		);
		txn.cancel().await.unwrap();
		// The job is still there for the reaper, and it destroys the versions
		assert_eq!(jobs(&ds).await.len(), 1);
		drain(&ds).await;
		assert_eq!(versions_under(&ds, &prefix).await, 0);
	}

	/// The prefix the index compaction queue lives in.
	fn queue_prefix() -> Key {
		b"/!ic".to_vec()
	}

	/// A processed compaction entry leaves nothing of itself behind.
	///
	/// Every entry carries a fresh uid and no later one is ever written over it,
	/// so a deletion that only tombstones keeps the entry and its tombstone for
	/// good. The queue tracks write traffic against count indexes, so that is
	/// unbounded growth in root-level metadata rather than a fixed overhead.
	#[tokio::test]
	#[serial_test::serial]
	async fn draining_the_compaction_queue_destroys_its_entries() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		// A database with a count index, and a queue of entries naming it
		exec(&ds, "ns", Some("db"), "DEFINE TABLE person; DEFINE INDEX idx ON person FIELDS name")
			.await;
		seed_index_compaction(&ds, "ns", "db", 8).await;
		assert_eq!(index_compaction_keys(&ds).await.len(), 8);
		let before = versions_under(&ds, &queue_prefix()).await;
		assert!(before >= 8, "the store retained no history, so this proves nothing");
		// Draining the queue destroys them rather than hiding them
		ds.index_compaction(INTERVAL).await.unwrap();
		assert!(index_compaction_keys(&ds).await.is_empty());
		assert_eq!(
			versions_under(&ds, &queue_prefix()).await,
			0,
			"the queue held {before} versions before the drain and still holds some after it"
		);
	}

	/// Purging the queue of a removed object destroys its entries too.
	///
	/// The entries are addressed by the name that was removed and are dropped
	/// with the rest of what it left behind, on the same terms: nothing will
	/// ever write over them, so a tombstone is permanent.
	#[tokio::test]
	#[serial_test::serial]
	async fn purging_the_compaction_queue_destroys_its_entries() {
		// A datastore which retains versions
		let dir = TempDir::new().unwrap();
		let path = format!("surrealkv+versioned:{}", dir.path().to_string_lossy());
		let ds = Datastore::new(&path).await.unwrap();
		// A database with a queue of entries naming it, removed
		exec(&ds, "ns", Some("db"), "DEFINE TABLE person; CREATE person:1").await;
		seed_index_compaction(&ds, "ns", "db", 8).await;
		exec(&ds, "ns", None, "REMOVE DATABASE db").await;
		let before = versions_under(&ds, &queue_prefix()).await;
		assert!(before >= 8, "the store retained no history, so this proves nothing");
		// The reclaim purges the queue of the name it is destroying
		drain(&ds).await;
		assert!(index_compaction_keys(&ds).await.is_empty());
		assert_eq!(
			versions_under(&ds, &queue_prefix()).await,
			0,
			"the queue held {before} versions before the purge and still holds some after it"
		);
	}
}
