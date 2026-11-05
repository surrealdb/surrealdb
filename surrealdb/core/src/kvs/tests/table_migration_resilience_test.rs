//! The table migration job and the reads that precede it, under the
//! conditions a deployment meets: a pass that stops mid-lap, a lease that
//! changes hands, an owner that wakes after its lease expired, a writer older
//! than a migration, a verification that must wait for older transactions,
//! and every schema change a table can take with and without rows.
//!
//! The datastores run without maintenance tasks and the job is driven by
//! hand, so each test decides when a pass runs and what happens between its
//! pages. A pass is stopped mid-lap by corrupting a record the next page
//! reads (the page fails, the pages before it have committed), and paused
//! inside a page through the datastore's scheduler, which a program yields
//! through once it has spent its quantum.

#![allow(clippy::unwrap_used)]

use std::borrow::Cow;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_types::{ToSql, Value};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::catalog::providers::{CatalogProvider, DatabaseProvider, TableProvider};
use crate::catalog::{DatabaseId, Generation, NamespaceId, RecordFormat, TaskLease};
use crate::dbs::Session;
use crate::exec::scheduler::{Scheduler, YieldFuture};
use crate::key::Key;
use crate::key::schema::{
	RecordPrefix, TableMigrationKey, TableMigrationPrefix, TableMigrationRequestPrefix,
	TaskLeaseKey,
};
use crate::key::table_migration::{TableMigrationState, VerifiedField};
use crate::kvs::tasklease::TaskLeaseType;
use crate::kvs::{Datastore, QueryRequest, Transaction};
use crate::val::{RecordIdKey, TableName, Value as Stored};

// ---------------------------------------------------------------------------
// Harness.
// ---------------------------------------------------------------------------

/// An in-memory datastore with no maintenance tasks, whose database `test` is at compatibility
/// level `level`; `quantum` is the number of units a program spends between yields (`None` keeps
/// the default).
async fn datastore_with(level: u8, quantum: Option<u32>) -> Arc<Datastore> {
	let mut builder = Datastore::builder().without_maintenance_tasks();
	if let Some(quantum) = quantum {
		builder = builder.with_config(
			surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", quantum.to_string()),
		);
	}
	let ds = builder.build_with_path("memory").await.unwrap();
	{
		let txn = ds.transaction(Write).await.unwrap();
		txn.ensure_ns_db(None, "test", "test").await.unwrap();
		txn.commit().await.unwrap();
	}
	run(&ds, &format!("ALTER DATABASE COMPATIBILITY {level};")).await;
	ds
}

async fn datastore(level: u8) -> Arc<Datastore> {
	datastore_with(level, None).await
}

fn session() -> Session {
	Session::owner().with_ns("test").with_db("test")
}

/// The result of every statement of `sql`, failing on the first error.
async fn run(ds: &Datastore, sql: &str) -> Vec<Value> {
	ds.execute(sql, &session(), None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.unwrap())
		.collect()
}

/// The error text of the first statement of `sql` that fails.
async fn fails(ds: &Datastore, sql: &str) -> String {
	for response in ds.execute(sql, &session(), None).await.unwrap() {
		if let Err(e) = response.result {
			return e.to_string();
		}
	}
	panic!("`{sql}` was expected to fail");
}

/// Run the job until every queue entry has nothing left to do, waiting at
/// most `interval` for transactions older than a verification. A pass that
/// does not return within two minutes is a failure, not a hang.
async fn drain_for(ds: &Arc<Datastore>, interval: Duration) {
	tokio::time::timeout(
		Duration::from_secs(120),
		Datastore::table_migration(Arc::clone(ds), interval, CancellationToken::new()),
	)
	.await
	.expect("the job did not return")
	.unwrap();
}

async fn drain(ds: &Arc<Datastore>) {
	drain_for(ds, Duration::from_secs(60)).await;
}

async fn ids(ds: &Datastore) -> (NamespaceId, DatabaseId) {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	txn.cancel().await.unwrap();
	(db.namespace_id, db.database_id)
}

/// The job's queue entry of `tb`, if it has one.
async fn entry(ds: &Datastore, tb: &str) -> Option<TableMigrationState> {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let state = txn.get_tb_migration_job(ns, db, &TableName::from(tb)).await.unwrap();
	txn.cancel().await.unwrap();
	state
}

/// How many of `tb`'s migration steps a row stamped `stamp` has passed: the steps the table has
/// run less the ones the row still goes through when it is read.
async fn passed(
	txn: &Transaction,
	ns: NamespaceId,
	db: DatabaseId,
	tb: &TableName,
	stamp: Option<Generation>,
) -> u32 {
	let total = txn.tb_migration_runs(ns, db, tb).await.unwrap().len();
	let layouts = txn.table_layouts(ns, db, tb, None).await.unwrap();
	let pending = layouts.pending_migrations(stamp).unwrap().len();
	u32::try_from(total - pending).unwrap()
}

/// The number of migration steps `tb` has run.
async fn steps_run(ds: &Datastore, tb: &str) -> u32 {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let total = txn.tb_migration_runs(ns, db, &TableName::from(tb)).await.unwrap().len();
	txn.cancel().await.unwrap();
	u32::try_from(total).unwrap()
}

/// The record `tb:id` as stored: its data and how many migration steps its stamp has passed.
/// No step is applied.
async fn stored(ds: &Datastore, tb: &str, id: i64) -> (Stored, u32) {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let table = TableName::from(tb);
	let (record, stamp) =
		txn.get_stamped_record(ns, db, &table, &RecordIdKey::Number(id), None).await.unwrap();
	let steps = passed(&txn, ns, db, &table, stamp).await;
	txn.cancel().await.unwrap();
	(record.data.clone(), steps)
}

/// The integer stored at `field` of `tb:id`, panicking on any other value.
async fn stored_int(ds: &Datastore, tb: &str, id: i64, field: &str) -> (i64, u32) {
	let (data, generation) = stored(ds, tb, id).await;
	let Stored::Object(object) = &data else {
		panic!("{tb}:{id} stores {data:?}");
	};
	match object.get(field) {
		Some(Stored::Number(crate::val::Number::Int(n))) => (*n, generation),
		other => panic!("{tb}:{id}.{field} stores {other:?}"),
	}
}

/// The key and stored bytes of every record of `tb`, in key order.
async fn raw_rows(ds: &Datastore, tb: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let table = TableName::from(tb);
	let range = RecordPrefix {
		ns,
		db,
		tb: Cow::Borrowed(&table),
	}
	.range()
	.unwrap();
	let rows = txn.scan_raw(range, u32::MAX, 0, None).await.unwrap();
	txn.cancel().await.unwrap();
	rows
}

async fn overwrite(ds: &Datastore, key: &[u8], value: Vec<u8>) {
	let txn = ds.transaction(Write).await.unwrap();
	txn.set(Key::from(key.to_vec()), value).await.unwrap();
	txn.commit().await.unwrap();
}

/// Replace the stored bytes of the `index`th record of `tb` with the start of
/// a v1 record that is shorter than its header, which no reader can decode.
/// The page that reads it fails. Answers the key and the bytes it held.
async fn corrupt(ds: &Datastore, tb: &str, index: usize) -> (Vec<u8>, Vec<u8>) {
	let (key, bytes) = raw_rows(ds, tb).await.swap_remove(index);
	overwrite(ds, &key, vec![crate::catalog::StoredRecord::MARKER]).await;
	(key, bytes)
}

/// The conformance of every field of `tb`: `a:G b:U` for a field the table holds a claim on that
/// holds for the field's current definition, and one it does not.
async fn conformance(ds: &Datastore, tb: &str) -> String {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let table = TableName::from(tb);
	let fields = txn.all_tb_fields(ns, db, &table, None).await.unwrap();
	let claims = txn.table_conformance(ns, db, &table, None).await.unwrap();
	txn.cancel().await.unwrap();
	let mut fields: Vec<String> = fields
		.iter()
		.map(|field| {
			let name = field.name.to_raw_string();
			let claimed = claims.as_ref().is_some_and(|claims| {
				claims.claim(&name).is_some_and(|claim| claim.holds_for(&field.to_stored()))
			});
			format!(
				"{name}:{}",
				if claimed {
					"G"
				} else {
					"U"
				}
			)
		})
		.collect();
	fields.sort();
	fields.join(" ")
}

async fn lease(ds: &Datastore) -> Option<TaskLease> {
	let txn = ds.transaction(Read).await.unwrap();
	let lease = txn
		.get_key(
			&TaskLeaseKey {
				task_id: TaskLeaseType::TableMigration,
			},
			None,
		)
		.await
		.unwrap();
	txn.cancel().await.unwrap();
	lease
}

/// Make the job's lease expire, as if its owner had been paused for longer
/// than the lease lasts.
async fn expire_lease(ds: &Datastore) {
	let held = lease(ds).await.expect("the job holds a lease");
	let txn = ds.transaction(Write).await.unwrap();
	txn.set_key(
		&TaskLeaseKey {
			task_id: TaskLeaseType::TableMigration,
		},
		&TaskLease::new(held.owner, chrono::Utc::now() - chrono::Duration::hours(1)),
	)
	.await
	.unwrap();
	txn.commit().await.unwrap();
}

/// `tb` with `rows` rows, `tb:0..rows`, each holding `f = id + 1`, under a
/// SCHEMAFULL `f TYPE int`, and a migration `dbl` that doubles every `f`.
async fn doubling_table(ds: &Datastore, rows: i64) {
	let mut sql = String::from("DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD f ON tb TYPE int;");
	for i in 0..rows {
		sql.push_str(&format!("CREATE tb:{i} SET f = {} RETURN NONE;", i + 1));
	}
	sql.push_str("ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS dbl USING $value * 2 FALLBACK 0;");
	run(ds, &sql).await;
}

/// Assert that `tb:id` holds `f == expected` stamped `generation`.
async fn assert_row(ds: &Datastore, id: i64, expected: i64, generation: u32) {
	assert_eq!(stored_int(ds, "tb", id, "f").await, (expected, generation), "tb:{id}");
}

/// The migrations the table's INFO lists, numbered in the order they ran, with the fields it
/// guarantees and the verifications it shows, as `steps [dbl@1] guaranteed [a] verify []`; `none`
/// when the table has no `migrations` section.
async fn section(ds: &Datastore, tb: &str) -> String {
	describe(&run(ds, &format!("(INFO FOR TABLE {tb}).migrations;")).await[0])
}

/// A `migrations` section of `INFO FOR TABLE`, on one line.
fn describe(section: &Value) -> String {
	let Value::Object(object) = section else {
		return "none".to_owned();
	};
	let list = |key: &str, each: &dyn Fn(&Value) -> String| match object.get(key) {
		Some(Value::Array(array)) => array.iter().map(each).collect::<Vec<_>>().join(","),
		None => String::new(),
		other => panic!("{key}: {other:?}"),
	};
	let text = |value: &Value| {
		if let Value::String(text) = value {
			text.clone()
		} else {
			value.to_sql()
		}
	};
	let member = |value: &Value, key: &str| {
		if let Value::Object(object) = value {
			object.get(key).map(text).unwrap_or_default()
		} else {
			String::new()
		}
	};
	let steps = match object.get("migrations") {
		Some(Value::Array(array)) => array
			.iter()
			.enumerate()
			.map(|(at, v)| format!("{}@{}", member(v, "name"), at + 1))
			.collect::<Vec<_>>()
			.join(","),
		None => String::new(),
		other => panic!("migrations: {other:?}"),
	};
	let guaranteed = list("guaranteed", &text);
	let verify = list("verification", &|v| {
		format!("{}:{}:{}", member(v, "field"), member(v, "failures"), member(v, "finished"))
	});
	let progress = match object.get("progress") {
		Some(progress) => format!(" progress {}", progress.to_sql()),
		None => String::new(),
	};
	let undeclared = match object.get("undeclared") {
		Some(check) => {
			format!(" undeclared {}:{}", member(check, "failures"), member(check, "finished"))
		}
		None => String::new(),
	};
	format!("steps [{steps}] guaranteed [{guaranteed}] verify [{verify}]{progress}{undeclared}")
}

// ---------------------------------------------------------------------------
// Pausing a pass inside a page.
// ---------------------------------------------------------------------------

/// A scheduler that holds one yield until it is released.
///
/// A program yields through its datastore's scheduler once it has spent its
/// quantum; with a quantum of one, the first unit of a page's step does. The
/// page has by then taken its snapshot and read the queue entry, so a pass
/// held here is an owner that went to sleep in the middle of a page.
#[derive(Default)]
struct Gate {
	armed: AtomicBool,
	blocked: AtomicUsize,
	paused: Notify,
	release: Notify,
}

impl Gate {
	/// Hold the next yield.
	fn arm(&self) {
		self.armed.store(true, Ordering::SeqCst);
	}

	/// Wait until a yield is held.
	async fn paused(&self) {
		tokio::time::timeout(Duration::from_secs(30), self.paused.notified())
			.await
			.expect("the pass never yielded: nothing was paused");
	}

	/// Let the held yield go.
	fn release(&self) {
		self.release.notify_one();
	}
}

struct GateScheduler(Arc<Gate>);

impl Scheduler for GateScheduler {
	fn yield_now(&self) -> YieldFuture {
		let gate = Arc::clone(&self.0);
		Box::pin(async move {
			if gate.armed.swap(false, Ordering::SeqCst) {
				gate.blocked.fetch_add(1, Ordering::SeqCst);
				gate.paused.notify_one();
				gate.release.notified().await;
			} else {
				tokio::task::yield_now().await;
			}
		})
	}
}

/// The datastore the test drives (`base`, a quantum of one) and a second
/// node over the same store (`gated`) whose scheduler is the gate.
async fn gated_nodes() -> (Arc<Datastore>, Arc<Datastore>, Arc<Gate>) {
	let base = datastore_with(4, Some(1)).await;
	let gate = Arc::new(Gate::default());
	let gated = Arc::new(
		base.fork_for_test_with_node_id(Uuid::new_v4())
			.with_scheduler(Arc::new(GateScheduler(Arc::clone(&gate)))),
	);
	(base, gated, gate)
}

/// A node over the same store as `ds`, as another compute node would be.
fn another_node(ds: &Datastore) -> Arc<Datastore> {
	Arc::new(ds.fork_for_test_with_node_id(Uuid::new_v4()))
}

/// Start a pass of `ds` that waits at most a second for older transactions,
/// so that it re-reads its lease every second.
fn spawn_pass(ds: &Arc<Datastore>) -> tokio::task::JoinHandle<anyhow::Result<()>> {
	let ds = Arc::clone(ds);
	tokio::spawn(async move {
		Datastore::table_migration(ds, Duration::from_secs(1), CancellationToken::new()).await
	})
}

/// Wait for a pass started with [`spawn_pass`] to return, and require that it
/// returned without an error.
async fn finish(pass: tokio::task::JoinHandle<anyhow::Result<()>>) {
	tokio::time::timeout(Duration::from_secs(60), pass)
		.await
		.expect("the pass did not end")
		.unwrap()
		.unwrap();
}

// ---------------------------------------------------------------------------
// A pass that stops, and a lease that changes hands.
// ---------------------------------------------------------------------------

/// A pass stopped mid-lap leaves its cursor in the queue entry, and the next
/// pass, on a datastore started afresh over the same store, reads on from
/// it: the rows before the cursor are not read again, and each row is
/// rewritten once.
#[tokio::test]
async fn a_pass_stopped_mid_lap_resumes_from_its_stored_cursor() {
	const ROWS: i64 = 600;
	const UNREADABLE: usize = 300;
	let ds = datastore(4).await;
	doubling_table(&ds, ROWS).await;
	// The page that reads row 300 fails; the pages before it have committed.
	let (bad_key, bad_bytes) = corrupt(&ds, "tb", UNREADABLE).await;
	drain(&ds).await;

	let state = entry(&ds, "tb").await.expect("a pass that stopped leaves its entry");
	let passed = state.scanned as usize;
	assert!(
		0 < passed && passed <= UNREADABLE,
		"the pass must have stopped mid-lap, having read {passed} rows"
	);
	assert_eq!(state.rewritten, state.scanned, "every row the pages read was behind the step");
	assert!(state.rewrite);
	let rows = raw_rows(&ds, "tb").await;
	assert_eq!(
		state.cursor.as_ref(),
		Some(&rows[passed - 1].0),
		"the cursor is the last key the committed pages passed"
	);
	for id in [0, passed as i64 - 1] {
		assert_row(&ds, id, 2 * (id + 1), 1).await;
	}
	for id in [passed as i64, 299, 301, ROWS - 1] {
		assert_row(&ds, id, id + 1, 0).await;
	}
	assert!(state.rewrite, "the lap has not ended");

	// The node restarts: a datastore over the same store with nothing held in
	// memory. The unreadable row is repaired, and a row the first pass passed
	// is broken instead: a pass that began again from the first record would
	// fail on it.
	let ds = Arc::new(Arc::try_unwrap(ds).ok().expect("no other handle").restart());
	overwrite(&ds, &bad_key, bad_bytes).await;
	let (passed_key, passed_bytes) = corrupt(&ds, "tb", 10).await;
	drain(&ds).await;
	assert!(
		entry(&ds, "tb").await.is_none(),
		"the resumed pass must finish the lap without reading what the first one passed"
	);
	overwrite(&ds, &passed_key, passed_bytes).await;

	// Every row was rewritten once: a second rewrite of a row, or a replay of
	// the step over its rewritten value, would store 4 * (id + 1).
	for id in 0..ROWS {
		assert_row(&ds, id, 2 * (id + 1), 1).await;
	}
	let doubled: Vec<String> = (0..ROWS).map(|id| (2 * (id + 1)).to_string()).collect();
	assert_eq!(
		run(&ds, "SELECT VALUE f FROM tb ORDER BY id;").await[0].to_sql(),
		format!("[{}]", doubled.join(", "))
	);
	assert_eq!(steps_run(&ds, "tb").await, 1);
}

/// The pass pages through the whole queue: a table whose progress sorts after more progress
/// entries than one batch holds, all of them asking for nothing (a finished verification kept
/// for `INFO`), is still worked.
#[tokio::test]
async fn a_pass_reaches_a_table_that_sorts_after_a_full_batch_of_idle_entries() {
	// More than one batch of the queue.
	const IDLE: u32 = 1100;
	let ds = datastore(4).await;
	doubling_table(&ds, 3).await;
	// A pass folds the table's requests and stops at a row it cannot read, so only the table's
	// progress asks for its lap.
	let (bad_key, bad_bytes) = corrupt(&ds, "tb", 1).await;
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_some_and(|job| job.has_work()));
	overwrite(&ds, &bad_key, bad_bytes).await;
	// Idle progress sorts before it: verifications that ended, as a pass leaves them.
	let (ns, db) = ids(&ds).await;
	let txn = ds.transaction(Write).await.unwrap();
	for ix in 0..IDLE {
		let table = TableName::new(format!("a{ix:04}"));
		let key = TableMigrationKey {
			ns,
			db,
			tb: Cow::Borrowed(&table),
		};
		let idle = TableMigrationState {
			lap: 1,
			verify: vec![VerifiedField {
				name: "f".into(),
				definition: 0,
				failures: 1,
				finished: true,
			}],
			..Default::default()
		};
		txn.set_key(&key, &idle).await.unwrap();
	}
	txn.commit().await.unwrap();
	drop(txn);
	assert!(!entry(&ds, "a0000").await.expect("an idle entry").has_work());

	drain(&ds).await;

	for id in 0..3 {
		assert_row(&ds, id, 2 * (id + 1), 1).await;
	}
	assert!(entry(&ds, "tb").await.is_none(), "the pass finished the table");
	assert!(entry(&ds, "a1099").await.is_some(), "an idle entry is kept");
}

/// The job runs on one node at a time. A node that finds the lease held
/// does nothing; once the lease has expired another node takes it and
/// reads on from the cursor the first left.
#[tokio::test]
async fn another_node_takes_the_expired_lease_and_resumes_at_the_cursor() {
	const ROWS: i64 = 600;
	let ds = datastore(4).await;
	doubling_table(&ds, ROWS).await;
	let (bad_key, bad_bytes) = corrupt(&ds, "tb", 300).await;
	drain(&ds).await;
	let stopped = entry(&ds, "tb").await.expect("the first pass stopped mid-lap");
	assert!(stopped.cursor.is_some());

	// While the lease is held, another node's pass changes nothing.
	let other = another_node(&ds);
	drain_for(&other, Duration::from_secs(1)).await;
	assert_eq!(entry(&ds, "tb").await.as_ref(), Some(&stopped));
	assert_eq!(lease(&ds).await.unwrap().owner, ds.id());

	// Once it has expired, the other node takes it and begins at the stored
	// cursor: it is not stopped by the row it would meet if it began again.
	expire_lease(&ds).await;
	let (passed_key, passed_bytes) = corrupt(&ds, "tb", 10).await;
	drain_for(&other, Duration::from_secs(1)).await;
	assert_eq!(lease(&ds).await.unwrap().owner, other.id(), "the lease changed hands");
	assert_eq!(
		entry(&ds, "tb").await.as_ref(),
		Some(&stopped),
		"the new owner stopped at the same unreadable row, from the same cursor"
	);
	overwrite(&ds, &bad_key, bad_bytes).await;
	drain_for(&other, Duration::from_secs(1)).await;
	assert!(entry(&ds, "tb").await.is_none(), "the new owner finished the lap");
	overwrite(&ds, &passed_key, passed_bytes).await;
	for id in 0..ROWS {
		assert_row(&ds, id, 2 * (id + 1), 1).await;
	}

	// A second migration queues the table again; the old owner no longer has
	// the lease, so its pass leaves the rows where they are.
	run(&ds, "ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS inc USING $value + 1 FALLBACK 0;").await;
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_some(), "the old owner must not work without the lease");
	assert_row(&ds, 3, 8, 1).await;
	expire_lease(&ds).await;
	drain(&ds).await;
	assert_eq!(lease(&ds).await.unwrap().owner, ds.id(), "the lease changed hands again");
	for id in 0..ROWS {
		assert_row(&ds, id, 2 * (id + 1) + 1, 2).await;
	}
}

/// An owner paused inside a page while its lease expires, and woken after
/// another node has taken the lease and passed pages of its own, cannot
/// commit the page it was building: the page was read from a snapshot older
/// than the new owner's commits and a user's write, and committing it would
/// put old rows over them. The old owner then finds the lease gone and
/// stands down at its next check; the new owner finishes from its cursor.
#[tokio::test]
async fn a_paused_owner_cannot_commit_its_page_after_the_lease_changed_hands() {
	const ROWS: i64 = 600;
	let (base, gated, gate) = gated_nodes().await;
	doubling_table(&base, ROWS).await;
	let (bad_key, bad_bytes) = corrupt(&base, "tb", 400).await;

	// The owner is held inside its first page: it has read the entry and its
	// snapshot, in which no row has been rewritten.
	gate.arm();
	let owner = spawn_pass(&gated);
	gate.paused().await;
	assert_eq!(lease(&base).await.unwrap().owner, gated.id());

	// Its lease expires. Another node takes it and passes pages of its own,
	// then stops at the unreadable row.
	expire_lease(&base).await;
	let successor = another_node(&base);
	drain_for(&successor, Duration::from_secs(1)).await;
	assert_eq!(lease(&base).await.unwrap().owner, successor.id());
	let state = entry(&base, "tb").await.expect("the successor's pass stopped mid-lap");
	let passed = state.scanned as i64;
	assert!(0 < passed && passed <= 400);

	// A user changes rows the owner's page would rewrite.
	run(&base, "UPDATE tb:5 SET f = 1000 RETURN NONE; DELETE tb:7 RETURN NONE;").await;

	// The owner wakes after its next lease check is due, and its page is
	// refused.
	tokio::time::sleep(Duration::from_millis(2100)).await;
	gate.release();
	finish(owner).await;
	assert_eq!(
		entry(&base, "tb").await.as_ref(),
		Some(&state),
		"the stale page must not move the successor's entry"
	);
	assert_row(&base, 5, 1000, 1).await;
	assert_eq!(
		run(&base, "SELECT VALUE id FROM tb:7;").await[0].to_sql(),
		"[]",
		"the stale page must not bring back a row a user deleted"
	);
	for id in [0, passed - 1] {
		if id != 5 && id != 7 {
			assert_row(&base, id, 2 * (id + 1), 1).await;
		}
	}
	for id in [passed, 399, 401, ROWS - 1] {
		assert_row(&base, id, id + 1, 0).await;
	}

	// The successor reads on from its cursor and ends the lap.
	overwrite(&base, &bad_key, bad_bytes).await;
	drain_for(&successor, Duration::from_secs(1)).await;
	assert!(entry(&base, "tb").await.is_none());
	for id in 0..ROWS {
		match id {
			5 => assert_row(&base, id, 1000, 1).await,
			7 => {}
			_ => assert_row(&base, id, 2 * (id + 1), 1).await,
		}
	}
	assert_eq!(steps_run(&base, "tb").await, 1);
}

/// An owner paused in a page while another node finishes the whole lap
/// commits nothing when it wakes: the rows it rewrote from its old
/// snapshot would otherwise undo a user's update and a user's delete.
#[tokio::test]
async fn a_paused_owner_cannot_undo_what_a_finished_lap_and_a_user_wrote() {
	const ROWS: i64 = 300;
	let (base, gated, gate) = gated_nodes().await;
	doubling_table(&base, ROWS).await;
	gate.arm();
	let owner = spawn_pass(&gated);
	gate.paused().await;

	expire_lease(&base).await;
	let successor = another_node(&base);
	drain_for(&successor, Duration::from_secs(1)).await;
	assert!(entry(&base, "tb").await.is_none(), "the successor finished the lap");
	run(&base, "UPDATE tb:5 SET f = 1000 RETURN NONE; DELETE tb:7 RETURN NONE; CREATE tb:900 SET f = 9 RETURN NONE;")
		.await;

	gate.release();
	finish(owner).await;
	assert!(entry(&base, "tb").await.is_none(), "the woken owner left no entry behind");
	assert_row(&base, 5, 1000, 1).await;
	assert_row(&base, 900, 9, 1).await;
	assert_eq!(run(&base, "SELECT VALUE id FROM tb:7;").await[0].to_sql(), "[]");
	for id in (0..ROWS).filter(|id| *id != 5 && *id != 7) {
		assert_row(&base, id, 2 * (id + 1), 1).await;
	}
	assert_eq!(steps_run(&base, "tb").await, 1);
}

/// A verification waits for the transactions older than the job's fold of
/// its request. An owner waiting there settles the progress it read before
/// the wait; a migration committed meanwhile leaves requests that progress
/// does not hold, and the next page folds them, which unsettles the
/// verification the migration asks for: the rewrite is carried out, and the
/// new definition is verified only after the transactions older than it.
#[tokio::test]
async fn an_owner_waiting_for_older_transactions_cannot_overwrite_a_migration_queued_meanwhile() {
	let ds = datastore(4).await;
	let mut sql = String::from("DEFINE TABLE tb SCHEMALESS;");
	for i in 0..20 {
		sql.push_str(&format!("CREATE tb:{i} SET a = {} RETURN NONE;", i + 1));
	}
	sql.push_str("DEFINE FIELD a ON tb TYPE int;");
	run(&ds, &sql).await;
	let queued = entry(&ds, "tb").await.expect("the new field queued a verification");
	assert!(!queued.settled && !queued.rewrite);

	// An older transaction is open, so the owner's pass waits.
	let older = ds.transaction(Read).await.unwrap();
	let owner = Arc::clone(&ds);
	let pass = tokio::spawn(async move {
		Datastore::table_migration(owner, Duration::from_secs(30), CancellationToken::new()).await
	});
	tokio::time::sleep(Duration::from_millis(200)).await;
	assert_eq!(entry(&ds, "tb").await.as_ref(), Some(&queued), "the pass must be waiting");

	// A migration commits while it waits.
	run(
		&ds,
		"ALTER FIELD a ON tb TYPE float MIGRATE EXISTING ROWS AS widen USING <float>$value * 2 FALLBACK 0.5;",
	)
	.await;
	let queued_by_migration = entry(&ds, "tb").await.unwrap();
	assert!(queued_by_migration.rewrite, "the migration queued a rewrite");

	// The older transaction ends; the owner wakes, settles what it read, and
	// folds the migration's requests on its next page.
	older.cancel().await.unwrap();
	drop(older);
	finish(pass).await;
	assert!(entry(&ds, "tb").await.is_none(), "the rewrite the migration queued was carried out");
	for id in 0..20 {
		let (data, generation) = stored(&ds, "tb", id).await;
		assert_eq!(generation, 1, "tb:{id} rewritten");
		let Stored::Object(object) = &data else {
			panic!("{data:?}")
		};
		assert_eq!(
			format!("{:?}", object.get("a")),
			format!("{:?}", Some(Stored::from(2.0 * (id + 1) as f64))),
			"tb:{id} migrated once"
		);
	}
	assert_eq!(conformance(&ds, "tb").await, "a:G");
}

// ---------------------------------------------------------------------------
// A statement beside the job.
// ---------------------------------------------------------------------------

/// Run `sql` in `txn`, a transaction the caller owns and commits, failing on
/// the first error.
async fn run_in(ds: &Datastore, txn: &Arc<Transaction>, sql: &str) {
	for response in
		ds.run(QueryRequest::new(sql, &session()).with_transaction(Arc::clone(txn))).await.unwrap()
	{
		response.result.unwrap();
	}
}

/// `doubling_table` with an untyped `f` beside a typed `g`: the migration of
/// `f` queues a rewrite and no verification, so the job's lap waits for no
/// older transaction.
async fn untyped_doubling_table(ds: &Datastore, rows: i64) {
	let mut sql = String::from(
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD f ON tb; DEFINE FIELD g ON tb TYPE int;",
	);
	for i in 0..rows {
		sql.push_str(&format!("CREATE tb:{i} SET f = {}, g = {i} RETURN NONE;", i + 1));
	}
	sql.push_str("ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS dbl USING $value * 2 FALLBACK 0;");
	run(ds, &sql).await;
}

/// A migration's transaction is open while the job passes every page of a lap
/// over the same table, the one that ends the lap included. The migration's
/// request is a key of its own, which no page writes, so the migration commits;
/// and the lap the job ended without seeing its step leaves the request in
/// place, so the next pass rewrites every row through it.
#[tokio::test]
async fn a_migration_commits_beside_the_pages_the_job_commits_for_its_table() {
	const ROWS: i64 = 300;
	let ds = datastore(4).await;
	untyped_doubling_table(&ds, ROWS).await;

	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	run_in(
		&ds,
		&txn,
		"ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS inc USING $value + 1 FALLBACK 0;",
	)
	.await;
	// The job passes the first migration's lap, in more than one page.
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_none(), "the lap ended without the open step");
	assert_row(&ds, ROWS - 1, 2 * ROWS, 1).await;

	txn.commit().await.expect("a page of the job must not fail the migration's commit");
	drop(txn);
	assert!(entry(&ds, "tb").await.is_some_and(|job| job.rewrite), "the migration queued a lap");
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_none());
	for id in 0..ROWS {
		assert_row(&ds, id, 2 * (id + 1) + 1, 2).await;
	}
}

/// Raising a database to compatibility level 4 queues a verification of every
/// `SCHEMAFULL` table with rows. Its transaction is open while the job
/// rewrites one of those tables, and commits: the verification is a request
/// of its own, which the job carries out once the rewrite is done.
#[tokio::test]
async fn raising_the_compatibility_level_commits_beside_the_pages_the_job_commits() {
	const ROWS: i64 = 300;
	let ds = datastore(3).await;
	untyped_doubling_table(&ds, ROWS).await;

	let txn = Arc::new(ds.transaction(Write).await.unwrap());
	run_in(&ds, &txn, "ALTER DATABASE COMPATIBILITY 4;").await;
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_none(), "the rewrite ended");

	txn.commit().await.expect("a page of the job must not fail the raise's commit");
	drop(txn);
	let queued = entry(&ds, "tb").await.expect("the raise queued a verification");
	assert!(queued.verify.iter().any(|field| field.name == "g" && !field.finished));
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_none());
	assert!(conformance(&ds, "tb").await.contains("g:G"));
}

/// An `OPTION IMPORT` write into a table the job is rewriting forfeits the
/// table's claims and reads whether a verification is under way, which it
/// must start over. The read takes no lock, so the job's pages over the table
/// do not fail the writer's commit. The writer's calls are made by hand: a
/// transaction its caller owns runs no `OPTION` statement.
#[tokio::test]
async fn an_import_write_commits_beside_the_pages_the_job_commits_for_its_table() {
	const ROWS: i64 = 300;
	let ds = datastore(4).await;
	untyped_doubling_table(&ds, ROWS).await;
	let (ns, db) = ids(&ds).await;
	let table = TableName::from("tb");

	let txn = ds.transaction(Write).await.unwrap();
	txn.forfeit_conformance(ns, db, &table).await.unwrap();
	txn.restart_tb_verification(ns, db, &table).await.unwrap();
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_none(), "the rewrite ended");

	txn.commit().await.expect("a page of the job must not fail the import's commit");
	drop(txn);
	assert!(entry(&ds, "tb").await.is_none(), "no verification was under way to restart");
	assert_row(&ds, ROWS - 1, 2 * ROWS, 1).await;
}

/// How many keys the job's queue holds, progress and requests, over every table.
async fn queue_keys(ds: &Datastore) -> usize {
	let txn = ds.transaction(Read).await.unwrap();
	let progress = txn.keys(TableMigrationPrefix {}.range().unwrap(), u32::MAX, 0, None);
	let progress = progress.await.unwrap().len();
	let requests = txn.keys(TableMigrationRequestPrefix {}.range().unwrap(), u32::MAX, 0, None);
	let requests = requests.await.unwrap().len();
	txn.cancel().await.unwrap();
	progress + requests
}

/// The removal of a table, of its database or of its namespace is open while the job passes a
/// check of the table, writing its progress on every page. No removal writes the progress or
/// deletes a request, so each commits; the job then deletes what it held for the table. The check
/// is settled by hand, so the lap waits for no older transaction, and it rewrites no row.
#[tokio::test]
async fn a_removal_commits_beside_the_pages_the_job_commits_for_its_table() {
	for removal in ["REMOVE TABLE t;", "REMOVE DATABASE test;", "REMOVE NAMESPACE test;"] {
		let ds = datastore(4).await;
		run(
			&ds,
			"DEFINE TABLE t SCHEMALESS;
			 DEFINE FIELD a ON t TYPE int;
			 CREATE t:1 SET a = 1, z = 1;
			 CREATE t:2 SET a = 2;
			 ALTER TABLE t SCHEMAFULL;",
		)
		.await;
		let (ns, db) = ids(&ds).await;
		let table = TableName::from("t");
		{
			let txn = ds.transaction(Write).await.unwrap();
			let mut state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
			state.verify.clear();
			state.settled = true;
			for (request, _) in txn.tb_migration_requests(ns, db, &table, u32::MAX).await.unwrap() {
				txn.del(Key::from(request)).await.unwrap();
			}
			let key = TableMigrationKey {
				ns,
				db,
				tb: Cow::Borrowed(&table),
			};
			txn.set_key(&key, &state).await.unwrap();
			txn.commit().await.unwrap();
		}

		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		run_in(&ds, &txn, removal).await;
		drain(&ds).await;
		let check = entry(&ds, "t").await.and_then(|state| state.undeclared).unwrap();
		assert_eq!((check.failures, check.finished), (1, true), "{removal}");

		txn.commit().await.expect("a page of the job must not fail the removal's commit");
		drop(txn);
		drain(&ds).await;
		assert_eq!(queue_keys(&ds).await, 0, "{removal}");
	}
}

/// A table removed while a page of the job over it is under way fails the page's commit, so no
/// row is written beneath the removed table, and the job then deletes what it held for the table.
#[tokio::test]
async fn a_page_under_way_when_its_table_is_removed_commits_nothing() {
	const ROWS: i64 = 300;
	let (base, gated, gate) = gated_nodes().await;
	untyped_doubling_table(&base, ROWS).await;

	// The owner is held inside its first page of rows.
	gate.arm();
	let owner = spawn_pass(&gated);
	gate.paused().await;
	run(&base, "REMOVE TABLE tb;").await;
	gate.release();
	finish(owner).await;

	assert!(raw_rows(&base, "tb").await.is_empty());
	assert_eq!(queue_keys(&base).await, 0);
}

/// A table defined again under the name of one removed before the job visited it inherits
/// nothing the job held for the removed one.
#[tokio::test]
async fn a_table_defined_again_inherits_nothing_queued_for_the_removed_one() {
	let ds = datastore(4).await;
	untyped_doubling_table(&ds, 3).await;
	assert!(entry(&ds, "tb").await.is_some_and(|job| job.rewrite));
	run(&ds, "REMOVE TABLE tb; DEFINE TABLE tb SCHEMALESS; CREATE tb:1 SET f = 1 RETURN NONE;")
		.await;
	assert_eq!(entry(&ds, "tb").await, None);
	drain(&ds).await;
	assert_eq!(queue_keys(&ds).await, 0);
	assert_eq!(run(&ds, "SELECT VALUE f FROM tb;").await[0].to_sql(), "[1]");
}

/// A field change forfeits the table's conformance claims, a write of the
/// claims record, and the page that ends a verifying lap writes the same
/// record when it makes a claim. A field change whose transaction is open
/// across that page therefore still fails to commit.
#[tokio::test]
#[ignore = "BUG: a field change loses a write conflict on the claims record to the job's promotion"]
async fn a_field_change_commits_beside_the_promotion_the_job_commits_for_its_table() {
	const ROWS: i64 = 300;
	let (base, gated, gate) = gated_nodes().await;
	doubling_table(&base, ROWS).await;

	// The owner has waited for the transactions older than the verification,
	// and is held inside its first page of rows.
	gate.arm();
	let owner = spawn_pass(&gated);
	gate.paused().await;
	let txn = Arc::new(base.transaction(Write).await.unwrap());
	run_in(&base, &txn, "DEFINE FIELD h ON tb TYPE option<int>;").await;
	gate.release();
	finish(owner).await;
	assert_eq!(conformance(&base, "tb").await, "f:G");

	txn.commit().await.expect("the job's promotion must not fail the field change's commit");
}

/// A migration that commits while a page of the job is under way does not
/// fail the page, which commits the rows it rewrote; the page after it folds
/// the migration's request, and the lap the page belonged to ends into
/// another, which brings every row through the new step.
#[tokio::test]
async fn a_migration_committed_while_a_page_is_under_way_is_carried_out_by_the_next_lap() {
	const ROWS: i64 = 300;
	let (base, gated, gate) = gated_nodes().await;
	doubling_table(&base, ROWS).await;

	// The owner folds the first migration's request, then is held inside its
	// first page of rows.
	gate.arm();
	let owner = spawn_pass(&gated);
	gate.paused().await;
	run(&base, "ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS inc USING $value + 1 FALLBACK 0;")
		.await;
	gate.release();
	finish(owner).await;

	assert!(entry(&base, "tb").await.is_none());
	for id in 0..ROWS {
		assert_row(&base, id, 2 * (id + 1) + 1, 2).await;
	}
}

// ---------------------------------------------------------------------------
// A writer older than a migration.
// ---------------------------------------------------------------------------

/// A transaction that began before a migration commits a row after the job's
/// lap has passed. The row was written under the old definition, so it is
/// stamped with the generation the writer could see, below the newest step;
/// every read applies the step to it once, and the next lap rewrites it
/// through the steps it has not passed.
///
/// The migrated field has no type, so no verification waits for the older
/// writer and the lap passes ahead of it.
#[tokio::test]
async fn a_row_committed_by_a_writer_older_than_a_migration_still_reads_migrated_once() {
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb;
		 CREATE tb:1 SET f = 1 RETURN NONE;
		 CREATE tb:2 SET f = 2 RETURN NONE;",
	)
	.await;

	// The writer begins; the migration commits; the job rewrites every row.
	let late = Arc::new(ds.transaction(Write).await.unwrap());
	run(&ds, "ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS dbl USING $value * 2 FALLBACK 0;").await;
	drain(&ds).await;
	assert_eq!(steps_run(&ds, "tb").await, 1);

	// The writer commits a row after the lap passed.
	for response in ds
		.run(
			QueryRequest::new("CREATE tb:3 SET f = 10 RETURN NONE;", &session())
				.with_transaction(Arc::clone(&late)),
		)
		.await
		.unwrap()
	{
		response.result.unwrap();
	}
	late.commit().await.unwrap();
	assert_row(&ds, 3, 10, 0).await;
	assert_eq!(steps_run(&ds, "tb").await, 1);

	// Every read path sees it migrated; none sees the stored 10.
	let reads = "
		SELECT VALUE f FROM tb ORDER BY id;
		SELECT VALUE id FROM tb WHERE f > 15;
		SELECT VALUE f FROM ONLY tb:3;
		tb:3.f;
		SELECT math::sum(f) AS s FROM tb GROUP ALL;
		SELECT VALUE type::is_int(f) FROM tb:3;";
	let expected = ["[2, 4, 20]", "[tb:3]", "20", "20", "[{ s: 26 }]", "[true]"];
	let answers = run(&ds, reads).await;
	for (answer, expected) in answers.iter().zip(expected) {
		assert_eq!(answer.to_sql(), expected);
	}
	// No pass is queued: the row stays where it is, and reads the same.
	drain(&ds).await;
	assert_row(&ds, 3, 10, 0).await;
	let after_pass: Vec<String> = run(&ds, reads).await.iter().map(ToSql::to_sql).collect();
	assert_eq!(after_pass, expected);

	// The next migration queues a lap, which passes through every step a row
	// has not passed: tb:3 through both, tb:1 and tb:2 through the second.
	run(&ds, "ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS inc USING $value + 1 FALLBACK 0;").await;
	assert_eq!(run(&ds, "SELECT VALUE f FROM tb ORDER BY id;").await[0].to_sql(), "[3, 5, 21]");
	drain(&ds).await;
	assert_row(&ds, 1, 3, 2).await;
	assert_row(&ds, 2, 5, 2).await;
	assert_row(&ds, 3, 21, 2).await;
	assert_eq!(run(&ds, "SELECT VALUE f FROM tb ORDER BY id;").await[0].to_sql(), "[3, 5, 21]");
	assert_eq!(steps_run(&ds, "tb").await, 2);
}

/// A transaction its caller owns keeps the writes of a statement that failed
/// in it. A migration a check of its definition refuses leaves no step and no
/// rewrite there for the caller to commit, so the rows read as they did.
#[tokio::test]
async fn a_refused_migration_leaves_no_step_in_a_transaction_its_caller_commits() {
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD f ON tb TYPE string; \
		 DEFINE FIELD labels ON tb TYPE set<string>; \
		 CREATE tb:1 SET f = '1', labels = <set>['2'] RETURN NONE;",
	)
	.await;
	for refused in [
		// Only a kind holding an object can be `FLEXIBLE`.
		"ALTER FIELD f ON tb FLEXIBLE TYPE int MIGRATE EXISTING ROWS AS flex USING <int>$value \
		 FALLBACK 0;",
		// The elements of `labels` are strings.
		"ALTER FIELD labels.* ON tb TYPE int MIGRATE EXISTING ROWS AS ints USING <int>$value \
		 FALLBACK 0;",
	] {
		let txn = Arc::new(ds.transaction(Write).await.unwrap());
		let refusal = ds
			.run(QueryRequest::new(refused, &session()).with_transaction(Arc::clone(&txn)))
			.await
			.unwrap()
			.into_iter()
			.find_map(|response| response.result.err());
		assert!(refusal.is_some(), "`{refused}` was expected to be refused");
		txn.commit().await.unwrap();
		assert_eq!(steps_run(&ds, "tb").await, 0, "`{refused}` left a step: {refusal:?}");
	}
	drain(&ds).await;
	assert_eq!(
		run(&ds, "SELECT f, labels FROM tb;").await[0].to_sql(),
		"[{ f: '1', labels: {'2',} }]"
	);
	assert_eq!(steps_run(&ds, "tb").await, 0);
}

/// A transaction older than a migration updates a row the job has since
/// rewritten. Whether the store lets it commit or refuses it as a conflict,
/// the row reads as one application of the step to the value the writer
/// stored, or as the job's rewrite; never twice.
///
/// The migrated field has no type, so no verification waits for the older
/// writer and the lap passes ahead of it.
#[tokio::test]
async fn a_writer_older_than_a_migration_updating_a_rewritten_row_never_doubles_it() {
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD f ON tb; CREATE tb:1 SET f = 1 RETURN NONE;",
	)
	.await;
	let late = Arc::new(ds.transaction(Write).await.unwrap());
	run(&ds, "ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS dbl USING $value * 2 FALLBACK 0;").await;
	drain(&ds).await;
	assert_row(&ds, 1, 2, 1).await;

	let wrote = ds
		.run(
			QueryRequest::new("UPDATE tb:1 SET f = 7 RETURN NONE;", &session())
				.with_transaction(Arc::clone(&late)),
		)
		.await
		.unwrap()
		.into_iter()
		.all(|response| response.result.is_ok());
	let committed = wrote && late.commit().await.is_ok();
	let (raw, generation) = stored_int(&ds, "tb", 1, "f").await;
	let read = run(&ds, "SELECT VALUE f FROM ONLY tb:1;").await[0].to_sql();
	if committed {
		assert_eq!((raw, generation), (7, 0), "the writer's value, stamped as it saw it");
		assert_eq!(read, "14", "read through the step once");
	} else {
		let _ = late.cancel().await;
		assert_eq!((raw, generation), (2, 1), "the job's rewrite stands");
		assert_eq!(read, "2");
	}
}

// ---------------------------------------------------------------------------
// Verification.
// ---------------------------------------------------------------------------

/// A row that does not fit a field's type, written before the type changed
/// without a migration, is reported by the verification, and the field is not
/// promoted; once the row is repaired and the field verified again, it is.
#[tokio::test]
async fn a_misfit_row_is_reported_and_the_field_is_promoted_only_once_it_is_repaired() {
	// A SCHEMALESS table at level 4 takes a retype without a migration.
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMALESS;
		 CREATE tb:1 SET a = 1 RETURN NONE;
		 CREATE tb:2 SET a = 'x' RETURN NONE;
		 CREATE tb:3 SET a = 3 RETURN NONE;
		 DEFINE FIELD a ON tb TYPE int | string;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(conformance(&ds, "tb").await, "a:G", "every row fits int | string");

	run(&ds, "ALTER FIELD a ON tb TYPE int;").await;
	assert_eq!(conformance(&ds, "tb").await, "a:U", "a retype guarantees nothing until verified");
	drain(&ds).await;
	assert_eq!(conformance(&ds, "tb").await, "a:U", "tb:2 does not fit: the field stays unchecked");
	assert_eq!(section(&ds, "tb").await, "steps [] guaranteed [] verify [a:1:true]");
	assert_eq!(run(&ds, "SELECT VALUE a FROM ONLY tb:2;").await[0].to_sql(), "'x'");

	// Repair the row and verify again.
	run(
		&ds,
		"UPDATE tb:2 SET a = 2 RETURN NONE; REMOVE FIELD a ON tb; DEFINE FIELD a ON tb TYPE int;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(conformance(&ds, "tb").await, "a:G");
	assert_eq!(section(&ds, "tb").await, "none");
	assert!(entry(&ds, "tb").await.is_none());

	// Rows stored under level 3 are verified when the database rises to level
	// 4, and a misfit among them is reported there too.
	let ds = datastore(3).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD a ON tb TYPE int | string;
		 CREATE tb:1 SET a = 1 RETURN NONE;
		 CREATE tb:2 SET a = 'x' RETURN NONE;
		 ALTER FIELD a ON tb TYPE int;
		 ALTER DATABASE COMPATIBILITY 4;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(conformance(&ds, "tb").await, "a:U");
	assert_eq!(section(&ds, "tb").await, "steps [] guaranteed [] verify [a:1:true]");
	run(&ds, "UPDATE tb:2 SET a = 2 RETURN NONE; ALTER DATABASE COMPATIBILITY 3; ALTER DATABASE COMPATIBILITY 4;")
		.await;
	drain(&ds).await;
	assert_eq!(conformance(&ds, "tb").await, "a:G");
	assert_eq!(section(&ds, "tb").await, "steps [] guaranteed [a] verify []");
}

/// While a transaction older than a change is open, the job neither
/// promotes a verified field nor begins the lap that waits behind it: it
/// cannot know that the transaction will not commit a row the verification
/// has passed. When the transaction ends, the next pass does both.
#[tokio::test]
async fn a_verification_and_the_lap_behind_it_wait_for_an_older_open_transaction() {
	let ds = datastore(4).await;
	let mut sql = String::from("DEFINE TABLE tb SCHEMALESS;");
	for i in 0..20 {
		sql.push_str(&format!("CREATE tb:{i} SET a = {}, b = {} RETURN NONE;", i + 1, i + 1));
	}
	run(&ds, &sql).await;

	// The transaction opens before the change.
	let older = ds.transaction(Read).await.unwrap();
	run(
		&ds,
		"DEFINE FIELD a ON tb TYPE int;
		 DEFINE FIELD b ON tb TYPE int MIGRATE EXISTING ROWS AS dbl USING $value * 2 FALLBACK 0;",
	)
	.await;
	let queued = entry(&ds, "tb").await.expect("the change queued work");
	assert!(queued.rewrite && !queued.settled);

	for _ in 0..2 {
		drain_for(&ds, Duration::from_millis(100)).await;
		let held = entry(&ds, "tb").await.expect("the entry stays while the transaction is open");
		assert_eq!(held, queued, "nothing was recorded");
		assert_eq!(conformance(&ds, "tb").await, "a:U b:U", "neither field is promoted");
		assert_eq!(
			section(&ds, "tb").await,
			"steps [dbl@1] guaranteed [] verify [a:0:false,b:0:false] progress { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 }"
		);
		assert_eq!(stored_int(&ds, "tb", 0, "b").await, (1, 0), "no row was rewritten");
	}

	older.cancel().await.unwrap();
	drop(older);
	drain_for(&ds, Duration::from_millis(100)).await;
	assert!(entry(&ds, "tb").await.is_none());
	assert_eq!(conformance(&ds, "tb").await, "a:G b:G");
	assert_eq!(section(&ds, "tb").await, "steps [dbl@1] guaranteed [a,b] verify []");
	assert_eq!(stored_int(&ds, "tb", 0, "b").await, (2, 1));
	assert_eq!(stored_int(&ds, "tb", 19, "b").await, (40, 1));
}

/// A table with a rewrite queued and nothing to verify does not wait for an
/// older transaction: a rewrite is a saving on reads, not a promise, and a
/// row the older transaction commits later is stamped below the newest step
/// and read through the steps. A migration that removes the undeclared fields
/// verifies nothing: no field is typed by it.
#[tokio::test]
async fn a_rewrite_with_nothing_to_verify_does_not_wait_for_an_older_transaction() {
	let ds = datastore(4).await;
	let mut sql = String::from("DEFINE TABLE tb SCHEMALESS; DEFINE FIELD a ON tb TYPE int;");
	for i in 0..20 {
		sql.push_str(&format!("CREATE tb:{i} SET a = {i}, extra = {i} RETURN NONE;"));
	}
	sql.push_str("ALTER TABLE tb MIGRATE EXISTING ROWS AS tidy REMOVE UNDECLARED FIELDS;");
	run(&ds, &sql).await;
	let queued = entry(&ds, "tb").await;
	assert!(
		queued.as_ref().is_some_and(|queued| queued.rewrite && queued.verify.is_empty()),
		"{queued:?}"
	);
	let older = ds.transaction(Read).await.unwrap();
	drain_for(&ds, Duration::from_millis(100)).await;
	assert!(entry(&ds, "tb").await.is_none());
	assert_eq!(steps_run(&ds, "tb").await, 1);
	let (data, generation) = stored(&ds, "tb", 0).await;
	assert_eq!(generation, 1);
	let Stored::Object(object) = &data else {
		panic!("{data:?}")
	};
	assert!(object.get("extra").is_none(), "the undeclared field was removed: {object:?}");
	older.cancel().await.unwrap();
}

// ---------------------------------------------------------------------------
// A row reads the same replayed, rewritten and freshly written.
// ---------------------------------------------------------------------------

/// A 64-bit linear congruential sequence, so a case writes the same rows on
/// every run and every machine.
struct Lcg(u64);

impl Lcg {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		self.0 >> 33
	}

	fn below(&mut self, n: usize) -> usize {
		(self.next() % n as u64) as usize
	}

	fn pick<'a>(&mut self, pool: &[&'a str]) -> &'a str {
		pool[self.below(pool.len())]
	}

	/// A signed integer in `low..=high`.
	fn int(&mut self, low: i64, high: i64) -> i64 {
		low + self.below((high - low + 1) as usize) as i64
	}
}

/// The statements of one case, in order: definitions, rows, and the steps
/// that make rows stale, with rows written between the steps.
struct Script {
	name: &'static str,
	schemafull: bool,
	statements: Vec<String>,
}

/// `CREATE tb:{id} {body}` for each `(id, body)`.
fn creates(rows: impl IntoIterator<Item = (i64, String)>) -> Vec<String> {
	rows.into_iter().map(|(id, body)| format!("CREATE tb:{id} {body} RETURN NONE;")).collect()
}

fn scripts() -> Vec<Script> {
	let mut rng = Lcg(0x5eed_1234);
	let mut scripts = Vec::new();

	// Strings to integers, with a FALLBACK taking effect for what does not
	// convert, then integers to floats, then floats to optional text: three
	// steps, rows written under each generation, a row without the field.
	let mut statements =
		vec!["DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD a ON tb TYPE string;".to_owned()];
	const STRINGS: [&str; 11] =
		["0", "7", "-3", "x", "", "  12 ", "3.5", "9223372036854775807", "1e3", "abc", "-0"];
	statements.extend(creates((0..24).map(|i| (i, format!("SET a = '{}'", rng.pick(&STRINGS))))));
	statements.push(
		"ALTER FIELD a ON tb TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 99;"
			.to_owned(),
	);
	statements.extend(creates((0..8).map(|i| (100 + i, format!("SET a = {}", rng.int(-5, 5))))));
	statements.push(
		"ALTER FIELD a ON tb TYPE float MIGRATE EXISTING ROWS AS to_float USING <float>$value * 2 FALLBACK 0.5;"
			.to_owned(),
	);
	statements.extend(creates(
		(0..8).map(|i| (200 + i, format!("SET a = <float>{}", rng.int(-100, 100)))),
	));
	statements.push(
		"ALTER FIELD a ON tb TYPE option<string> MIGRATE EXISTING ROWS AS to_text USING <string>$value FALLBACK 'none';"
			.to_owned(),
	);
	statements.extend(creates((0..4).map(|i| (300 + i, format!("SET a = 'k{i}'")))));
	statements.extend(creates([(310, String::new())]));
	scripts.push(Script {
		name: "strings to ints to floats to text",
		schemafull: true,
		statements,
	});

	// Integers to floats: a value a float holds exactly converts, one it
	// would round takes the FALLBACK.
	const INTS: [&str; 9] = [
		"0",
		"1",
		"-1",
		"42",
		"9007199254740992",
		"9007199254740993",
		"9223372036854775807",
		"-9223372036854775807",
		"4611686018427387904",
	];
	let mut statements =
		vec!["DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD a ON tb TYPE int;".to_owned()];
	statements.extend(creates((0..20).map(|i| (i, format!("SET a = {}", rng.pick(&INTS))))));
	statements.push(
		"ALTER FIELD a ON tb TYPE float MIGRATE EXISTING ROWS AS widen USING $value FALLBACK 0.5;"
			.to_owned(),
	);
	statements.extend(creates([(100, "SET a = 3".to_owned()), (101, "SET a = 0.25".to_owned())]));
	scripts.push(Script {
		name: "integers to floats, lossy ones falling back",
		schemafull: true,
		statements,
	});

	// A removed field and the undeclared fields beneath a flexible one, then
	// every undeclared top-level field.
	let mut statements = vec![
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD a ON tb TYPE int;
		 DEFINE FIELD b ON tb TYPE option<string>;
		 DEFINE FIELD c ON tb TYPE option<object> FLEXIBLE;"
			.to_owned(),
	];
	statements.extend(creates((0..16).map(|i| {
		let b = if rng.below(3) == 0 {
			String::new()
		} else {
			format!(", b = 'x{i}'")
		};
		let c = match rng.below(3) {
			0 => String::new(),
			1 => format!(", c = {{ k: {i} }}"),
			_ => ", c = { k: 'v', z: [1, 2] }".to_owned(),
		};
		(i, format!("SET a = {i}{b}{c}"))
	})));
	statements.push("REMOVE FIELD b ON tb;".to_owned());
	statements.extend(creates((100..104).map(|i| (i, format!("SET a = {i}")))));
	statements.push(
		"ALTER TABLE tb MIGRATE EXISTING ROWS AS drop_rest REMOVE UNDECLARED FIELDS;".to_owned(),
	);
	statements.extend(creates((200..203).map(|i| (i, format!("SET a = {i}")))));
	scripts.push(Script {
		name: "a removed field, then every undeclared field",
		schemafull: true,
		statements,
	});

	// Fields no definition declares, in a table that becomes SCHEMAFULL.
	let mut statements = vec!["DEFINE TABLE tb SCHEMALESS;".to_owned()];
	statements.extend(creates((0..16).map(|i| {
		let extra = match rng.below(4) {
			0 => String::new(),
			1 => ", x = 1".to_owned(),
			2 => ", y = { n: [1, 2] }, z = 'q'".to_owned(),
			_ => ", x = NULL, z = [{ k: 1 }]".to_owned(),
		};
		(i, format!("SET a = {i}{extra}"))
	})));
	statements.push("DEFINE FIELD a ON tb TYPE int; ALTER TABLE tb SCHEMAFULL;".to_owned());
	statements.push(
		"ALTER TABLE tb MIGRATE EXISTING ROWS AS drop_rest REMOVE UNDECLARED FIELDS;".to_owned(),
	);
	statements.extend(creates((100..103).map(|i| (i, format!("SET a = {i}")))));
	scripts.push(Script {
		name: "undeclared fields removed",
		schemafull: true,
		statements,
	});

	// A nested path `a.b`: a row with the field present, with it absent, with
	// the parent absent; a step that keeps absence and one that fills it.
	let mut statements = vec![
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD a ON tb TYPE option<object>;
		 DEFINE FIELD a.b ON tb TYPE option<int>;"
			.to_owned(),
	];
	statements.extend(creates((0..20).map(|i| {
		let body = match rng.below(3) {
			0 => String::new(),
			1 => "SET a = {}".to_owned(),
			_ => format!("SET a = {{ b: {} }}", rng.int(-3, 3)),
		};
		(i, body)
	})));
	statements.push(
		"ALTER FIELD a.b ON tb TYPE option<float> MIGRATE EXISTING ROWS AS b_float USING $value && <float>$value * 1.5 FALLBACK 0.25;"
			.to_owned(),
	);
	statements
		.extend(creates((100..106).map(|i| (i, format!("SET a = {{ b: <float>{} }}", i - 100)))));
	statements.push(
		"ALTER FIELD a.b ON tb TYPE option<float> MIGRATE EXISTING ROWS AS fill USING $value ?? 0.75 FALLBACK 0.25;"
			.to_owned(),
	);
	scripts.push(Script {
		name: "a nested field, kept absent then filled",
		schemafull: true,
		statements,
	});

	// Array elements `a.*`: empty arrays, longer ones, a row without the
	// array; a step that changes each element's type and one that keeps it.
	let mut statements = vec![
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD a ON tb TYPE option<array>;
		 DEFINE FIELD OVERWRITE a.* ON tb TYPE int;"
			.to_owned(),
	];
	statements.extend(creates((0..20).map(|i| {
		let body = match rng.below(4) {
			0 => String::new(),
			1 => "SET a = []".to_owned(),
			_ => {
				let elements: Vec<String> =
					(0..1 + rng.below(4)).map(|_| rng.int(-5, 15).to_string()).collect();
				format!("SET a = [{}]", elements.join(", "))
			}
		};
		(i, body)
	})));
	statements.push(
		"ALTER FIELD a.* ON tb TYPE float MIGRATE EXISTING ROWS AS widen USING <float>$value * 2 FALLBACK 0.5;"
			.to_owned(),
	);
	statements
		.extend(creates([(100, "SET a = [1, 2]".to_owned()), (101, "SET a = []".to_owned())]));
	statements.push(
		"ALTER FIELD a.* ON tb MIGRATE EXISTING ROWS AS shift USING $value + 0.5 FALLBACK 0.5;"
			.to_owned(),
	);
	scripts.push(Script {
		name: "array elements widened, then shifted",
		schemafull: true,
		statements,
	});

	// Fields of the objects an array holds, `a.*.b`.
	let mut statements = vec![
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD a ON tb TYPE array<object>;
		 DEFINE FIELD a.*.b ON tb TYPE option<int>;"
			.to_owned(),
	];
	statements.extend(creates((0..16).map(|i| {
		let elements: Vec<String> = (0..rng.below(4))
			.map(|_| match rng.below(3) {
				0 => "{}".to_owned(),
				_ => format!("{{ b: {} }}", rng.int(-4, 4)),
			})
			.collect();
		(i, format!("SET a = [{}]", elements.join(", ")))
	})));
	statements.push(
		"ALTER FIELD a.*.b ON tb TYPE option<float> MIGRATE EXISTING ROWS AS b_float USING <float>$value + 0.5 FALLBACK 0.0;"
			.to_owned(),
	);
	statements.extend(creates([(100, "SET a = [{ b: 1.5 }, {}]".to_owned())]));
	scripts.push(Script {
		name: "the field of each object in an array",
		schemafull: true,
		statements,
	});

	// An option and an object: a value of one kind becomes another, and a
	// row without the field stays so.
	let mut statements = vec![
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD o ON tb TYPE option<object> FLEXIBLE;".to_owned(),
	];
	statements.extend(creates((0..16).map(|i| {
		let body = match rng.below(4) {
			0 => String::new(),
			1 => "SET o = {}".to_owned(),
			2 => format!("SET o = {{ x: {i} }}"),
			_ => "SET o = { x: 'q', y: [1, 2] }".to_owned(),
		};
		(i, body)
	})));
	statements.push(
		"ALTER FIELD o ON tb DROP FLEXIBLE TYPE option<array<string>> MIGRATE EXISTING ROWS AS keys USING $value && object::keys($value) FALLBACK [];"
			.to_owned(),
	);
	statements.extend(creates([(100, "SET o = ['k']".to_owned()), (101, String::new())]));
	scripts.push(Script {
		name: "an object becomes its keys",
		schemafull: true,
		statements,
	});

	scripts
}

/// The numeric ids of the records of `tb`, in order.
async fn row_ids(ds: &Datastore, tb: &str) -> Vec<i64> {
	let Value::Array(ids) = &run(ds, &format!("SELECT VALUE id FROM {tb} ORDER BY id;")).await[0]
	else {
		panic!("ids are an array");
	};
	ids.iter()
		.map(|id| id.to_sql().rsplit(':').next().unwrap().parse().expect("a numeric key"))
		.collect()
}

/// Every row of `tb`, by a scan and by a read of each id.
async fn read_rows(ds: &Datastore, ids: &[i64]) -> String {
	let mut out = run(ds, "SELECT * FROM tb ORDER BY id;").await[0].to_sql();
	for id in ids {
		out.push('\n');
		out.push_str(&run(ds, &format!("SELECT * FROM ONLY tb:{id};")).await[0].to_sql());
	}
	out
}

/// The newest generation of `tb`'s ring.
async fn head(ds: &Datastore, tb: &str) -> Option<Generation> {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let layouts = txn.table_layouts(ns, db, &TableName::from(tb), None).await.unwrap();
	txn.cancel().await.unwrap();
	layouts.head()
}

/// The stored data of `tb:id` without its `id`, in a form that tells an
/// integer from a float.
async fn stored_without_id(ds: &Datastore, tb: &str, id: i64) -> String {
	let (mut data, generation) = stored(ds, tb, id).await;
	if let Stored::Object(object) = &mut data {
		object.remove("id");
	}
	format!("{data:?} @ {generation}")
}

/// What a case's rows read as while the job has not run, once it has, and
/// what writing the read values afresh into a table with the same final
/// definitions stores: the three agree.
async fn replay_equals_rewrite(script: &Script, level: u8) {
	let label = format!("{} (level {level})", script.name);
	let ds = datastore_with(level, None).await;
	for statement in &script.statements {
		run(&ds, statement).await;
	}
	let ids = row_ids(&ds, "tb").await;
	let steps = steps_run(&ds, "tb").await;
	assert!(steps > 0, "{label}: the case runs a migration");

	// The job has not run: every read replays.
	let mut behind = 0;
	for id in &ids {
		behind += usize::from(stored(&ds, "tb", *id).await.1 < steps);
	}
	assert!(behind > 0, "{label}: some row is behind the newest step");
	let replayed = read_rows(&ds, &ids).await;

	drain(&ds).await;
	let left = entry(&ds, "tb").await;
	assert!(left.is_none(), "{label}: the job finished: {left:?}");
	for id in &ids {
		assert_eq!(stored(&ds, "tb", *id).await.1, steps, "{label}: tb:{id} is current");
	}
	let rewritten = read_rows(&ds, &ids).await;
	assert_eq!(replayed, rewritten, "{label}: reads before and after the job");

	// A table with the final definitions and no migration, written with the
	// values the rows read as.
	let Value::Object(definitions) = &run(&ds, "(INFO FOR TABLE tb).fields;").await[0] else {
		panic!("fields are an object");
	};
	run(
		&ds,
		if script.schemafull {
			"DEFINE TABLE fresh SCHEMAFULL;"
		} else {
			"DEFINE TABLE fresh SCHEMALESS;"
		},
	)
	.await;
	for definition in definitions.values() {
		let Value::String(definition) = definition else {
			panic!("{definition:?}")
		};
		let definition = definition
			.replacen("DEFINE FIELD ", "DEFINE FIELD OVERWRITE ", 1)
			.replace(" ON `tb` ", " ON fresh ")
			.replace(" ON tb ", " ON fresh ");
		assert!(definition.contains(" ON fresh "), "{label}: {definition}");
		run(&ds, &definition).await;
	}
	let Value::Array(rows) = &run(&ds, "SELECT * FROM tb ORDER BY id;").await[0] else {
		panic!("rows are an array");
	};
	for (row, id) in rows.iter().zip(&ids) {
		let Value::Object(object) = row else {
			panic!("{row:?}")
		};
		let mut content = object.clone();
		content.remove("id");
		run(
			&ds,
			&format!("CREATE fresh:{id} CONTENT {} RETURN NONE;", Value::Object(content).to_sql()),
		)
		.await;
	}
	for id in &ids {
		let rewritten = stored_without_id(&ds, "tb", *id).await;
		let fresh = stored_without_id(&ds, "fresh", *id).await;
		let strip = |s: &str| s.split(" @ ").next().unwrap().to_owned();
		assert_eq!(
			strip(&rewritten),
			strip(&fresh),
			"{label}: tb:{id}, rewritten and freshly written"
		);
	}
}

#[tokio::test]
async fn replay_equals_rewrite_at_level_4() {
	for script in scripts() {
		replay_equals_rewrite(&script, 4).await;
	}
}

#[tokio::test]
async fn replay_equals_rewrite_at_level_3() {
	for script in scripts() {
		replay_equals_rewrite(&script, 3).await;
	}
}

/// A stored row that is not an object (which no statement writes) passes
/// every step unchanged, is stamped current by the job, and does not stop it.
#[tokio::test]
async fn a_stored_row_that_is_not_an_object_is_kept_and_stamped_current() {
	let ds = datastore(4).await;
	doubling_table(&ds, 6).await;
	let others = [(2usize, Stored::from(7i64)), (4, Stored::from("text"))];
	for (index, data) in &others {
		let (key, _) = raw_rows(&ds, "tb").await.swap_remove(*index);
		let bytes = crate::catalog::encode_record(
			RecordFormat::Document {
				generation: None,
			},
			None,
			data,
			None,
		)
		.unwrap();
		overwrite(&ds, &key, bytes).await;
	}
	drain(&ds).await;
	assert!(entry(&ds, "tb").await.is_none(), "the job finished");
	for (index, data) in &others {
		let (found, generation) = stored(&ds, "tb", *index as i64).await;
		assert_eq!(format!("{found:?}"), format!("{data:?}"));
		assert_eq!(generation, 1);
	}
	for id in [0, 1, 3, 5] {
		assert_row(&ds, id, 2 * (id + 1), 1).await;
	}
}

// ---------------------------------------------------------------------------
// Every schema change, with and without rows, before and after the job.
// ---------------------------------------------------------------------------

/// One schema change and the table it is made to. The table is `tb`, built as
/// a SCHEMALESS table, given its rows and `fields` (in the order `rows_first`
/// says: a field defined over rows queues a verification of them, one defined
/// before them does not), made SCHEMAFULL unless `schemafull` is false, and
/// given `then`; `change` is the statement under test.
struct Case {
	/// The change's number in the list of schema changes, and what it is.
	name: &'static str,
	fields: &'static str,
	rows: &'static str,
	rows_first: bool,
	then: &'static str,
	change: &'static str,
	schemafull: bool,
	/// The rows as read before the change.
	read_before: &'static str,
	/// What the change leaves, at compatibility level 3 and at level 4.
	levels: [Level; 2],
}

/// What a change leaves at one compatibility level.
struct Level {
	/// The outcome and the table's state, as [`Observed::signature`] gives it,
	/// when the table holds no rows.
	empty: &'static str,
	/// The same when the table holds `Case::rows`.
	populated: &'static str,
	/// The rows as read after the change, and after the job.
	read_after: &'static str,
}

/// What a change did to a table, and what the table reads as.
struct Observed {
	/// `accepted` or `refused:` and the error; then the table's state before
	/// the change, after it (with the job's queue entry) and after the job, each
	/// as the `migrations` section of its `INFO` (the steps `name@position`, the
	/// `guaranteed` fields, the `verify` entries `field:failures:finished`) and
	/// the conformance of every field, `G` for a field whose claim holds for its
	/// current definition and `U` for one that has none.
	signature: String,
	/// The rows read before the change, after it and after the job.
	reads: [String; 3],
}

async fn state(ds: &Datastore) -> String {
	format!("{} | {}", section(ds, "tb").await, conformance(ds, "tb").await)
}

/// What the job's queue entry of `tb` asks for.
async fn queued(ds: &Datastore) -> String {
	match entry(ds, "tb").await {
		None => "none".to_owned(),
		Some(entry) => {
			let verify: Vec<String> = entry
				.verify
				.iter()
				.map(|field| format!("{}:{}:{}", field.name, field.failures, field.finished))
				.collect();
			format!("rewrite={} verify=[{}]", entry.rewrite, verify.join(","))
		}
	}
}

async fn read_rows_of_tb(ds: &Datastore) -> String {
	run(ds, "SELECT * FROM tb ORDER BY id;").await[0].to_sql()
}

async fn observe(case: &Case, level: u8, with_rows: bool) -> Observed {
	let ds = datastore(level).await;
	run(&ds, "DEFINE TABLE tb SCHEMALESS;").await;
	let rows = if with_rows {
		case.rows
	} else {
		""
	};
	let (first, second) = if case.rows_first {
		(rows, case.fields)
	} else {
		(case.fields, rows)
	};
	for sql in [first, second] {
		if !sql.is_empty() {
			run(&ds, sql).await;
		}
	}
	if case.schemafull {
		run(&ds, "ALTER TABLE tb SCHEMAFULL;").await;
	}
	if !case.then.is_empty() {
		run(&ds, case.then).await;
	}
	drain(&ds).await;
	let before = state(&ds).await;
	let read_before = read_rows_of_tb(&ds).await;
	let mut outcome = "accepted".to_owned();
	for response in ds.execute(case.change, &session(), None).await.unwrap() {
		if let Err(e) = response.result {
			outcome = format!("refused: {e}");
		}
	}
	let changed = format!("{} | {}", state(&ds).await, queued(&ds).await);
	let read_changed = read_rows_of_tb(&ds).await;
	drain(&ds).await;
	let finished = state(&ds).await;
	let read_finished = read_rows_of_tb(&ds).await;
	Observed {
		signature: format!("{outcome} | before: {before} | change: {changed} | job: {finished}"),
		reads: [read_before, read_changed, read_finished],
	}
}

const CASES: &[Case] = &[
	Case {
		name: r#"1 widen int to number"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb TYPE number;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [a:0:false] | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"2 T to option<T>"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb TYPE option<int>;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [a:0:false] | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"3 T to T | U"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb TYPE int | string;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [a:0:false] | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"4 add FLEXIBLE"#,
		fields: r#"DEFINE FIELD a ON tb TYPE object;"#,
		rows: r#"CREATE tb:1 SET a = {}; CREATE tb:2 SET a = {};"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb FLEXIBLE;"#,
		schemafull: true,
		read_before: r#"[{ a: {  }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: none | a:G"#,
				read_after: r#"[{ a: {  }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [a:0:false] | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: {  }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"5 int to float"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb TYPE float;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [] verify [a:2:true] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"refused: Cannot change field `a` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [a] verify [] | a:G | none | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"6 remove FLEXIBLE"#,
		fields: r#"DEFINE FIELD a ON tb TYPE object;"#,
		rows: r#"CREATE tb:1 SET a = {}; CREATE tb:2 SET a = {};"#,
		rows_first: true,
		then: r#"ALTER FIELD a ON tb FLEXIBLE;"#,
		change: r#"ALTER FIELD a ON tb DROP FLEXIBLE;"#,
		schemafull: true,
		read_before: r#"[{ a: {  }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: none | a:G"#,
				read_after: r#"[{ a: {  }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"refused: Cannot change field `a` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [a] verify [] | a:G | none | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: {  }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"7 narrow a literal shape"#,
		fields: r#"DEFINE FIELD a ON tb TYPE { x: int | string };"#,
		rows: r#"CREATE tb:1 SET a = { x: 1 }; CREATE tb:2 SET a = { x: 'q' };"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb TYPE { x: int };"#,
		schemafull: true,
		read_before: r#"[{ a: { x: 1 }, id: tb:1 }, { a: { x: 'q' }, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [] verify [a:1:true] | a:U"#,
				read_after: r#"[{ a: { x: 1 }, id: tb:1 }, { a: { x: 'q' }, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"refused: Cannot change field `a` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [a] verify [] | a:G | none | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: { x: 1 }, id: tb:1 }, { a: { x: 'q' }, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"8 new field accepting NONE"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD c ON tb TYPE option<int>;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U c:U | none | job: none | a:U c:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U c:U | rewrite=false verify=[c:0:false] | job: none | a:U c:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U c:U | none | job: steps [] guaranteed [] verify [] | a:U c:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [c:0:false] | a:U c:U | rewrite=false verify=[c:0:false] | job: steps [] guaranteed [c] verify [] | a:U c:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"9 new field refusing NONE"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD c ON tb TYPE int;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U c:U | none | job: none | a:U c:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U c:U | rewrite=false verify=[c:0:false] | job: steps [] guaranteed [] verify [c:2:true] | a:U c:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U c:U | none | job: steps [] guaranteed [] verify [] | a:U c:U"#,
				populated: r#"refused: Cannot change field `c` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the field's TYPE does not accept NONE: add MIGRATE EXISTING ROWS to say what existing rows hold | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [a] verify [] | a:G | none | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"10 new field with MIGRATE"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD c ON tb TYPE int MIGRATE EXISTING ROWS AS add_c USING 7;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: steps [add_c@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a:U c:U | rewrite=true verify=[] | job: steps [add_c@1] guaranteed [] verify [] | a:U c:U"#,
				populated: r#"accepted | before: none | a:G | change: steps [add_c@1] guaranteed [] verify [c:0:false] progress { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 } | a:U c:U | rewrite=true verify=[c:0:false] | job: steps [add_c@1] guaranteed [c] verify [] | a:U c:G"#,
				read_after: r#"[{ a: 1, c: 7, id: tb:1 }, { a: 2, c: 7, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [add_c@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a:U c:U | rewrite=true verify=[] | job: steps [add_c@1] guaranteed [] verify [] | a:U c:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [add_c@1] guaranteed [] verify [c:0:false] progress { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 } | a:U c:U | rewrite=true verify=[c:0:false] | job: steps [add_c@1] guaranteed [c] verify [] | a:U c:G"#,
				read_after: r#"[{ a: 1, c: 7, id: tb:1 }, { a: 2, c: 7, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"11 COMPUTED made stored"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int; DEFINE FIELD c ON tb COMPUTED a * 2;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD OVERWRITE c ON tb TYPE option<int>;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, c: 2, id: tb:1 }, { a: 2, c: 4, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U c:U | change: none | a:U c:U | none | job: none | a:U c:U"#,
				populated: r#"accepted | before: none | a:G c:U | change: none | a:U c:U | rewrite=false verify=[c:0:false] | job: none | a:U c:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U c:U | change: steps [] guaranteed [] verify [] | a:U c:U | none | job: steps [] guaranteed [] verify [] | a:U c:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G c:U | change: steps [remove c after 2@1] guaranteed [] verify [c:0:false] progress { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 } | a:U c:U | rewrite=true verify=[c:0:false] | job: steps [remove c after 2@1] guaranteed [c] verify [] | a:U c:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"12 stored made COMPUTED"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int; DEFINE FIELD c ON tb TYPE option<int>;"#,
		rows: r#"CREATE tb:1 SET a = 1, c = 5; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD OVERWRITE c ON tb COMPUTED a * 2;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, c: 5, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U c:U | change: none | a:U c:U | none | job: none | a:U c:U"#,
				populated: r#"accepted | before: none | a:G c:G | change: none | a:U c:U | none | job: none | a:U c:U"#,
				read_after: r#"[{ a: 1, c: 2, id: tb:1 }, { a: 2, c: 4, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U c:U | change: steps [] guaranteed [] verify [] | a:U c:U | none | job: steps [] guaranteed [] verify [] | a:U c:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a,c] verify [] | a:G c:G | change: steps [remove c after 2@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a:U c:U | rewrite=true verify=[] | job: steps [remove c after 2@1] guaranteed [] verify [] | a:U c:U"#,
				read_after: r#"[{ a: 1, c: 2, id: tb:1 }, { a: 2, c: 4, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"13 MIGRATE with no TYPE change"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb MIGRATE EXISTING ROWS AS dbl USING $value * 2 FALLBACK 0;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: steps [dbl@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a:U | rewrite=true verify=[] | job: steps [dbl@1] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: none | a:G | change: steps [dbl@1] guaranteed [a] verify [a:0:false] progress { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 } | a:G | rewrite=true verify=[a:0:false] | job: steps [dbl@1] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 2, id: tb:1 }, { a: 4, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [dbl@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a:U | rewrite=true verify=[] | job: steps [dbl@1] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [dbl@1] guaranteed [a] verify [a:0:false] progress { fallbacks: 0, lap: 2, rewritten: 0, scanned: 0 } | a:G | rewrite=true verify=[a:0:false] | job: steps [dbl@1] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 2, id: tb:1 }, { a: 4, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"14 definition restated unchanged"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD OVERWRITE a ON tb TYPE int;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:G | none | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [a] verify [] | a:G | none | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"15 DEFINE FIELD IF NOT EXISTS on an existing field"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"DEFINE FIELD IF NOT EXISTS a ON tb TYPE string;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:G | none | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [a] verify [] | a:G | none | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"16 REMOVE FIELD"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int; DEFINE FIELD b ON tb TYPE option<string>;"#,
		rows: r#"CREATE tb:1 SET a = 1, b = 'x'; CREATE tb:2 SET a = 2, b = 'y';"#,
		rows_first: true,
		then: r#""#,
		change: r#"REMOVE FIELD b ON tb;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, b: 'x', id: tb:1 }, { a: 2, b: 'y', id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U b:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G b:G | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: 1, b: 'x', id: tb:1 }, { a: 2, b: 'y', id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U b:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a,b] verify [] | a:G b:G | change: steps [remove b after 2@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a:U | rewrite=true verify=[] | job: steps [remove b after 2@1] guaranteed [] verify [] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"18 VALUE changed"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb VALUE $value + 1;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"19 DEFAULT ALWAYS changed"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int; DEFINE FIELD d ON tb TYPE option<int> DEFAULT 1;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD d ON tb DEFAULT ALWAYS 9;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U d:U | change: none | a:U d:U | none | job: none | a:U d:U"#,
				populated: r#"accepted | before: none | a:G d:G | change: none | a:U d:U | none | job: none | a:U d:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U d:U | change: steps [] guaranteed [] verify [] | a:U d:U | none | job: steps [] guaranteed [] verify [] | a:U d:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a,d] verify [] | a:G d:G | change: steps [] guaranteed [] verify [] | a:U d:U | none | job: steps [] guaranteed [] verify [] | a:U d:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"20 READONLY toggled"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb READONLY;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"21 PERMISSIONS-only ALTER"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb PERMISSIONS FOR select NONE;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"22 COMMENT-only ALTER"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb COMMENT 'hi';"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"23 REFERENCE added"#,
		fields: r#"DEFINE FIELD a ON tb TYPE option<record<other>>;"#,
		rows: r#"CREATE other:1; CREATE tb:1 SET a = other:1; CREATE tb:2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a ON tb REFERENCE;"#,
		schemafull: true,
		read_before: r#"[{ a: other:1, id: tb:1 }, { id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: other:1, id: tb:1 }, { id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				read_after: r#"[{ a: other:1, id: tb:1 }, { id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"25a SCHEMALESS to SCHEMAFULL, rows fitting every field"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: false,
		then: r#""#,
		change: r#"ALTER TABLE tb SCHEMAFULL;"#,
		schemafull: false,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: none | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: none | a:U | change: steps [] guaranteed [] verify [a:0:false] undeclared 0:false | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [a] verify [] | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"25b SCHEMALESS to SCHEMAFULL, a row misfitting a field"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 'x';"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER TABLE tb SCHEMAFULL;"#,
		schemafull: false,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 'x', id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [] verify [a:1:true] | a:U | change: steps [] guaranteed [] verify [a:1:true] | a:U | rewrite=false verify=[a:1:true] | job: steps [] guaranteed [] verify [a:1:true] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 'x', id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: none | a:U | change: steps [] guaranteed [] verify [] | a:U | none | job: steps [] guaranteed [] verify [] | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [] verify [a:1:true] | a:U | change: steps [] guaranteed [] verify [a:0:false] undeclared 0:false | a:U | rewrite=false verify=[a:0:false] | job: steps [] guaranteed [] verify [a:1:true] | a:U"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 'x', id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"25c SCHEMAFULL to SCHEMALESS"#,
		fields: r#"DEFINE FIELD a ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER TABLE tb SCHEMALESS;"#,
		schemafull: true,
		read_before: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: none | a:G | change: none | a:G | none | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a:U | change: none | a:U | none | job: none | a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a:G | change: none | a:G | none | job: none | a:G"#,
				read_after: r#"[{ a: 1, id: tb:1 }, { a: 2, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"34a nested a.b int to float"#,
		fields: r#"DEFINE FIELD a ON tb TYPE object; DEFINE FIELD a.b ON tb TYPE int;"#,
		rows: r#"CREATE tb:1 SET a = { b: 1 }; CREATE tb:2 SET a = { b: 2 };"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a.b ON tb TYPE float;"#,
		schemafull: true,
		read_before: r#"[{ a: { b: 1 }, id: tb:1 }, { a: { b: 2 }, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a.b:U a:U | change: none | a.b:U a:U | none | job: none | a.b:U a:U"#,
				populated: r#"accepted | before: none | a.b:U a:G | change: none | a.b:U a:U | none | job: none | a.b:U a:U"#,
				read_after: r#"[{ a: { b: 1 }, id: tb:1 }, { a: { b: 2 }, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a.b:U a:U | change: steps [] guaranteed [] verify [] | a.b:U a:U | none | job: steps [] guaranteed [] verify [] | a.b:U a:U"#,
				populated: r#"refused: Cannot change field `a.b` of table `tb`: the table is SCHEMAFULL in a database at compatibility level 4 and has rows, and the new definition does not accept every value the old one did: add MIGRATE EXISTING ROWS | before: steps [] guaranteed [a] verify [] | a.b:U a:G | change: steps [] guaranteed [a] verify [] | a.b:U a:G | none | job: steps [] guaranteed [a] verify [] | a.b:U a:G"#,
				read_after: r#"[{ a: { b: 1 }, id: tb:1 }, { a: { b: 2 }, id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"34b array elements a.* int to float"#,
		fields: r#"DEFINE FIELD a ON tb TYPE array<int>;"#,
		rows: r#"CREATE tb:1 SET a = [1, 2]; CREATE tb:2 SET a = [3];"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a.* ON tb TYPE float;"#,
		schemafull: true,
		read_before: r#"[{ a: [1, 2], id: tb:1 }, { a: [3], id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"refused: Cannot set field `a.*` with type `float` as it mismatched with field `a` with type `array<int>` | before: none | a.*:U a:U | change: none | a.*:U a:U | none | job: none | a.*:U a:U"#,
				populated: r#"refused: Cannot set field `a.*` with type `float` as it mismatched with field `a` with type `array<int>` | before: none | a.*:U a:U | change: none | a.*:U a:U | none | job: none | a.*:U a:U"#,
				read_after: r#"[{ a: [1, 2], id: tb:1 }, { a: [3], id: tb:2 }]"#,
			},
			Level {
				empty: r#"refused: Cannot set field `a.*` with type `float` as it mismatched with field `a` with type `array<int>` | before: steps [] guaranteed [] verify [] | a.*:U a:U | change: steps [] guaranteed [] verify [] | a.*:U a:U | none | job: steps [] guaranteed [] verify [] | a.*:U a:U"#,
				populated: r#"refused: Cannot set field `a.*` with type `float` as it mismatched with field `a` with type `array<int>` | before: steps [] guaranteed [] verify [] | a.*:U a:U | change: steps [] guaranteed [] verify [] | a.*:U a:U | none | job: steps [] guaranteed [] verify [] | a.*:U a:U"#,
				read_after: r#"[{ a: [1, 2], id: tb:1 }, { a: [3], id: tb:2 }]"#,
			},
		],
	},
	Case {
		name: r#"34c nested a.b with MIGRATE"#,
		fields: r#"DEFINE FIELD a ON tb TYPE object; DEFINE FIELD a.b ON tb TYPE option<int>;"#,
		rows: r#"CREATE tb:1 SET a = { b: 1 }; CREATE tb:2 SET a = {};"#,
		rows_first: true,
		then: r#""#,
		change: r#"ALTER FIELD a.b ON tb TYPE float MIGRATE EXISTING ROWS AS m USING <float>$value ?? 0.5 FALLBACK 0.5;"#,
		schemafull: true,
		read_before: r#"[{ a: { b: 1 }, id: tb:1 }, { a: {  }, id: tb:2 }]"#,
		levels: [
			Level {
				empty: r#"accepted | before: none | a.b:U a:U | change: steps [m@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a.b:U a:U | rewrite=true verify=[] | job: steps [m@1] guaranteed [] verify [] | a.b:U a:U"#,
				populated: r#"accepted | before: none | a.b:U a:G | change: steps [m@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a.b:U a:U | rewrite=true verify=[] | job: steps [m@1] guaranteed [] verify [] | a.b:U a:U"#,
				read_after: r#"[{ a: { b: 1f }, id: tb:1 }, { a: { b: 0.5f }, id: tb:2 }]"#,
			},
			Level {
				empty: r#"accepted | before: steps [] guaranteed [] verify [] | a.b:U a:U | change: steps [m@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a.b:U a:U | rewrite=true verify=[] | job: steps [m@1] guaranteed [] verify [] | a.b:U a:U"#,
				populated: r#"accepted | before: steps [] guaranteed [a] verify [] | a.b:U a:G | change: steps [m@1] guaranteed [] verify [] progress { fallbacks: 0, lap: 1, rewritten: 0, scanned: 0 } | a.b:U a:U | rewrite=true verify=[] | job: steps [m@1] guaranteed [] verify [] | a.b:U a:U"#,
				read_after: r#"[{ a: { b: 1f }, id: tb:1 }, { a: { b: 0.5f }, id: tb:2 }]"#,
			},
		],
	},
];

/// Every change of the list that a table's definitions alone decide, at both
/// compatibility levels, on a table with no rows and on one with rows. Each
/// cell states whether the change is accepted or refused (with the error),
/// the steps it mints, the verification it queues and what that finds, and
/// which fields hold a claim before the change, after it and after the job;
/// and the rows read the same before and after the job.
///
/// A change to any field definition of a table forfeits every claim the table
/// holds. The job states a claim again only for a field it verified: one the
/// change queued, which is a new field, one whose type or `FLEXIBLE` changed,
/// or the field of a migration, and only on a table that had rows.
#[tokio::test]
async fn each_schema_change_leaves_the_state_the_rules_give_before_and_after_the_job() {
	let mut wrong = Vec::new();
	for case in CASES {
		for (index, level) in [3u8, 4].into_iter().enumerate() {
			let expect = &case.levels[index];
			for with_rows in [false, true] {
				let cell = format!(
					"{} (level {level}, {})",
					case.name,
					if with_rows {
						"rows"
					} else {
						"no rows"
					}
				);
				let observed = observe(case, level, with_rows).await;
				let want = if with_rows {
					expect.populated
				} else {
					expect.empty
				};
				if observed.signature != want {
					wrong.push(format!("{cell}\n  want {want}\n  got  {}", observed.signature));
				}
				let (before, after) = if with_rows {
					(case.read_before, expect.read_after)
				} else {
					("[]", "[]")
				};
				let [read_before, read_changed, read_finished] = &observed.reads;
				if read_before != before {
					wrong.push(format!(
						"{cell}: rows before the change\n  want {before}\n  got  {read_before}"
					));
				}
				if read_changed != after {
					wrong.push(format!(
						"{cell}: rows after the change\n  want {after}\n  got  {read_changed}"
					));
				}
				if read_finished != read_changed {
					wrong.push(format!(
						"{cell}: the job changed what the rows read as\n  before the job {read_changed}\n  after it       {read_finished}"
					));
				}
			}
		}
	}
	assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}

// ---------------------------------------------------------------------------
// Changes whose rule the matrix cannot hold.
// ---------------------------------------------------------------------------

/// Change 17: an ASSERT is not a shape the planner trusts, so adding one
/// mints nothing and the stored rows read as they did; it queues a
/// verification, which counts the row that violates it, and a later write is
/// judged by it.
#[tokio::test]
async fn an_assert_change_mints_nothing_changes_no_read_and_binds_later_writes() {
	for level in [3u8, 4] {
		let ds = datastore(level).await;
		run(
			&ds,
			"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD a ON tb TYPE int;
			 CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;",
		)
		.await;
		let before = read_rows_of_tb(&ds).await;
		let minted = head(&ds, "tb").await;
		run(&ds, "ALTER FIELD a ON tb ASSERT $value > 1;").await;
		assert_eq!(head(&ds, "tb").await, minted, "level {level}");
		assert_eq!(read_rows_of_tb(&ds).await, before, "level {level}");
		drain(&ds).await;
		assert_eq!(read_rows_of_tb(&ds).await, before, "level {level}");
		let verified = entry(&ds, "tb").await.map(|state| state.verify);
		assert!(
			verified.as_ref().is_some_and(|verify| {
				verify.len() == 1
					&& verify[0].name == "a"
					&& verify[0].failures == 1
					&& verify[0].finished
			}),
			"level {level}: the verification counts tb:1: {verified:?}"
		);
		let refusal = fails(&ds, "UPDATE tb:1 SET a = 1;").await;
		assert!(refusal.contains("$value > 1"), "level {level}: {refusal}");
		run(&ds, "UPDATE tb:2 SET a = 5;").await;
		assert_eq!(stored_int(&ds, "tb", 2, "a").await.0, 5, "level {level}");
	}
}

/// Change 17: on an enforced table an added ASSERT is verified, and the rows
/// that violate it are counted in `INFO`.
#[tokio::test]
async fn an_assert_added_to_an_enforced_table_with_rows_counts_its_violations() {
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD a ON tb TYPE int;
		 CREATE tb:1 SET a = 1; CREATE tb:2 SET a = 2;",
	)
	.await;
	run(&ds, "ALTER FIELD a ON tb ASSERT $value > 1;").await;
	drain(&ds).await;
	let section = section(&ds, "tb").await;
	assert!(section.contains(":1:true"), "one row violates the ASSERT: {section}");
}

/// Change 25: making a table SCHEMAFULL in a level 4 database verifies the
/// absence of undeclared fields, and reports the rows that hold one.
#[tokio::test]
async fn making_a_table_schemafull_reports_rows_holding_undeclared_fields() {
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMALESS; DEFINE FIELD a ON tb TYPE int;
		 CREATE tb:1 SET a = 1, z = 1; CREATE tb:2 SET a = 2;",
	)
	.await;
	run(&ds, "ALTER TABLE tb SCHEMAFULL;").await;
	drain(&ds).await;
	let section = section(&ds, "tb").await;
	assert!(
		section.ends_with(" undeclared 1:true"),
		"tb:1 holds `z`, which no field declares: {section}"
	);
}

/// Change 6: a row that holds a key beneath an object field is refused when
/// written afresh once the field is no longer FLEXIBLE, so the verification
/// that follows the change reports it and leaves the field Unchecked.
#[tokio::test]
async fn removing_flexible_reports_rows_holding_undeclared_keys_beneath_the_field() {
	let ds = datastore(3).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD a ON tb TYPE object FLEXIBLE;
		 CREATE tb:1 SET a = { x: 1 }; CREATE tb:2 SET a = {};",
	)
	.await;
	run(&ds, "ALTER FIELD a ON tb DROP FLEXIBLE;").await;
	let refusal = fails(&ds, "CREATE tb:3 SET a = { x: 1 };").await;
	assert!(
		refusal.contains("Found field 'a.x'"),
		"a fresh write of the row is refused: {refusal}"
	);
	drain(&ds).await;
	assert_eq!(conformance(&ds, "tb").await, "a:U");
	let section = section(&ds, "tb").await;
	assert!(section.contains(":1:true"), "tb:1 holds `x`, which no field declares: {section}");
}

/// Changes 12 and 16: on an enforced table the removal step hides the values
/// of a removed field, or of a stored field made COMPUTED, from every reader at
/// once, and the job then deletes them from every row; a row written after the
/// step never held them.
#[tokio::test]
async fn the_job_deletes_the_values_a_removal_step_hid() {
	let ds = datastore(4).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL; DEFINE FIELD a ON tb TYPE int;
		 DEFINE FIELD b ON tb TYPE option<string>; DEFINE FIELD c ON tb TYPE option<int>;
		 CREATE tb:1 SET a = 1, b = 'x', c = 5; CREATE tb:2 SET a = 2, b = 'y';",
	)
	.await;
	let holds = |data: Stored, field: &str| {
		let Stored::Object(object) = data else {
			panic!("{data:?}");
		};
		object.get(field).is_some()
	};
	run(&ds, "REMOVE FIELD b ON tb; DEFINE FIELD OVERWRITE c ON tb COMPUTED a * 2;").await;
	assert_eq!(steps_run(&ds, "tb").await, 2);
	for id in [1, 2] {
		let (data, generation) = stored(&ds, "tb", id).await;
		assert_eq!(generation, 0);
		assert!(holds(data, "b"), "tb:{id} still stores b before the job");
	}
	assert_eq!(
		run(&ds, "SELECT * FROM tb ORDER BY id;").await[0].to_sql(),
		"[{ a: 1, c: 2, id: tb:1 }, { a: 2, c: 4, id: tb:2 }]",
		"no reader sees b, and c is computed"
	);
	run(&ds, "CREATE tb:3 SET a = 3;").await;
	drain(&ds).await;
	for id in [1, 2, 3] {
		let (data, generation) = stored(&ds, "tb", id).await;
		assert_eq!(generation, 2, "tb:{id} is current");
		assert!(!holds(data.clone(), "b"), "tb:{id} no longer stores b");
		assert!(!holds(data, "c"), "tb:{id} no longer stores c");
	}
	assert_eq!(
		run(&ds, "SELECT * FROM tb ORDER BY id;").await[0].to_sql(),
		"[{ a: 1, c: 2, id: tb:1 }, { a: 2, c: 4, id: tb:2 }, { a: 3, c: 6, id: tb:3 }]"
	);
}
