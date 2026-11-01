//! The table migration job, driven by hand on a datastore without
//! maintenance tasks.
//!
//! The property under test is that a row reads the same before and after
//! the job rewrites it: every read applies the steps a row has not passed,
//! so the job changes what a row costs to read, never what it reads as.

#![allow(clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Duration;

use surrealdb_kvs::TransactionType::{Read, Write};
use surrealdb_types::Value;
use tokio_util::sync::CancellationToken;

use crate::catalog::Generation;
use crate::catalog::providers::{CatalogProvider, DatabaseProvider, TableProvider};
use crate::dbs::Session;
use crate::kvs::Datastore;
use crate::val::{RecordIdKey, TableName};

/// A fresh in-memory datastore with `test`/`test` defined.
async fn ds() -> Arc<Datastore> {
	let ds =
		Datastore::builder().without_maintenance_tasks().build_with_path("memory").await.unwrap();
	{
		let txn = ds.transaction(Write).await.unwrap();
		txn.ensure_ns_db(None, "test", "test").await.unwrap();
		txn.commit().await.unwrap();
	}
	ds
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

/// Run the job until every queue entry has nothing left to do.
async fn drain(ds: &Arc<Datastore>) {
	Datastore::table_migration(Arc::clone(ds), Duration::from_secs(60), CancellationToken::new())
		.await
		.unwrap();
}

/// The generation the stored record `tb:id` is stamped with.
async fn stamp(ds: &Datastore, tb: &str, id: i64) -> Option<Generation> {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let (_, generation) = txn
		.get_stamped_record(
			db.namespace_id,
			db.database_id,
			&TableName::from(tb),
			&RecordIdKey::Number(id),
			None,
		)
		.await
		.unwrap();
	txn.cancel().await.unwrap();
	generation
}

/// The newest generation of `tb`'s ring.
async fn head(ds: &Datastore, tb: &str) -> Option<Generation> {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let layouts = txn
		.table_layouts(db.namespace_id, db.database_id, &TableName::from(tb), None)
		.await
		.unwrap();
	txn.cancel().await.unwrap();
	layouts.head()
}

/// Whether `tb` holds a claim that field `field` conforms, against the field's current
/// definition.
async fn claimed(ds: &Datastore, tb: &str, field: &str) -> bool {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	let table = TableName::from(tb);
	let definition = txn
		.get_tb_field(db.namespace_id, db.database_id, &table, field, None)
		.await
		.unwrap()
		.unwrap();
	let conformance =
		txn.table_conformance(db.namespace_id, db.database_id, &table, None).await.unwrap();
	txn.cancel().await.unwrap();
	conformance.is_some_and(|conformance| {
		conformance.claim(field).is_some_and(|claim| claim.holds_for(&definition.to_stored()))
	})
}

/// The queries whose answers must not change when the job rewrites rows.
const READS: &str = "
	SELECT VALUE field FROM tb ORDER BY id;
	SELECT VALUE id FROM tb WHERE field > 300 ORDER BY id;
	SELECT math::sum(field) AS s, count() AS c FROM tb GROUP ALL;
	tb:1.field;
	tb:599.field;
";

#[tokio::test]
async fn the_job_rewrites_rows_without_changing_what_they_read_as() {
	let ds = ds().await;
	// More rows than one page holds, written under three generations.
	let mut setup = String::from(
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD field ON tb TYPE string;",
	);
	for i in 0..600 {
		let value = if i % 7 == 0 {
			"'x'".to_owned()
		} else {
			format!("'{i}'")
		};
		setup.push_str(&format!("CREATE tb:{i} SET field = {value} RETURN NONE;"));
	}
	setup.push_str(
		"ALTER FIELD field ON tb TYPE int
		     MIGRATE EXISTING ROWS AS field_to_int USING <int>$value FALLBACK 0;
		 UPSERT tb:600 SET field = 234 RETURN NONE;
		 ALTER FIELD field ON tb TYPE float
		     MIGRATE EXISTING ROWS AS double_field USING $value * 2 FALLBACK 7;",
	);
	run(&ds, &setup).await;
	let newest = head(&ds, "tb").await.expect("the migrations minted generations");
	let middle = stamp(&ds, "tb", 600).await.expect("a row written between the steps is stamped");
	assert_ne!(middle, newest, "a row written between the steps is behind the newest one");
	assert_ne!(stamp(&ds, "tb", 1).await, Some(middle), "a row written before either step");

	let before = run(&ds, READS).await;
	drain(&ds).await;
	let after = run(&ds, READS).await;
	assert_eq!(before, after);
	assert_eq!(after[3], Value::Number(surrealdb_types::Number::Float(2.0)));

	// Every row now carries the newest generation.
	assert_eq!(stamp(&ds, "tb", 1).await, Some(newest));
	assert_eq!(stamp(&ds, "tb", 7).await, Some(newest));
	assert_eq!(stamp(&ds, "tb", 600).await, Some(newest));
	let info = run(&ds, "(INFO FOR TABLE tb).migrations;").await;
	let Value::Object(section) = &info[0] else {
		panic!("no migrations section: {info:?}");
	};
	assert_eq!(
		section.get("generation"),
		Some(&Value::Number(surrealdb_types::Number::Int(i64::from(newest.get()))))
	);

	// Draining again finds nothing to do and changes nothing.
	drain(&ds).await;
	assert_eq!(run(&ds, READS).await, before);
}

#[tokio::test]
async fn a_verification_promotes_only_a_field_whose_rows_conform() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE ok SCHEMALESS;
		 CREATE ok:1 SET a = 1;
		 CREATE ok:2 SET a = 2;
		 DEFINE FIELD a ON ok TYPE int;
		 DEFINE TABLE bad SCHEMALESS;
		 CREATE bad:1 SET a = 1;
		 CREATE bad:2 SET a = 'x';
		 DEFINE FIELD a ON bad TYPE int;",
	)
	.await;
	drain(&ds).await;
	assert!(claimed(&ds, "ok", "a").await);
	assert!(!claimed(&ds, "bad", "a").await);
	let answers =
		run(&ds, "(INFO FOR TABLE ok).migrations; (INFO FOR TABLE bad).migrations;").await;
	// A field whose rows all conform is promoted, which is nothing to show
	// for a table that has run no migration.
	assert_eq!(answers[0], Value::None);
	// The failing verification is shown with its count.
	let Value::Object(section) = &answers[1] else {
		panic!("no migrations section for a failing verification: {answers:?}");
	};
	let verification = format!("{:?}", section.get("verification"));
	assert!(verification.contains("failures"), "{verification}");
}

/// The verification entries `INFO FOR TABLE` reports for `tb`, as `field:failures:finished`.
async fn verifications(ds: &Datastore, tb: &str) -> Vec<String> {
	let answers = run(ds, &format!("(INFO FOR TABLE {tb}).migrations.verification;")).await;
	let Value::Array(entries) = &answers[0] else {
		panic!("no verification entries for {tb}: {answers:?}");
	};
	entries
		.iter()
		.map(|entry| {
			let Value::Object(entry) = entry else {
				panic!("a verification entry that is not an object: {entry:?}");
			};
			let member = |key: &str| entry.get(key).map(|v| format!("{v:?}")).unwrap_or_default();
			format!("{}:{}:{}", member("field"), member("failures"), member("finished"))
		})
		.collect()
}

/// A finished verification is not judged by a later lap of its table, so the lap a verification
/// of another field starts leaves the count the finished pass reported.
#[tokio::test]
async fn a_finished_verification_keeps_its_count_through_a_later_lap() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE bad SCHEMALESS;
		 CREATE bad:1 SET a = 1, b = 1;
		 CREATE bad:2 SET a = 'x', b = 2;
		 CREATE bad:3 SET a = 'y', b = 3;
		 DEFINE FIELD a ON bad TYPE int;",
	)
	.await;
	drain(&ds).await;
	let first = verifications(&ds, "bad").await;
	assert_eq!(first.len(), 1, "{first:?}");
	assert!(first[0].contains("Int(2)") && first[0].ends_with("Bool(true)"), "{first:?}");
	run(&ds, "DEFINE FIELD b ON bad TYPE int;").await;
	drain(&ds).await;
	assert!(claimed(&ds, "bad", "b").await);
	assert_eq!(verifications(&ds, "bad").await, first);
}

/// A read that brings a row through a step leaves no transaction open: the job's verification
/// waits for every transaction older than the change, so one a read held would keep every row
/// behind the step for as long as it is held.
#[tokio::test]
async fn a_read_that_replays_a_row_leaves_no_transaction_open() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE string;
		 CREATE tb:1 SET f = '1';
		 ALTER FIELD f ON tb TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	assert_eq!(run(&ds, "SELECT VALUE f FROM tb;").await, run(&ds, "RETURN [1];").await);
	Datastore::table_migration(Arc::clone(&ds), Duration::from_secs(2), CancellationToken::new())
		.await
		.unwrap();
	assert_eq!(stamp(&ds, "tb", 1).await, head(&ds, "tb").await);
}

#[tokio::test]
async fn restating_a_migration_does_not_run_it_again() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE int;
		 CREATE tb:1 SET f = 1;
		 ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS double USING $value * 2 FALLBACK 0;",
	)
	.await;
	let minted = head(&ds, "tb").await;
	run(&ds, "ALTER FIELD f ON tb MIGRATE EXISTING ROWS AS double USING $value * 2 FALLBACK 0;")
		.await;
	assert_eq!(head(&ds, "tb").await, minted);
	drain(&ds).await;
	assert_eq!(run(&ds, "SELECT VALUE f FROM tb;").await[0], run(&ds, "RETURN [2];").await[0]);
}

/// Whether every transaction opened through `ds` so far has ended within `limit`.
async fn ended(ds: &Datastore, limit: Duration) -> bool {
	let live = ds.commit_triggers().live_transactions();
	let deadline = web_time::Instant::now() + limit;
	let bucket = loop {
		if let Some(bucket) = live.advance() {
			break bucket;
		}
		if web_time::Instant::now() >= deadline {
			return false;
		}
		tokio::time::sleep(Duration::from_millis(5)).await;
	};
	while !live.drained(bucket) {
		if web_time::Instant::now() >= deadline {
			return false;
		}
		tokio::time::sleep(Duration::from_millis(5)).await;
	}
	true
}

/// Building an index over existing rows ends every transaction it opens, so
/// the job's wait for older transactions settles.
#[tokio::test]
async fn defining_an_index_over_rows_leaves_no_transaction_open() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE tb; CREATE tb:1 SET n = 1; DEFINE INDEX ix ON tb FIELDS n;").await;
	assert!(ended(&ds, Duration::from_secs(3)).await);
}

/// The job rewrites the rows of a table whose index was built over existing
/// rows: the build leaves no transaction open for the job to wait on.
#[tokio::test]
async fn the_job_rewrites_rows_of_a_table_with_an_index_built_over_them() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE string;
		 DEFINE FIELD g ON tb TYPE int;
		 CREATE tb:1 SET f = '1', g = 1;
		 DEFINE INDEX ix ON tb FIELDS g;
		 ALTER FIELD f ON tb TYPE int
		     MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	Datastore::table_migration(Arc::clone(&ds), Duration::from_secs(2), CancellationToken::new())
		.await
		.unwrap();
	assert_eq!(stamp(&ds, "tb", 1).await, head(&ds, "tb").await);
}

/// The result of each statement of `sql`, as an error's text where it failed.
async fn attempt(ds: &Datastore, sql: &str) -> Vec<Result<Value, String>> {
	ds.execute(sql, &session(), None)
		.await
		.unwrap()
		.into_iter()
		.map(|response| response.result.map_err(|e| e.to_string()))
		.collect()
}

/// The ids of the `test` namespace and database.
async fn ids(ds: &Datastore) -> (crate::catalog::NamespaceId, crate::catalog::DatabaseId) {
	let txn = ds.transaction(Read).await.unwrap();
	let db = txn.get_db_by_name("test", "test", None).await.unwrap().unwrap();
	txn.cancel().await.unwrap();
	(db.namespace_id, db.database_id)
}

/// A key no definition declares beneath an object of a `SCHEMAFULL` table is one a write of the
/// row would refuse, so a pass that finds one makes no claim, however the table came to be
/// `SCHEMAFULL`. A claim made while the table was `SCHEMALESS` is withdrawn by the change.
#[tokio::test]
async fn a_verification_judges_keys_no_definition_declares_beneath_an_object() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE open SCHEMALESS;
		 CREATE open:1 SET o = { x: 1 };
		 DEFINE FIELD o ON open TYPE object;
		 DEFINE TABLE kept SCHEMALESS;
		 CREATE kept:1 SET o = { k: 1 };
		 DEFINE FIELD o ON kept TYPE object;
		 DEFINE FIELD o.k ON kept TYPE int;",
	)
	.await;
	drain(&ds).await;
	// Beneath a SCHEMALESS table's object any key may sit.
	assert!(claimed(&ds, "open", "o").await);
	run(&ds, "ALTER TABLE open SCHEMAFULL; ALTER TABLE kept SCHEMAFULL;").await;
	assert!(!claimed(&ds, "open", "o").await, "the change withdraws a claim made before it");
	assert_eq!(
		attempt(&ds, "UPDATE open:1 SET o.y = 2 RETURN NONE;").await[0],
		Err("Found field 'o.x', but no such field exists for table 'open'".to_owned()),
		"a write of the row refuses the key",
	);
	drain(&ds).await;
	assert!(!claimed(&ds, "open", "o").await);
	let report = verifications(&ds, "open").await;
	assert!(report.len() == 1 && report[0].contains("Int(1)"), "{report:?}");
	// Every key of `kept` is declared.
	assert!(claimed(&ds, "kept", "o").await);
}

/// A set orders numbers of every variant as one line, so a set of ints equals the set of the
/// same floats; a pass judges the variant of every element, and counts the row whose set holds
/// ints against `set<float>`, as it counts an array of ints against `array<float>`. A row whose
/// set holds floats conforms, which leaves nothing to report.
#[tokio::test]
async fn a_verification_judges_the_variant_of_every_element_of_a_set() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE ints SCHEMALESS;
		 CREATE ints:1 SET v = <set>[1, 2];
		 DEFINE FIELD v ON ints TYPE set<float>;
		 DEFINE TABLE nested SCHEMALESS;
		 CREATE nested:1 SET v = [<set>[1f], <set>[2]];
		 DEFINE FIELD v ON nested TYPE array<set<float>>;
		 DEFINE TABLE arrays SCHEMALESS;
		 CREATE arrays:1 SET v = [1, 2];
		 DEFINE FIELD v ON arrays TYPE array<float>;
		 DEFINE TABLE floats SCHEMALESS;
		 CREATE floats:1 SET v = <set>[1f, 2f];
		 DEFINE FIELD v ON floats TYPE set<float>;",
	)
	.await;
	drain(&ds).await;
	let one_failure = ["String(\"v\"):Number(Int(1)):Bool(true)".to_owned()];
	for tb in ["ints", "nested", "arrays"] {
		assert_eq!(verifications(&ds, tb).await, one_failure, "{tb}");
	}
	assert_eq!(run(&ds, "(INFO FOR TABLE floats).migrations;").await, vec![Value::None]);
}

/// A pass reads a field's value by the key a row holds it under. A name that would not parse bare
/// has canonical text in backticks, which is no row's key: read by it, every row would be judged as
/// `NONE`, claiming `option<int>` over a row holding a string and refusing `int` to a row holding
/// an int.
#[tokio::test]
async fn a_verification_reads_a_field_by_the_key_the_row_holds_it_under() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE bad SCHEMALESS;
		 CREATE bad:1 SET `a-b` = 'x';
		 DEFINE FIELD `a-b` ON bad TYPE option<int>;
		 DEFINE TABLE good SCHEMALESS;
		 CREATE good:1 SET `a-b` = 1;
		 DEFINE FIELD `a-b` ON good TYPE int;",
	)
	.await;
	drain(&ds).await;
	assert!(!claimed(&ds, "bad", "`a-b`").await, "a string is not an option<int>");
	assert_eq!(verifications(&ds, "bad").await, ["String(\"`a-b`\"):Number(Int(1)):Bool(true)"]);
	assert!(claimed(&ds, "good", "`a-b`").await, "an int is an int");
}

/// `FLEXIBLE` dropped where nothing enforces it leaves an undeclared key in place; the pass the
/// change queues makes no claim, and neither does the one the rise to level 4 queues.
#[tokio::test]
async fn dropping_flexible_is_verified_against_the_closed_object() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE t SCHEMAFULL;
		 DEFINE FIELD o ON t TYPE object FLEXIBLE;
		 CREATE t:1 SET o = { x: 1 };
		 ALTER FIELD o ON t DROP FLEXIBLE;",
	)
	.await;
	drain(&ds).await;
	assert!(!claimed(&ds, "t", "o").await);
	run(&ds, "ALTER DATABASE test COMPATIBILITY 4;").await;
	drain(&ds).await;
	assert!(!claimed(&ds, "t", "o").await);
}

/// A migration may leave out `FALLBACK` only where the claims vouch for every value against the
/// step's whole test: a value with a key the new definition does not declare takes the
/// `FALLBACK`, so the step without one is refused, and the row reads as it did.
#[tokio::test]
async fn a_step_that_closes_an_object_needs_a_fallback() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE t SCHEMAFULL;
		 DEFINE FIELD o ON t TYPE option<object> FLEXIBLE;
		 CREATE t:1 SET o = { x: 1 };
		 ALTER FIELD o ON t TYPE object;
		 ALTER DATABASE test COMPATIBILITY 4;",
	)
	.await;
	drain(&ds).await;
	assert!(claimed(&ds, "t", "o").await, "every value is an object, and FLEXIBLE leaves it open");
	let refused = attempt(
		&ds,
		"ALTER FIELD o ON t DROP FLEXIBLE MIGRATE EXISTING ROWS AS strict USING $value;",
	)
	.await;
	assert!(
		refused[0].as_ref().is_err_and(|e| e.contains("FALLBACK may be left out only when")),
		"{refused:?}"
	);
	let refused = attempt(
		&ds,
		"DEFINE FIELD OVERWRITE o ON t TYPE object | string
		     MIGRATE EXISTING ROWS AS widen USING $value;",
	)
	.await;
	assert!(
		refused[0].as_ref().is_err_and(|e| e.contains("FALLBACK may be left out only when")),
		"{refused:?}"
	);
	assert_eq!(run(&ds, "SELECT VALUE o FROM t;").await, run(&ds, "RETURN [{ x: 1 }];").await);
	run(
		&ds,
		"ALTER FIELD o ON t DROP FLEXIBLE MIGRATE EXISTING ROWS AS strict USING $value FALLBACK {};",
	)
	.await;
	drain(&ds).await;
	assert_eq!(run(&ds, "SELECT VALUE o FROM t;").await, run(&ds, "RETURN [{}];").await);
	assert!(claimed(&ds, "t", "o").await);
}

/// A pass judges a field's `ASSERT` over `$value`, so a row a later write would refuse for it
/// leaves the field unclaimed, and changing nothing but the `ASSERT` queues a pass.
#[tokio::test]
async fn a_verification_judges_the_assert() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE bad SCHEMALESS;
		 CREATE bad:1 SET n = 1;
		 CREATE bad:2 SET n = 5;
		 DEFINE FIELD n ON bad TYPE int ASSERT $value > 3;
		 DEFINE TABLE ok SCHEMALESS;
		 CREATE ok:1 SET n = 4;
		 DEFINE FIELD n ON ok TYPE int ASSERT $value > 3;",
	)
	.await;
	drain(&ds).await;
	assert!(!claimed(&ds, "bad", "n").await);
	let report = verifications(&ds, "bad").await;
	assert!(report.len() == 1 && report[0].contains("Int(1)"), "{report:?}");
	assert!(claimed(&ds, "ok", "n").await);
	// Only the ASSERT changes. INFO shows no pass over a SCHEMALESS table until it finds a row,
	// so the queue entry is read.
	run(&ds, "ALTER FIELD n ON ok ASSERT $value > 4;").await;
	let (ns, db) = ids(&ds).await;
	let queued = {
		let txn = ds.transaction(Read).await.unwrap();
		let job = txn.get_tb_migration_job(ns, db, &TableName::from("ok")).await.unwrap();
		txn.cancel().await.unwrap();
		job
	};
	assert!(
		queued.is_some_and(|job| job.verify.iter().any(|f| f.name == "n" && !f.finished)),
		"the change queues a pass"
	);
	drain(&ds).await;
	assert!(!claimed(&ds, "ok", "n").await);
	let report = verifications(&ds, "ok").await;
	assert!(report.len() == 1 && report[0].contains("Int(1)"), "{report:?}");
}

/// An `ASSERT` that reads more than `$value` cannot be judged without the write that ran it, so a
/// pass over the field makes no claim even where every value conforms to its type.
#[tokio::test]
async fn an_assert_that_reads_the_row_states_no_claim() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET n = 4, m = 1;
		 DEFINE FIELD n ON t TYPE int ASSERT $value > $this.m;",
	)
	.await;
	drain(&ds).await;
	assert!(!claimed(&ds, "t", "n").await);
}

/// A pass that finds a row which does not conform withdraws any claim the table holds on the
/// definition it judged, so no claim outlives a refutation.
#[tokio::test]
async fn a_failing_pass_withdraws_a_claim_on_the_definition_it_judged() {
	let ds = ds().await;
	// The definition queues a pass over a row it does not admit.
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET n = 'x';
		 DEFINE FIELD n ON t TYPE int;",
	)
	.await;
	let (ns, db) = ids(&ds).await;
	let table = TableName::from("t");
	// A claim on the definition the pass judges, which the row refutes. The transaction is
	// dropped, not only committed, before the job waits for older transactions to end.
	{
		let txn = ds.transaction(Write).await.unwrap();
		let fd = txn.get_tb_field(ns, db, &table, "n", None).await.unwrap().unwrap();
		let digest = crate::exec::migrate::definition_digest(&fd).unwrap();
		txn.promote_conformance(
			ns,
			db,
			&table,
			"n",
			digest,
			crate::catalog::Conformance::Guaranteed,
		)
		.await
		.unwrap();
		txn.commit().await.unwrap();
	}
	assert!(claimed(&ds, "t", "n").await);
	drain(&ds).await;
	assert!(!claimed(&ds, "t", "n").await);
	let report = verifications(&ds, "t").await;
	assert!(report.len() == 1 && report[0].contains("Int(1)"), "{report:?}");
}

/// A definition stored or removed beneath a field starts a pass under way on the table over: the
/// pass folds the definitions beneath a field into what it judges the field against.
#[tokio::test]
async fn a_definition_beneath_a_field_restarts_a_pass_under_way() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET o = { k: 1 };
		 DEFINE FIELD o ON t TYPE object;",
	)
	.await;
	let (ns, db) = ids(&ds).await;
	let table = TableName::from("t");
	let key = crate::key::schema::TableMigrationKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	let requests = crate::key::schema::TableMigrationRequestTbPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	for change in ["DEFINE FIELD o.k ON t TYPE int;", "REMOVE FIELD o.k ON t;"] {
		// The pass has folded the requests, passed a row and counted it.
		let txn = ds.transaction(Write).await.unwrap();
		let mut state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
		let lap = state.lap;
		state.cursor = Some(b"passed".to_vec());
		state.verify[0].failures = 3;
		txn.delr(requests.range().unwrap()).await.unwrap();
		txn.set_key(&key, &state).await.unwrap();
		txn.commit().await.unwrap();
		run(&ds, change).await;
		let txn = ds.transaction(Read).await.unwrap();
		let state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		assert_eq!(state.cursor, None, "{change}");
		assert_eq!(state.verify[0].failures, 0, "{change}");
		assert_eq!(state.lap, lap + 1, "{change}");
	}
}

/// A row with keys no definition declares, at the top and beneath an object, keeps them through
/// the rewrite the job makes when the table is `SCHEMAFULL`: the rewrite changes what the row
/// costs to read, never what it reads as.
#[tokio::test]
async fn undeclared_keys_survive_the_rewrite() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET a = '1', z = 1, o = { x: 1 };
		 CREATE t:2 SET a = 'q', o = {};
		 DEFINE FIELD a ON t TYPE string;
		 DEFINE FIELD o ON t TYPE object;
		 ALTER TABLE t SCHEMAFULL;
		 ALTER FIELD a ON t TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	let expected =
		run(&ds, "RETURN [{ a: 1, id: t:1, o: { x: 1 }, z: 1 }, { a: 0, id: t:2, o: {} }];").await;
	assert_eq!(run(&ds, "SELECT * FROM t ORDER BY id;").await, expected);
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 1).await, head(&ds, "t").await);
	assert_eq!(run(&ds, "SELECT * FROM t ORDER BY id;").await, expected);
}

/// `REMOVE UNDECLARED FIELDS` keeps the record's own keys. The `in` and `out` of an edge stored in
/// a table of `TYPE ANY` are declared by no definition, and a `SCHEMAFULL` write keeps them, so
/// the step keeps them too: through a read that replays the step, through the job's rewrite, and
/// for the graph the edge's keys still join.
#[tokio::test]
async fn removing_undeclared_fields_keeps_an_edges_in_and_out() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE e SCHEMALESS;
		 CREATE a:1, b:1;
		 RELATE a:1->e:1->b:1 SET k = 'v', x = 1;
		 CREATE e:2 SET k = 'w', y = 2;
		 DEFINE FIELD k ON e TYPE string;
		 ALTER TABLE e MIGRATE EXISTING ROWS AS tidy REMOVE UNDECLARED FIELDS;",
	)
	.await;
	let expected = run(
		&ds,
		"RETURN [{ id: e:1, in: a:1, k: 'v', out: b:1 }, { id: e:2, k: 'w' }];
		 RETURN [{ in: a:1, out: b:1 }];
		 RETURN [b:1];",
	)
	.await;
	let reads = "SELECT * FROM e ORDER BY id;
		 SELECT in, out FROM e WHERE in != NONE;
		 SELECT VALUE ->e->b FROM ONLY a:1;";
	assert_eq!(run(&ds, reads).await, expected);
	drain(&ds).await;
	assert_eq!(stamp(&ds, "e", 1).await, head(&ds, "e").await);
	assert_eq!(run(&ds, reads).await, expected);
}

/// A step minted while a lap is under way reaches the rows the lap passed before it. A lap that
/// began with no step stood at target `0`; `REMOVE UNDECLARED FIELDS` queues a rewrite without
/// restarting the lap, so the step leaves that target behind the table's newest step and the
/// lap's end starts another lap, which rewrites the rows the first one had passed.
#[tokio::test]
async fn a_step_minted_mid_lap_reaches_the_rows_the_lap_had_passed() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET a = '1', z = 1;
		 CREATE t:2 SET a = '2', z = 2;
		 CREATE t:3 SET a = '3', z = 3;
		 DEFINE FIELD a ON t TYPE string;",
	)
	.await;
	let (ns, db) = ids(&ds).await;
	let table = TableName::from("t");
	let key = crate::key::schema::TableMigrationKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	let requests = crate::key::schema::TableMigrationRequestTbPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	// The verification's lap has folded the requests, began with no step standing and has
	// passed `t:1`.
	let passed = crate::key::schema::RecordKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
		id: std::borrow::Cow::Owned(RecordIdKey::Number(1)),
	};
	{
		let txn = ds.transaction(Write).await.unwrap();
		let mut state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
		state.target = 0;
		state.cursor = Some(crate::key::KVKey::encode_key(&passed).unwrap().into_vec());
		txn.delr(requests.range().unwrap()).await.unwrap();
		txn.set_key(&key, &state).await.unwrap();
		txn.commit().await.unwrap();
	}
	run(&ds, "ALTER TABLE t MIGRATE EXISTING ROWS AS tidy REMOVE UNDECLARED FIELDS;").await;
	// A transaction counts as open until it is dropped, and the job's verification waits for
	// every open one older than it.
	{
		let txn = ds.transaction(Read).await.unwrap();
		let state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
		txn.cancel().await.unwrap();
		assert!(state.rewrite && state.cursor.is_some(), "the lap is still under way: {state:?}");
	}
	drain(&ds).await;
	for id in 1..=3 {
		assert_eq!(stamp(&ds, "t", id).await, head(&ds, "t").await, "t:{id}");
	}
	assert_eq!(
		run(&ds, "SELECT * FROM t ORDER BY id;").await,
		run(&ds, "RETURN [{ a: '1', id: t:1 }, { a: '2', id: t:2 }, { a: '3', id: t:3 }];").await
	);
}

/// The table migration job's entry for `tb`, if it has one.
async fn entry(
	ds: &Datastore,
	tb: &str,
) -> Option<crate::key::table_migration::TableMigrationState> {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let state = txn.get_tb_migration_job(ns, db, &TableName::from(tb)).await.unwrap();
	txn.cancel().await.unwrap();
	state
}

/// What `INFO FOR TABLE` reports of `tb`'s check for undeclared keys.
async fn undeclared(ds: &Datastore, tb: &str) -> Value {
	run(ds, &format!("(INFO FOR TABLE {tb}).migrations.undeclared;")).await.remove(0)
}

/// The report of a check for undeclared keys, as `INFO FOR TABLE` shows it.
async fn report(ds: &Datastore, failures: i64, finished: bool) -> Value {
	run(ds, &format!("RETURN {{ failures: {failures}, finished: {finished} }};")).await.remove(0)
}

/// A level-4 table made `SCHEMAFULL` over rows holding top-level keys no definition declares
/// counts each such row once, however many such keys it holds: a write of any of them is refused.
/// The check states no claim, and does not keep a declared field from being claimed.
#[tokio::test]
async fn a_table_made_schemafull_reports_the_rows_holding_undeclared_keys() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD a ON t TYPE int;
		 CREATE t:1 SET a = 1;
		 CREATE t:2 SET a = 2, z = 1;
		 CREATE t:3 SET a = 3, y = 'x', z = 2;
		 CREATE t:4 SET a = 4, o = { k: 1 };
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 0, false).await, "the change queues it");
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 3, true).await);
	assert!(claimed(&ds, "t", "a").await);
	let writes = attempt(
		&ds,
		"UPDATE t:1 SET a = 5 RETURN NONE;
		 UPDATE t:2 SET a = 5 RETURN NONE;
		 UPDATE t:4 SET a = 5 RETURN NONE;",
	)
	.await;
	assert!(writes[0].is_ok(), "{writes:?}");
	assert_eq!(
		writes[1],
		Err("Found field 'z', but no such field exists for table 't'".to_owned())
	);
	assert_eq!(
		writes[2],
		Err("Found field 'o', but no such field exists for table 't'".to_owned())
	);
	// A finished check is not judged by the lap a verification of a field starts, and keeps its
	// count.
	run(&ds, "ALTER FIELD a ON t ASSERT $value > 0;").await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 3, true).await);
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 3, true).await);
}

/// `id`, `in` and `out` are the record's own: a write of an edge never refuses them, declared or
/// not, so the check counts only the edge holding a key no definition declares. A table of any
/// type declares no `in` or `out` of its own.
#[tokio::test]
async fn the_check_for_undeclared_keys_passes_the_keys_of_an_edge() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE e TYPE ANY SCHEMALESS;
		 RELATE p:1->e:1->p:2 SET w = 1;
		 RELATE p:1->e:2->p:3 SET w = 2, z = 1;
		 DEFINE FIELD w ON e TYPE int;
		 ALTER TABLE e SCHEMAFULL;",
	)
	.await;
	let declared = entry(&ds, "e").await.and_then(|state| state.undeclared).map(|c| c.declared);
	assert_eq!(declared, Some(vec!["w".to_owned()]));
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "e").await, report(&ds, 1, true).await);
	let writes =
		attempt(&ds, "UPDATE e:1 SET w = 3 RETURN NONE; UPDATE e:2 SET w = 3 RETURN NONE;").await;
	assert!(writes[0].is_ok(), "{writes:?}");
	assert_eq!(
		writes[1],
		Err("Found field 'z', but no such field exists for table 'e'".to_owned())
	);
}

/// A lap that judged every row and counted none leaves nothing to report, and the entry goes once
/// nothing else is left in it.
#[tokio::test]
async fn a_table_whose_rows_hold_only_declared_keys_reports_nothing() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD a ON t TYPE int;
		 DEFINE FIELD b ON t TYPE option<string>;
		 CREATE t:1 SET a = 1;
		 CREATE t:2 SET a = 2, b = 'x';
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	assert!(entry(&ds, "t").await.is_some_and(|state| state.undeclared.is_some()));
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, Value::None);
	assert_eq!(entry(&ds, "t").await, None);
}

/// Raising a database to level 4 checks each of its `SCHEMAFULL` tables, whose rows were written
/// under level 3, for undeclared keys.
#[tokio::test]
async fn raising_the_database_to_level_4_reports_the_rows_holding_undeclared_keys() {
	let ds = ds().await;
	run(
		&ds,
		"ALTER DATABASE test COMPATIBILITY 3;
		 DEFINE TABLE t SCHEMALESS;
		 CREATE t:1 SET a = 1, z = 1;
		 CREATE t:2 SET a = 2;
		 DEFINE FIELD a ON t TYPE int;
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, Value::None, "level 3 queues no check");
	run(&ds, "ALTER DATABASE test COMPATIBILITY 4;").await;
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 1, true).await);
}

/// `REMOVE UNDECLARED FIELDS` clears the keys the check counted, so the check runs again, finds
/// none, and every row is writable.
#[tokio::test]
async fn removing_undeclared_fields_runs_the_check_again() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD a ON t TYPE int;
		 CREATE t:1 SET a = 1, z = 1;
		 CREATE t:2 SET a = 2;
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 1, true).await);
	run(&ds, "ALTER TABLE t MIGRATE EXISTING ROWS AS tidy REMOVE UNDECLARED FIELDS;").await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 0, false).await);
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, Value::None);
	assert_eq!(entry(&ds, "t").await, None);
	let write = attempt(&ds, "UPDATE t:1 SET a = 5 RETURN NONE;").await.remove(0);
	assert!(write.is_ok(), "{write:?}");
}

/// A definition that declares a counted key, or a removal that leaves a key undeclared, changes the
/// names the check judges rows against, so the check runs again over every row.
#[tokio::test]
async fn a_change_of_the_declared_names_runs_the_check_again() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD a ON t TYPE int;
		 CREATE t:1 SET a = 1, z = 1;
		 CREATE t:2 SET a = 2, y = 1, z = 2;
		 CREATE t:3 SET a = 3, z = 3;
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 3, true).await);
	run(&ds, "DEFINE FIELD z ON t TYPE option<int>;").await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 0, false).await);
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 1, true).await);
	// A definition that declares no new name leaves the finished check as it is.
	run(&ds, "DEFINE FIELD OVERWRITE z ON t TYPE option<int> COMMENT 'kept';").await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 1, true).await);
	// At level 3 a removed field keeps its values, which the table no longer declares. `INFO`
	// shows no check there until it counts a row, so the queue entry is read.
	run(&ds, "ALTER DATABASE test COMPATIBILITY 3; REMOVE FIELD z ON t;").await;
	let check = entry(&ds, "t").await.and_then(|state| state.undeclared);
	assert_eq!(
		check,
		Some(crate::key::table_migration::UndeclaredKeys {
			declared: vec!["a".to_owned()],
			failures: 0,
			finished: false,
		})
	);
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 3, true).await);
}

/// A lap that has passed rows under names the table no longer declares starts over, whatever
/// changed the names: the rows it passed were judged against names that no longer stand.
#[tokio::test]
async fn a_lap_judged_under_other_names_starts_over() {
	let ds = ds().await;
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
	let key = crate::key::schema::TableMigrationKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	let requests = crate::key::schema::TableMigrationRequestTbPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	// The lap has folded the requests, passed every row under names that declared `z`, and
	// counted none.
	{
		let txn = ds.transaction(Write).await.unwrap();
		let records = crate::key::schema::RecordPrefix {
			ns,
			db,
			tb: std::borrow::Cow::Borrowed(&table),
		}
		.range()
		.unwrap();
		let rows = txn.scan_raw(records, 16, 0, None).await.unwrap();
		let mut state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
		state.cursor = rows.last().map(|(key, _)| key.clone());
		let check = state.undeclared.as_mut().unwrap();
		check.declared = vec!["a".to_owned(), "z".to_owned()];
		check.failures = 0;
		txn.delr(requests.range().unwrap()).await.unwrap();
		txn.set_key(&key, &state).await.unwrap();
		txn.commit().await.unwrap();
	}
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 1, true).await);
}

/// A write that stores a row without checking it (`OPTION IMPORT`) starts a check under way over,
/// as it starts the verification of a field over.
#[tokio::test]
async fn an_unchecked_write_restarts_a_check_under_way() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD a ON t TYPE int;
		 CREATE t:1 SET a = 1;
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	let (ns, db) = ids(&ds).await;
	let table = TableName::from("t");
	let key = crate::key::schema::TableMigrationKey {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	let requests = crate::key::schema::TableMigrationRequestTbPrefix {
		ns,
		db,
		tb: std::borrow::Cow::Borrowed(&table),
	};
	// The lap has folded the requests, passed a row and counted it.
	let lap = {
		let txn = ds.transaction(Write).await.unwrap();
		let mut state = txn.get_tb_migration_job(ns, db, &table).await.unwrap().unwrap();
		state.verify.clear();
		state.cursor = Some(b"passed".to_vec());
		state.undeclared.as_mut().unwrap().failures = 3;
		txn.delr(requests.range().unwrap()).await.unwrap();
		txn.set_key(&key, &state).await.unwrap();
		txn.commit().await.unwrap();
		state.lap
	};
	run(&ds, "OPTION IMPORT; CREATE t:2 SET a = 2;").await;
	let state = entry(&ds, "t").await.unwrap();
	assert_eq!(state.cursor, None);
	assert_eq!(state.lap, lap + 1);
	assert_eq!(state.undeclared.unwrap().failures, 0);
	drain(&ds).await;
	assert_eq!(entry(&ds, "t").await, None, "the lap judged every row and counted none");
}

/// A `SCHEMALESS` table refuses no key, so a table made `SCHEMALESS` drops its check, and the
/// entry goes once nothing else is left in it.
#[tokio::test]
async fn a_table_made_schemaless_drops_its_check() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE t SCHEMALESS;
		 DEFINE FIELD a ON t TYPE int;
		 CREATE t:1 SET a = 1, z = 1;
		 ALTER TABLE t SCHEMAFULL;",
	)
	.await;
	drain(&ds).await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 1, true).await);
	run(&ds, "ALTER TABLE t SCHEMALESS;").await;
	assert_eq!(entry(&ds, "t").await, None);
	run(&ds, "ALTER TABLE t SCHEMAFULL;").await;
	assert_eq!(undeclared(&ds, "t").await, report(&ds, 0, false).await, "queued again");
}

/// A pass runs a bounded number of pages and keeps its place: a table whose rows outnumber a
/// pass's pages is worked through over several passes, each continuing where the last stopped,
/// and counted once, by the pass that finishes it.
#[tokio::test]
async fn a_pass_stops_at_its_pages_and_the_next_continues() {
	use crate::kvs::tasks::Progress;

	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE string;
		 CREATE |tb:1..1000| SET f = '1';
		 ALTER FIELD f ON tb TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	let interval = Duration::from_secs(60);

	let first = Datastore::table_migration_with_pages(Arc::clone(&ds), interval, 2).await.unwrap();
	assert_eq!(first.progress, Progress::More, "the pass stopped at its pages: {first:?}");
	assert_ne!(stamp(&ds, "tb", 999).await, head(&ds, "tb").await, "the rows past it wait");

	let mut items = first.items;
	for _ in 0..64 {
		let pass =
			Datastore::table_migration_with_pages(Arc::clone(&ds), interval, 2).await.unwrap();
		items += pass.items;
		if pass.progress == Progress::Done {
			break;
		}
	}
	assert_eq!(items, 1, "the table is counted once");
	assert_eq!(stamp(&ds, "tb", 1).await, head(&ds, "tb").await);
	assert_eq!(stamp(&ds, "tb", 999).await, head(&ds, "tb").await);
}

/// The requests queued for `tb` in `test`/`test`, by the database's ids.
async fn queued_requests(
	ds: &Datastore,
	(ns, db): (crate::catalog::NamespaceId, crate::catalog::DatabaseId),
	tb: &str,
) -> usize {
	let txn = ds.transaction(Read).await.unwrap();
	let requests = txn.tb_migration_requests(ns, db, &TableName::from(tb), u32::MAX).await.unwrap();
	txn.cancel().await.unwrap();
	requests.len()
}

/// A queue key names a table's database by id, so the job reads the catalog's lists for its
/// names a page at a time, each page one of the pass's. A pass over a catalog larger than a
/// page stops part-way through the lookup, and the next continues it rather than reading the
/// lists again.
#[tokio::test]
async fn naming_a_table_s_database_spends_the_pass_s_pages() {
	use crate::kvs::tasks::Progress;

	let ds = ds().await;
	// A page of namespaces sorting before `test`, which the lookup finds on the second page.
	let mut sql = String::new();
	for n in 0..crate::kvs::NORMAL_BATCH_SIZE {
		sql.push_str(&format!("DEFINE NAMESPACE a{n:04};"));
	}
	run(&ds, &sql).await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE string;
		 CREATE tb:1 SET f = '1';
		 ALTER FIELD f ON tb TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	let ids = ids(&ds).await;
	let interval = Duration::from_secs(60);
	let queued = queued_requests(&ds, ids, "tb").await;
	assert!(queued > 0);

	// The queue's batch, and the namespace list's first page.
	let first = Datastore::table_migration_with_pages(Arc::clone(&ds), interval, 2).await.unwrap();
	assert_eq!(first.progress, Progress::More, "{first:?}");
	assert_eq!(queued_requests(&ds, ids, "tb").await, queued, "the table's work has not begun");
	// The namespace list's second page, which holds `test`, and the database list's page.
	let second = Datastore::table_migration_with_pages(Arc::clone(&ds), interval, 2).await.unwrap();
	assert_eq!(second.progress, Progress::More, "{second:?}");
	assert_eq!(queued_requests(&ds, ids, "tb").await, queued, "the table's work has not begun");
	// The names are found: the table's first page folds its request.
	let third = Datastore::table_migration_with_pages(Arc::clone(&ds), interval, 1).await.unwrap();
	assert_eq!(third.progress, Progress::More, "{third:?}");
	assert_eq!(queued_requests(&ds, ids, "tb").await, 0, "the request is folded");

	drain(&ds).await;
	assert_eq!(stamp(&ds, "tb", 1).await, head(&ds, "tb").await);
}

/// A table whose database is gone has nothing left to work through: the job deletes its
/// progress and requests.
#[tokio::test]
async fn a_table_whose_database_is_gone_is_forgotten() {
	let ds = ds().await;
	run(
		&ds,
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE string;
		 CREATE tb:1 SET f = '1';
		 ALTER FIELD f ON tb TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	let (ns, db) = ids(&ds).await;
	assert!(queued_requests(&ds, (ns, db), "tb").await > 0);
	run(&ds, "REMOVE DATABASE test;").await;

	drain(&ds).await;
	assert_eq!(queued_requests(&ds, (ns, db), "tb").await, 0, "the requests are deleted");
	let txn = ds.transaction(Read).await.unwrap();
	let progress = txn.get_tb_migration_job(ns, db, &TableName::from("tb")).await.unwrap();
	txn.cancel().await.unwrap();
	assert!(progress.is_none(), "the progress is deleted: {progress:?}");
}

/// A host that builds its datastore without maintenance tasks drives the job with
/// [`Datastore::run_table_migrations`]: a migration's commit writes a request within the
/// request bounds and notifies the job's trigger, and passes run until one reports
/// [`Progress::Done`] leave every row rewritten and nothing queued.
#[tokio::test]
async fn a_host_drives_the_job_with_bounded_passes() {
	use surrealdb_observe::TaskLeaseState;

	use crate::kvs::tasks::Progress;

	let ds = ds().await;
	let mut setup = String::from(
		"DEFINE TABLE tb SCHEMAFULL;
		 DEFINE FIELD f ON tb TYPE string;",
	);
	for i in 0..5000 {
		setup.push_str(&format!("CREATE tb:{i} SET f = '{i}' RETURN NONE;"));
	}
	run(&ds, &setup).await;
	let notified = ds.commit_triggers().table_migration.notified();
	run(
		&ds,
		"ALTER FIELD f ON tb TYPE int MIGRATE EXISTING ROWS AS to_int USING <int>$value FALLBACK 0;",
	)
	.await;
	tokio::time::timeout(Duration::from_secs(1), notified)
		.await
		.expect("the migration's commit notifies the job's trigger");
	let (ns, db) = ids(&ds).await;
	let (start, end) = Datastore::table_migration_request_bounds().unwrap();
	let txn = ds.transaction(Read).await.unwrap();
	let requests =
		txn.tb_migration_requests(ns, db, &TableName::from("tb"), u32::MAX).await.unwrap();
	txn.cancel().await.unwrap();
	// A transaction counts as open until dropped, and a verification waits for those older than it.
	drop(txn);
	assert!(!requests.is_empty(), "the migration writes a request");
	for (key, _) in &requests {
		assert!(start <= *key && *key < end, "{key:?} lies outside the request bounds");
	}

	let before = run(&ds, "SELECT VALUE f FROM tb:1; SELECT VALUE f FROM tb:4999;").await;
	let progress = "(INFO FOR TABLE tb).migrations.progress;";
	let pending = run(&ds, progress).await;
	assert!(matches!(pending[0], Value::Object(_)), "a rewrite under way: {pending:?}");
	let mut passes = 0;
	loop {
		let pass = ds.run_table_migrations(Duration::from_secs(30)).await.unwrap();
		assert_eq!(pass.lease, TaskLeaseState::Held, "{pass:?}");
		passes += 1;
		if pass.progress == Progress::Done {
			break;
		}
		assert!(passes < 100, "the passes did not finish the job");
	}
	assert!(passes > 1, "5000 rows outlast one pass's pages");
	assert_eq!(queued_requests(&ds, (ns, db), "tb").await, 0, "the requests are folded");
	assert_eq!(stamp(&ds, "tb", 1).await, head(&ds, "tb").await);
	assert_eq!(stamp(&ds, "tb", 4999).await, head(&ds, "tb").await);
	assert_eq!(run(&ds, "SELECT VALUE f FROM tb:1; SELECT VALUE f FROM tb:4999;").await, before);
	assert_eq!(run(&ds, progress).await, vec![Value::None], "no rewrite is under way");
}

/// A table of more rows than a page holds, with a rewrite of them queued and not yet begun.
async fn table_under_rewrite(ds: &Datastore) {
	let mut setup = String::from("DEFINE TABLE t SCHEMAFULL; DEFINE FIELD f ON t TYPE int;");
	for i in 0..600 {
		setup.push_str(&format!("CREATE t:{i} SET f = {i} RETURN NONE;"));
	}
	setup.push_str(
		"ALTER FIELD f ON t TYPE float MIGRATE EXISTING ROWS AS to_float USING <float>$value FALLBACK 0f;",
	);
	run(ds, &setup).await;
}

/// Run a pass of the job inside each of the next `commits` commits through `ds`, after that
/// transaction's writes and before its storage commit, counting the passes run. A removal commits
/// its hold on the job's progress before its own commit, so the pass that matters runs inside the
/// last of them.
fn pass_inside_commits(ds: &Arc<Datastore>, commits: usize) -> Arc<std::sync::atomic::AtomicUsize> {
	let ran = Arc::new(std::sync::atomic::AtomicUsize::new(0));
	arm_pass(Arc::clone(ds), Arc::clone(&ran), commits);
	ran
}

fn arm_pass(ds: Arc<Datastore>, ran: Arc<std::sync::atomic::AtomicUsize>, left: usize) {
	if left == 0 {
		return;
	}
	let triggers = Arc::clone(ds.commit_triggers());
	triggers.set_before_commit_hook(Box::new(move || {
		Box::pin(async move {
			// On a task of its own, as the job runs, so it reads the catalog as the job does
			// whatever the committing work around it may read.
			let job = Arc::clone(&ds);
			let pass = Datastore::table_migration_with_pages(job, Duration::from_secs(1), 4);
			tokio::spawn(pass).await.unwrap().unwrap();
			ran.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			arm_pass(ds, ran, left - 1);
		})
	}));
}

/// `REMOVE TABLE` deletes the table's rows key by key in its own transaction, so a page of the
/// job that rewrote rows and committed while the removal ran would fail the removal's commit. A
/// removal holds the job off the table, so the job's pass skips it and the removal commits; the
/// table's rows and its migration go with it.
#[tokio::test]
async fn removing_a_table_under_rewrite_holds_the_job_off_its_rows() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let ran = pass_inside_commits(&ds, 2);
	let removed = attempt(&ds, "BEGIN; REMOVE TABLE t; COMMIT;").await;
	assert!(removed.iter().all(Result::is_ok), "{removed:?}");
	assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 2, "the job ran inside the removal");
	drain(&ds).await;
	let (ns, db) = ids(&ds).await;
	assert_eq!(queued_requests(&ds, (ns, db), "t").await, 0, "the requests are discarded");
	let again = run(
		&ds,
		"DEFINE TABLE t SCHEMAFULL; DEFINE FIELD f ON t TYPE int;
		 SELECT VALUE id FROM t;
		 (INFO FOR TABLE t).migrations.progress;",
	)
	.await;
	assert_eq!(again[2], Value::Array(Default::default()), "the rows went with the table");
	assert_eq!(again[3], Value::None, "the table defined again inherits no rewrite");
}

/// A rewrite queued by the removing transaction itself is seen by no page, so it holds nothing off
/// and the removal commits with it.
#[tokio::test]
async fn a_rewrite_queued_by_the_removing_transaction_does_not_fail_it() {
	let ds = ds().await;
	run(&ds, "DEFINE TABLE t SCHEMAFULL; DEFINE FIELD f ON t TYPE int; CREATE t:1 SET f = 1;")
		.await;
	let removed = attempt(
		&ds,
		"BEGIN;
		 ALTER FIELD f ON t TYPE float MIGRATE EXISTING ROWS AS to_float USING <float>$value FALLBACK 0f;
		 REMOVE TABLE t;
		 COMMIT;",
	)
	.await;
	assert!(removed.iter().all(Result::is_ok), "{removed:?}");
	assert!(progress(&ds).await.is_none(), "no hold was recorded");
}

/// `REMOVE DATABASE` deletes only the definition and leaves the rows to a background reclaim, so
/// a page of the job that commits while the removal runs conflicts with nothing it writes.
#[tokio::test]
async fn removing_a_database_under_rewrite_commits() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let ran = pass_inside_commits(&ds, 1);
	let removed = attempt(&ds, "BEGIN; REMOVE DATABASE test; COMMIT;").await;
	assert_eq!(ran.load(std::sync::atomic::Ordering::SeqCst), 1, "the job ran inside the removal");
	assert!(removed.iter().all(Result::is_ok), "{removed:?}");
	drain(&ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	assert!(txn.get_db_by_name("test", "test", None).await.unwrap().is_none());
	txn.cancel().await.unwrap();
}

/// The hold lasts as long as the removing transaction: the job does not rewrite the table's rows
/// while it is open, and resumes them once it rolls back.
#[tokio::test]
async fn a_rolled_back_removal_releases_the_rows_to_the_job() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let (ns, db) = ids(&ds).await;
	let before = stamp(&ds, "t", 599).await;
	assert_ne!(before, head(&ds, "t").await, "the rows are behind the newest step");
	let removal = ds.transaction(Write).await.unwrap();
	removal.hold_table_for_removal(ns, db, &TableName::from("t")).await.unwrap();
	assert!(ds.commit_triggers().table_removals().removing(&(ns, db, TableName::from("t"))));
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 599).await, before, "no row is rewritten under the hold");
	removal.cancel().await.unwrap();
	// Counted among the transactions the verification waits out until dropped.
	drop(removal);
	assert!(!ds.commit_triggers().table_removals().removing(&(ns, db, TableName::from("t"))));
	hold_taken_out(&ds).await;
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 599).await, head(&ds, "t").await, "the rewrite resumes");
}

/// Wait for the end of a removal to take its hold out of the progress through `t`, which it does
/// on a task of its own.
async fn hold_taken_out(ds: &Datastore) {
	let out = async {
		while progress(ds).await.is_some_and(|state| state.held()) {
			tokio::time::sleep(Duration::from_millis(5)).await;
		}
	};
	tokio::time::timeout(Duration::from_secs(5), out).await.expect("the hold is taken out");
}

/// The job's progress through `t`, as stored.
async fn progress(ds: &Datastore) -> Option<crate::key::table_migration::TableMigrationState> {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Read).await.unwrap();
	let key = crate::key::schema::TableMigrationKey {
		ns,
		db,
		tb: std::borrow::Cow::Owned(TableName::from("t")),
	};
	let state = txn.get_key(&key, None).await.unwrap();
	txn.cancel().await.unwrap();
	state
}

/// Write `change` of the job's progress through `t` in a transaction of its own, as a removal on
/// another node records and takes out its hold.
async fn change_progress(
	ds: &Datastore,
	change: impl FnOnce(&mut crate::key::table_migration::TableMigrationState),
) {
	let (ns, db) = ids(ds).await;
	let txn = ds.transaction(Write).await.unwrap();
	let key = crate::key::schema::TableMigrationKey {
		ns,
		db,
		tb: std::borrow::Cow::Owned(TableName::from("t")),
	};
	let before = txn.get_key(&key, None).await.unwrap();
	let mut after = before.clone().unwrap_or_default();
	change(&mut after);
	txn.put_compare_key(&key, &after, before.as_ref()).await.unwrap();
	txn.commit().await.unwrap();
}

fn hold(node: uuid::Uuid) -> crate::key::table_migration::RemovalHold {
	crate::key::table_migration::RemovalHold {
		id: uuid::Uuid::now_v7(),
		node,
	}
}

/// A hold in the progress keeps the job off the table's rows whichever node recorded it, without
/// the removing datastore's own registration, and the rewrite resumes once it is taken out.
#[tokio::test]
async fn a_hold_in_the_progress_keeps_the_job_off_the_rows() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let before = stamp(&ds, "t", 0).await;
	let held = hold(ds.id());
	change_progress(&ds, |state| state.holds.push(held.clone())).await;
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 0).await, before, "no row is rewritten under the hold");
	let state = progress(&ds).await.expect("the requests are folded beneath the hold");
	assert_eq!((state.rewrite, state.holds.clone()), (true, vec![held.clone()]));
	change_progress(&ds, |state| state.holds.retain(|h| *h != held)).await;
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 0).await, head(&ds, "t").await, "the rewrite resumes");
}

/// A page whose snapshot was taken before a hold committed writes the progress by
/// compare-and-set, so its commit fails and its rows are not rewritten: a removal's hold is in
/// force from its commit on, not from the next page.
#[tokio::test]
async fn a_page_begun_before_a_hold_commits_nothing() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let pass = |pages| {
		Datastore::table_migration_with_pages(Arc::clone(&ds), Duration::from_secs(60), pages)
	};
	// Single pages until the requests are folded into a progress, before any row is read: the
	// lease is taken and the queues read by then, so the next commit is a page of rows.
	while progress(&ds).await.is_none() {
		pass(1).await.unwrap();
	}
	let before = stamp(&ds, "t", 0).await;
	assert_ne!(before, head(&ds, "t").await, "no row is rewritten yet");
	let held = hold(ds.id());
	let recorder = Arc::clone(&ds);
	let recorded = Arc::new(std::sync::atomic::AtomicBool::new(false));
	let flag = Arc::clone(&recorded);
	// The hold commits inside the next commit, after that page read and rewrote its rows.
	ds.commit_triggers().set_before_commit_hook(Box::new(move || {
		Box::pin(async move {
			change_progress(&recorder, |state| state.holds.push(held)).await;
			flag.store(true, std::sync::atomic::Ordering::SeqCst);
		})
	}));
	while !recorded.load(std::sync::atomic::Ordering::SeqCst) {
		pass(1).await.unwrap();
	}
	assert_eq!(stamp(&ds, "t", 0).await, before, "the page that lost to the hold wrote nothing");
	pass(8).await.unwrap();
	assert_eq!(stamp(&ds, "t", 0).await, before, "no later page passes the hold");
	assert_eq!(progress(&ds).await.expect("progress").scanned, 0, "the lap has not moved");
}

/// A hold whose node is not in the cluster lost its release: the job takes it out and the
/// rewrite goes on. Judged so only where this datastore's own node is registered.
#[tokio::test]
async fn a_hold_of_a_node_that_is_gone_is_taken_out() {
	let ds = ds().await;
	ds.insert_node().await.unwrap();
	table_under_rewrite(&ds).await;
	change_progress(&ds, |state| state.holds.push(hold(uuid::Uuid::now_v7()))).await;
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 599).await, head(&ds, "t").await, "the rewrite went on");
	assert!(progress(&ds).await.is_none_or(|state| state.holds.is_empty()), "the hold is gone");
}

/// A progress holding nothing but a hold whose release was lost, as one is once the removal's
/// discard is folded and the release never comes, is visited for its hold: the job takes it out
/// and deletes the spent progress, so the table defined again inherits no hold.
#[tokio::test]
async fn a_lost_hold_left_alone_in_the_progress_is_taken_out() {
	let ds = ds().await;
	ds.insert_node().await.unwrap();
	run(&ds, "DEFINE TABLE t SCHEMAFULL;").await;
	change_progress(&ds, |state| state.holds.push(hold(uuid::Uuid::now_v7()))).await;
	let held = progress(&ds).await.expect("the hold made a progress");
	assert!(held.held() && !held.has_work(), "{held:?}");
	drain(&ds).await;
	assert!(progress(&ds).await.is_none(), "the spent progress is deleted");
}

/// A removal through `REMOVE TABLE` records its hold in the progress where the table has work
/// queued, and takes it out when it rolls back.
#[tokio::test]
async fn a_removal_records_its_hold_and_takes_it_out_when_it_ends() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let (ns, db) = ids(&ds).await;
	let removal = ds.transaction(Write).await.unwrap();
	removal.hold_table_for_removal(ns, db, &TableName::from("t")).await.unwrap();
	let holds = progress(&ds).await.expect("the hold made a progress").holds;
	assert_eq!(holds.len(), 1, "{holds:?}");
	assert_eq!(holds[0].node, ds.id());
	removal.cancel().await.unwrap();
	drop(removal);
	hold_taken_out(&ds).await;
	drain(&ds).await;
	assert_eq!(stamp(&ds, "t", 599).await, head(&ds, "t").await, "the rewrite resumes");
}

/// A removal whose snapshot predates a page of rows the job has since committed would fail its
/// commit on those rows, so it fails with a retryable conflict when it records its hold, before
/// it deletes anything.
#[tokio::test]
async fn a_removal_a_page_has_beaten_fails_before_it_deletes() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let (ns, db) = ids(&ds).await;
	let page =
		|| Datastore::table_migration_with_pages(Arc::clone(&ds), Duration::from_secs(60), 1);
	// The lap first waits out every transaction older than its verification, so the removal
	// begins once the lap is past that.
	for _ in 0..16 {
		if progress(&ds).await.is_some_and(|state| state.settled || !state.verifying()) {
			break;
		}
		page().await.unwrap();
	}
	let position = |state: Option<crate::key::table_migration::TableMigrationState>| {
		state.map(|state| (state.lap, state.scanned))
	};
	let removal = ds.transaction(Write).await.unwrap();
	let seen = position(progress(&ds).await);
	for _ in 0..16 {
		if position(progress(&ds).await) != seen {
			break;
		}
		page().await.unwrap();
	}
	assert!(progress(&ds).await.is_some_and(|state| state.rewrite && state.scanned > 0));
	let refused = removal.hold_table_for_removal(ns, db, &TableName::from("t")).await;
	let e = refused.expect_err("the removal is refused before it deletes");
	assert!(crate::kvs::is_retryable_transaction_conflict(&e), "{e}");
	removal.cancel().await.unwrap();
	assert!(!ds.commit_triggers().table_removals().removing(&(ns, db, TableName::from("t"))));
}

/// A rewrite that finished after the removal's snapshot rewrote rows the removal would fail its
/// commit on, though its progress no longer asks for a rewrite, so the removal fails before it
/// deletes anything.
#[tokio::test]
async fn a_removal_a_finished_rewrite_has_beaten_fails_before_it_deletes() {
	let ds = ds().await;
	table_under_rewrite(&ds).await;
	let (ns, db) = ids(&ds).await;
	let page =
		|| Datastore::table_migration_with_pages(Arc::clone(&ds), Duration::from_secs(60), 1);
	for _ in 0..16 {
		if progress(&ds).await.is_some_and(|state| state.settled || !state.verifying()) {
			break;
		}
		page().await.unwrap();
	}
	let removal = ds.transaction(Write).await.unwrap();
	for _ in 0..64 {
		if progress(&ds).await.is_none_or(|state| !state.rewrite) {
			break;
		}
		page().await.unwrap();
	}
	assert!(progress(&ds).await.is_none_or(|state| !state.rewrite), "the rewrite finished");
	let refused = removal.hold_table_for_removal(ns, db, &TableName::from("t")).await;
	let e = refused.expect_err("the removal is refused before it deletes");
	assert!(crate::kvs::is_retryable_transaction_conflict(&e), "{e}");
	removal.cancel().await.unwrap();
}
