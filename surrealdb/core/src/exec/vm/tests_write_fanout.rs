//! A write over rows whose iterations are independent answers what the serial run answers, at
//! every width: the compute half of each record runs on a worker, the statement's task applies the
//! records in the order of the serial run, and the rows it returns, the records it leaves, the
//! index entries and the change feed are those of a datastore whose scans never split.
//!
//! The reference of every case is the same statements over a datastore that never hands a batch to
//! a worker (`whole`), which runs the same code at width one. The tests that count what workers
//! did read process globals through [`counts_of`]; they take turns, and run one process each under
//! `cargo nextest`. A worker makes no read of stored data: the statement's task reads the
//! record-type marker of each row it hands on, and [`txn_reads_on_workers`] counts every read of a
//! record or a key range a worker makes through the transaction.

use super::morsel::{counts_of, encoded_ahead, txn_reads_on_workers, writes_ended};
use super::tests_morsel::{answered, bind, multiset, outcome, statement, tiny, whole};
use super::tests_placement::{admission_of, decided, write_stages};
use crate::exec::operators::test_util::TestDb;
use crate::kvs::TransactionType;
use crate::val::{Number, Value};

/// Workers draw on one pool the whole process shares, so the tests that count on holding places
/// take turns.
static TURNS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Runs `child`, an `#[ignore]`d test of this module, in a process that holds no other test, and
/// asserts that it succeeded and printed `marker`.
///
/// `reseed` switches the whole process to a seeded generator and every record id generated
/// without a name draws from it, so a test that compares draws at two widths runs where no other
/// test draws between its reseed and its last draw.
fn in_a_process_of_its_own(child: &str, marker: &str) {
	let output = std::process::Command::new(std::env::current_exe().expect("test binary"))
		.args([
			"--exact",
			&format!("exec::vm::tests_write_fanout::{child}"),
			"--ignored",
			"--nocapture",
			"--test-threads=1",
		])
		.output()
		.expect("child test runs");
	let stdout = String::from_utf8_lossy(&output.stdout);
	assert!(
		output.status.success() && stdout.contains(marker),
		"child failed: {}\n{stdout}\n{}",
		output.status,
		String::from_utf8_lossy(&output.stderr),
	);
}

/// Rows of the table the tests write.
const ROWS: usize = 1_200;

/// A table of [`ROWS`] rows.
fn setup() -> String {
	format!(
		"DEFINE TABLE t TYPE NORMAL;
		 FOR $i IN 0..{ROWS} {{
			CREATE type::record('t', $i) SET n = $i, name = 'n' + <string> $i, s = <string> $i
		 }};"
	)
}

/// What a request answers over `db`: one answer for each of its statements.
async fn request(db: &TestDb, sql: &str) -> Vec<Result<Value, String>> {
	db.datastore()
		.execute(sql, &TestDb::owner(), None)
		.await
		.expect("the request runs")
		.into_iter()
		.map(|response| match response.result {
			Ok(value) => Ok(Value::from(value)),
			Err(e) => Err(e.message().to_owned()),
		})
		.collect()
}

/// What the statements `statements` answer over `db`, each followed by the records the table
/// holds and `observe`, which reads what else the statements leave behind.
async fn answers(
	db: &TestDb,
	statements: &[&str],
	observe: &[&str],
) -> Vec<Result<Vec<Value>, String>> {
	answers_of(db, "t", statements, observe).await
}

/// [`answers`] for a table of another name.
async fn answers_of(
	db: &TestDb,
	table: &str,
	statements: &[&str],
	observe: &[&str],
) -> Vec<Result<Vec<Value>, String>> {
	let mut out = Vec::new();
	for sql in statements {
		out.push(answered(db, sql).await);
		out.push(answered(db, &format!("SELECT * FROM {table} ORDER BY id")).await);
		for sql in observe {
			out.push(answered(db, sql).await);
		}
	}
	out
}

/// How a statement of `db` is placed: the verdict of each of its pipelines.
async fn verdicts(db: &TestDb, sql: &str) -> Vec<String> {
	admission_of(&db.exec_ctx().await, sql).await.into_iter().map(|(verdict, _)| verdict).collect()
}

/// Whether the pipeline of the write is planned as morsels whose effects apply in order.
fn fans_out(verdicts: &[String]) -> bool {
	verdicts.iter().any(|verdict| verdict == "Batches OrderedApply")
}

/// What the workers of a datastore had done when a case began: the batches handed to them and the
/// reads of stored data they made through the transaction.
struct Mark {
	handed: usize,
	reads: usize,
}

impl Mark {
	async fn of(db: &TestDb) -> Self {
		Self {
			handed: counts_of(&db.exec_ctx().await).handed,
			reads: txn_reads_on_workers(),
		}
	}

	/// Asserts that a batch went to a worker since the mark, and that no worker read stored data
	/// through the transaction.
	async fn fanned_out(&self, db: &TestDb, what: &str) {
		let handed = counts_of(&db.exec_ctx().await).handed - self.handed;
		assert!(handed > 0, "{what}: no batch went to a worker");
		assert_eq!(
			txn_reads_on_workers(),
			self.reads,
			"{what}: a worker read stored data through the transaction"
		);
	}
}

/// Runs `statements` over a datastore that never hands a batch on and over one that hands every
/// batch it can, asserts the answers and what `observe` reads after each are equal, and that the
/// write fans out where `fans` says it must and runs serially where it must not. Answers how many
/// batches went to workers.
async fn same_answers(setup: &str, statements: &[(&str, bool)], observe: &[&str]) -> usize {
	let serial = TestDb::new_with_config(setup, whole()).await;
	let wide = TestDb::new_with_config(setup, tiny()).await;
	same_answers_over(&serial, &wide, statements, observe).await
}

/// [`same_answers`] over two datastores.
async fn same_answers_over(
	serial: &TestDb,
	wide: &TestDb,
	statements: &[(&str, bool)],
	observe: &[&str],
) -> usize {
	same_answers_over_table(serial, wide, "t", statements, observe).await
}

/// [`same_answers_over`] where the statements write the table `table`.
async fn same_answers_over_table(
	serial: &TestDb,
	wide: &TestDb,
	table: &str,
	statements: &[(&str, bool)],
	observe: &[&str],
) -> usize {
	let before = counts_of(&wide.exec_ctx().await).handed;
	let reads = txn_reads_on_workers();
	let sqls: Vec<&str> = statements.iter().map(|(sql, _)| *sql).collect();
	let expected = answers_of(serial, table, &sqls, observe).await;
	for (sql, fans) in statements {
		let placed = verdicts(wide, sql).await;
		assert_eq!(fans_out(&placed), *fans, "`{sql}` is placed {placed:?}");
	}
	let got = answers_of(wide, table, &sqls, observe).await;
	// After each statement come the records in key order and what `observe` reads: the rows of an
	// observation that no `ORDER BY` orders arrive in any order.
	let per_statement = 2 + observe.len();
	for (at, (got, expected)) in got.iter().zip(&expected).enumerate() {
		let unordered = at % per_statement >= 2
			&& !observe[at % per_statement - 2].to_uppercase().contains("ORDER BY");
		match (got, expected) {
			(Ok(got), Ok(expected)) if unordered => {
				assert_eq!(multiset(got), multiset(expected), "answer {at} of {sqls:?}");
			}
			_ => assert_eq!(got, expected, "answer {at} of {sqls:?}"),
		}
	}
	assert_eq!(txn_reads_on_workers(), reads, "a worker read stored data through the transaction");
	counts_of(&wide.exec_ctx().await).handed - before
}

const PLAIN: &[(&str, bool)] = &[
	("UPDATE t SET name = name + ' hi!'", true),
	("UPDATE t SET n = n + 1 WHERE n % 3 = 0", true),
	("UPDATE t SET s = 'z' RETURN NONE", true),
	("UPDATE t SET s = 'a' RETURN BEFORE", true),
	("UPDATE t SET s = 'b' RETURN DIFF", true),
	("UPDATE t SET s = 'c' RETURN AFTER", true),
	("UPDATE t SET n = n + 1 RETURN VALUE n", true),
	("DELETE t WHERE n % 2 = 0", true),
	("DELETE t WHERE n % 5 = 0 RETURN BEFORE", true),
	("DELETE t", true),
];

/// An update and a delete over a table scan fan out on the in-memory backend, which offers no
/// read view: their workers read no stored data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_over_a_scan_fan_out_without_a_read_view() {
	let _turn = TURNS.lock().await;
	let handed = same_answers(&setup(), PLAIN, &[]).await;
	assert!(handed > 0, "no batch went to a worker");
}

/// A `RETURN` clause that reads a field of the record's images (`$before`, `$after`) reads it off
/// the image, which is the record's object, and fetches nothing: the write fans out. One that reads
/// through a record id an image holds fetches that record, and the write runs serially.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_return_that_reads_the_images_fans_out() {
	let _turn = TURNS.lock().await;
	let statements = [
		("UPDATE t SET n = n + 1 RETURN VALUE $before.n", true),
		("UPDATE t SET name = name + '!' RETURN $before.name, $after.name", true),
		("UPDATE t SET s = 'z' WHERE n % 3 = 0 RETURN VALUE [$before.s, $after.s]", true),
		("DELETE t WHERE n % 4 = 0 RETURN VALUE $before.id", true),
		("UPDATE t SET s = 'y' RETURN VALUE $after.id.n", false),
	];
	let handed = same_answers(&setup(), &statements, &[]).await;
	assert!(handed > 0, "no batch went to a worker");
}

/// The same statements on a coordinator that may take no place answer the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_run_on_the_coordinator_alone_when_no_place_is_free() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let narrow =
		TestDb::new_with_config(&setup(), tiny().with_key_value("morsel_places", "0")).await;
	let handed = same_answers_over(&serial, &narrow, PLAIN, &[]).await;
	assert_eq!(handed, 0);
}

/// The data clauses and the schema of a table are the compute half: asserts, defaults, values and
/// computed fields, and a field the table's type coerces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_schema_is_applied_by_the_compute_half() {
	let _turn = TURNS.lock().await;
	let schema = "DEFINE TABLE t SCHEMAFULL;
		DEFINE FIELD n ON t TYPE int;
		DEFINE FIELD name ON t TYPE string ASSERT string::len($value) > 0;
		DEFINE FIELD s ON t TYPE string DEFAULT 'none';
		DEFINE FIELD up ON t VALUE string::uppercase(name);
		DEFINE FIELD sq ON t COMPUTED n * n;
		DEFINE FIELD score ON t TYPE float DEFAULT 1.5;"
		.to_owned()
		+ &format!(
			"FOR $i IN 0..{ROWS} {{ CREATE type::record('t', $i) SET n = $i, name = 'n' + <string> $i }};"
		);
	let handed = same_answers(
		&schema,
		&[
			("UPDATE t SET name = name + 'x'", true),
			("UPDATE t SET n = n + 2 WHERE n % 4 = 0", true),
			("UPDATE t SET score = 2.5 RETURN AFTER", true),
			("DELETE t WHERE n % 3 = 0 RETURN BEFORE", true),
		],
		&[],
	)
	.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// An index over the updated field is maintained by the apply half in the serial order, and its
/// entries are those of the serial run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_index_over_the_updated_field_is_maintained_in_order() {
	let _turn = TURNS.lock().await;
	let indexed = format!(
		"{} DEFINE INDEX by_n ON t FIELDS n; DEFINE INDEX by_name ON t FIELDS name;",
		setup()
	);
	let handed = same_answers(
		&indexed,
		&[
			("UPDATE t SET n = n + 10000", true),
			("UPDATE t SET name = 'x' + <string> n WHERE n % 2 = 0", true),
			("DELETE t WHERE n % 7 = 0", true),
		],
		&[
			"SELECT n FROM t WITH INDEX by_n WHERE n > 10000",
			"SELECT name FROM t WITH INDEX by_name WHERE name >= 'x'",
		],
	)
	.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// A failure of the compute half is raised at its position, after every record before it applied:
/// the failure of the first record that fails, which the serial run raises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_failure_is_raised_where_the_serial_run_raises_it() {
	let _turn = TURNS.lock().await;
	let failing = format!("{} UPDATE t:2 SET s = 'x'; UPDATE t:900 SET s = 'y';", setup());
	let serial = TestDb::new_with_config(&failing, whole()).await;
	let wide = TestDb::new_with_config(&failing, tiny()).await;
	let mark = Mark::of(&wide).await;
	for sql in [
		"UPDATE t SET i = <int> s",
		"UPDATE t SET i = <int> s WHERE n > 100",
		"DELETE t WHERE <int> s > 0",
	] {
		let expected = answers(&serial, &[sql], &[]).await;
		assert!(expected[0].is_err(), "`{sql}` fails serially");
		assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
		assert_eq!(answers(&wide, &[sql], &[]).await, expected, "`{sql}`");
	}
	mark.fanned_out(&wide, "failing writes").await;
}

/// A unique index violated by the apply half at record 300 wins over a compute failure at record
/// 360: the failures are raised in the order of the records. Both lie past the first batch, which
/// the statement's task runs whole.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_apply_failure_ahead_of_a_compute_failure_wins() {
	let _turn = TURNS.lock().await;
	let indexed =
		format!("{} DEFINE INDEX uniq ON t FIELDS u UNIQUE; UPDATE t:360 SET s = 'bad';", setup());
	let serial = TestDb::new_with_config(&indexed, whole()).await;
	let wide = TestDb::new_with_config(&indexed, tiny()).await;
	let sql = "UPDATE t SET u = 1, i = <int> s WHERE n >= 299";
	let mark = Mark::of(&wide).await;
	let expected = answers(&serial, &[sql], &[]).await;
	assert!(expected[0].is_err());
	assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
	assert_eq!(answers(&wide, &[sql], &[]).await, expected);
	mark.fanned_out(&wide, sql).await;
	let message = format!("{:?}", expected[0]);
	assert!(!message.contains("bad"), "the index violation is the first failure: {message}");
}

/// A statement that draws entropy or reads what it writes stays serial, with the answers the
/// serial run gives, the draws of a seeded generator included. The seeded draws run in a process
/// of their own ([`in_a_process_of_its_own`]).
#[test]
fn a_write_that_draws_entropy_or_reads_what_it_writes_runs_serially() {
	in_a_process_of_its_own(
		"a_write_that_draws_entropy_or_reads_what_it_writes_runs_serially_child",
		"entropy-write-checked",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run in a process of its own by a_write_that_draws_entropy_or_reads_what_it_writes_runs_serially"]
async fn a_write_that_draws_entropy_or_reads_what_it_writes_runs_serially_child() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	surrealdb_expr::val::rnd::reseed(7);
	let expected = answers(&serial, &["UPDATE t SET score = rand::float()"], &[]).await;
	surrealdb_expr::val::rnd::reseed(7);
	let got = answers(&wide, &["UPDATE t SET score = rand::float()"], &[]).await;
	assert_eq!(got, expected);
	assert!(!fans_out(&verdicts(&wide, "UPDATE t SET score = rand::float()").await));
	for sql in [
		"UPDATE t SET x = (SELECT VALUE math::sum(n) FROM t GROUP ALL)[0]",
		"FOR $i IN [1, 2] { UPDATE t SET k = $i; }",
	] {
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
		assert_eq!(
			answers(&wide, &[sql], &[]).await,
			answers(&serial, &[sql], &[]).await,
			"`{sql}`"
		);
	}
	// The ids a nested `CREATE` draws are random: the statement is placed serially, and fails to
	// fan out whatever the answers.
	assert!(!fans_out(&verdicts(&wide, "UPDATE t SET made = (CREATE u SET v = 1).id").await));
	println!("entropy-write-checked");
}

/// The refusals of the pipelines of `sql`, and the stage its write is cut at, if it has a cut.
async fn cut_and_refusals(db: &TestDb, sql: &str) -> (Option<u16>, Vec<String>) {
	let ctx = db.exec_ctx().await;
	let op = bind(&ctx, sql).await;
	let decided = decided(&ctx, &op).await.expect("the statement plans");
	let cut = decided.iter().find_map(|decided| decided.attr.write_cut()).map(|cut| cut.stage);
	let refusals = decided.iter().filter_map(|decided| decided.refusal.clone()).collect();
	(cut, refusals)
}

/// The cut of a write is at its first apply stage, and a write whose compute half is empty, whose
/// stages read stored data, or whose events and views may write the table it reads, has none.
#[tokio::test]
async fn a_write_is_cut_at_its_first_apply_stage() {
	let db = TestDb::new_with_config(
		&format!(
			"{} DEFINE TABLE ix; DEFINE INDEX ix_n ON ix FIELDS n; DEFINE TABLE ev TYPE NORMAL; \
			 DEFINE EVENT bump ON ev WHEN true THEN {{ UPDATE type::record('ev', $after.id.id() + 1) \
			 SET z = 1 }}; CREATE ix:1 SET n = 1; CREATE ev:1 SET n = 1;",
			setup()
		),
		tiny(),
	)
	.await;
	// `StoreRecordData` follows the thirteen stages of an update's compute half.
	let (cut, refusals) = cut_and_refusals(&db, "UPDATE t SET name = name + ' hi!'").await;
	assert_eq!((cut, refusals.len()), (Some(13), 0));
	// Its after-image is encoded ahead of the apply: no stage between the cut and the store changes
	// it.
	let ctx = db.exec_ctx().await;
	let op = bind(&ctx, "UPDATE t SET name = name + ' hi!'").await;
	let decided = decided(&ctx, &op).await.expect("the statement plans");
	let cut = decided.iter().find_map(|decided| decided.attr.write_cut()).expect("a cut");
	assert!(cut.encode_at_cut && !cut.compute_reads_data);
	// A delete cuts at the stage that cleans up the references to the record.
	let (cut, _) = cut_and_refusals(&db, "DELETE t WHERE n % 2 = 0").await;
	assert_eq!(cut, Some(5));
	// A table with an index is cut as any other.
	let (cut, _) = cut_and_refusals(&db, "UPDATE ix SET n = n + 1").await;
	assert_eq!(cut, Some(13));
	// A subquery that reads the table the statement writes cannot run ahead of the writes.
	let (cut, refusals) =
		cut_and_refusals(&db, "UPDATE t SET x = (SELECT VALUE math::sum(n) FROM t GROUP ALL)[0]")
			.await;
	assert_eq!(cut, None);
	assert_eq!(
		refusals,
		vec![
			"a stage the plan marks compute holds an op that only the apply half may run"
				.to_owned(),
			"the pipeline or its statement writes".to_owned()
		]
	);
	// An index scan is not a source the statement's own writes leave alone.
	let (_, refusals) = cut_and_refusals(&db, "UPDATE ix SET n = n + 1 WHERE n > 2").await;
	assert_eq!(
		refusals,
		vec!["the write reads its rows through an access path its own writes may move".to_owned()]
	);
	// An event may write the rows the scan has yet to read.
	let (_, refusals) = cut_and_refusals(&db, "UPDATE ev SET n = n + 1").await;
	assert_eq!(
		refusals,
		vec![
			"the loop body, or the code its table stores, may write a record of the table its source reads at another key".to_owned()
		]
	);
}

/// Statements of one request run in order over the same transaction: a write that follows another
/// in an explicit transaction fans out as it does alone and answers the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_second_write_in_an_explicit_transaction_answers_as_serial() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	let sql = "BEGIN; CREATE t:9999 SET n = 9999, name = 'x', s = 'y'; \
	           UPDATE t SET name = name + '!' WHERE n >= 1190; \
	           DELETE t WHERE n % 2 = 0; COMMIT;";
	let mark = Mark::of(&wide).await;
	assert_eq!(request(&wide, sql).await, request(&serial, sql).await);
	mark.fanned_out(&wide, "the writes of an explicit transaction").await;
	assert_eq!(
		answered(&wide, "SELECT * FROM t ORDER BY id").await,
		answered(&serial, "SELECT * FROM t ORDER BY id").await
	);
}

/// What a statement leaves in the change feed is what the serial run leaves, in its order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_change_feed_holds_the_records_in_serial_order() {
	let _turn = TURNS.lock().await;
	let feed = setup().replace(
		"DEFINE TABLE t TYPE NORMAL;",
		"DEFINE TABLE t TYPE NORMAL CHANGEFEED 1d INCLUDE ORIGINAL;",
	);
	let serial = TestDb::new_with_config(&feed, whole()).await;
	let wide = TestDb::new_with_config(&feed, tiny()).await;
	let sql = "UPDATE t SET name = name + '!' WHERE math::abs(n) >= 1100";
	assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
	let mark = Mark::of(&wide).await;
	assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await);
	mark.fanned_out(&wide, sql).await;
	// The versionstamps differ between datastores: the changes are compared in their order.
	let read = async |db: &TestDb| {
		let rows = answered(db, "SHOW CHANGES FOR TABLE t SINCE 0").await.expect("the feed reads");
		rows.into_iter()
			.filter_map(|row| {
				if let Value::Object(object) = row {
					object.get("changes").cloned()
				} else {
					None
				}
			})
			.collect::<Vec<_>>()
	};
	let (expected, got) = (read(&serial).await, read(&wide).await);
	assert!(!expected.is_empty());
	assert_eq!(got, expected);
}

/// A statement under `ONLY` that returns more than one record fails as the serial run fails, and a
/// statement that returns one answers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_is_judged_as_the_serial_run_judges_it() {
	let _turn = TURNS.lock().await;
	let handed = same_answers(
		&setup(),
		&[
			("UPDATE ONLY t SET name = 'x' WHERE n = 7", true),
			("UPDATE ONLY t SET name = 'y' WHERE n < 600", true),
			("DELETE ONLY t WHERE n = 8", true),
		],
		&[],
	)
	.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// A record user's writes are judged by the permissions of the table in the compute half, with
/// the session of the statement: the records a user may not update are left as they are, and the
/// answers are the serial run's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_users_permissions_are_judged_in_the_compute_half() {
	let _turn = TURNS.lock().await;
	let setup = format!(
		"DEFINE ACCESS user ON DATABASE TYPE RECORD;
		 DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		 DEFINE TABLE t SCHEMALESS TYPE NORMAL PERMISSIONS FOR select FULL, FOR update WHERE n % 3 = 0, \
			FOR delete WHERE n % 5 = 0, FOR create NONE;
		 CREATE user:alice;
		 FOR $i IN 0..{ROWS} {{ CREATE type::record('t', $i) SET n = $i, name = 'n' + <string> $i }};"
	);
	let session = || {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	};
	let serial = TestDb::new_with_auth_and_config(&setup, whole()).await;
	let wide = TestDb::new_with_auth_and_config(&setup, tiny()).await;
	let run = async |db: &TestDb, sql: &str| -> Vec<Result<Value, String>> {
		db.datastore()
			.execute(sql, &session(), None)
			.await
			.expect("runs")
			.into_iter()
			.map(|response| match response.result {
				Ok(value) => Ok(Value::from(value)),
				Err(e) => Err(e.message().to_owned()),
			})
			.collect()
	};
	let mark = Mark::of(&wide).await;
	for sql in [
		"UPDATE t SET name = name + '!'",
		"DELETE t RETURN BEFORE",
		"UPDATE t SET n = n + 1 WHERE n > 100",
	] {
		assert_eq!(run(&wide, sql).await, run(&serial, sql).await, "`{sql}`");
		assert_eq!(
			answered(&wide, "SELECT * FROM t ORDER BY id").await,
			answered(&serial, "SELECT * FROM t ORDER BY id").await,
			"after `{sql}`"
		);
	}
	mark.fanned_out(&wide, "the writes of a record user").await;
}

/// Whether the key below a table's prefix holds a table's statistics or the identity of one of the
/// layouts the table stores records under, which are stamped with a time or drawn at random for
/// each datastore.
fn stamped(key: &[u8]) -> bool {
	[&b"!bs"[..], b"!dh", b"!ds", b"!ih", b"!is"].iter().any(|tag| key.starts_with(tag))
}

/// The keys under the table `t` of the first database of `db`, which hold its records and the
/// entries of its indexes, and the bytes stored at them, but for [`stamped`] keys.
async fn stored_bytes(db: &TestDb) -> Vec<(Vec<u8>, Vec<u8>)> {
	let txn = db.datastore().transaction(crate::kvs::TransactionType::Read).await.expect("reads");
	let entries = txn.getr_raw(crate::key::RawRange::every_key(), None).await.expect("scans");
	txn.cancel().await.expect("ends");
	let table = b"/*\0\0\0\0*\0\0\0\0*t\0";
	entries
		.into_iter()
		.filter(|(key, _)| key.starts_with(table) && !stamped(&key[table.len()..]))
		.collect()
}

/// A record that resumes at the cut of a candidate leaves the bytes a record that never left the
/// coordinator leaves, whether the compute half encoded its after-image ahead of the store or the
/// store encodes it, and a statement's counts (`ONLY`, what it returns) are the serial run's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_record_is_stored_as_the_serial_run_stores_it() {
	let _turn = TURNS.lock().await;
	let schema = format!(
		"{} DEFINE FIELD up ON t VALUE string::uppercase(name); DEFINE INDEX by_n ON t FIELDS n;",
		setup()
	);
	let serial = TestDb::new_with_config(&schema, whole()).await;
	let wide = TestDb::new_with_config(&schema, tiny()).await;
	let handed = counts_of(&wide.exec_ctx().await).handed;
	let ahead = encoded_ahead();
	for sql in [
		// The image is encoded ahead of the store.
		"UPDATE t SET name = name + ' hi!' RETURN NONE",
		"UPDATE t SET n = n + 1 WHERE n % 3 = 0 RETURN AFTER",
		// The compute half skips a record, and the statement counts those it applied.
		"UPDATE ONLY t SET s = 'only' WHERE s = '11' RETURN BEFORE",
		"DELETE t WHERE n % 4 = 0 RETURN BEFORE",
	] {
		assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
		let stored = stored_bytes(&serial).await;
		assert!(stored.len() > ROWS / 2, "the dump holds {} entries", stored.len());
		let got = stored_bytes(&wide).await;
		assert_eq!(got, stored, "bytes after `{sql}`");
	}
	assert!(counts_of(&wide.exec_ctx().await).handed > handed, "no batch went to a worker");
	assert!(encoded_ahead() > ahead, "no record was stored from bytes encoded ahead of the store");
}

/// A record a `CREATE` over a range of ids stores leaves the bytes the serial run stores, whether
/// the compute half encoded its after-image ahead of the store or the store encodes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_created_record_is_stored_as_the_serial_run_stores_it() {
	let _turn = TURNS.lock().await;
	let schema = format!(
		"{} DEFINE FIELD up ON t VALUE string::uppercase(name);
		 DEFINE FIELD k ON t VALUE record::id(id);
		 DEFINE INDEX by_k ON t FIELDS k;",
		setup()
	);
	let serial = TestDb::new_with_config(&schema, whole()).await;
	let wide = TestDb::new_with_config(&schema, tiny()).await;
	let mark = Mark::of(&wide).await;
	let ahead = encoded_ahead();
	for sql in [
		"CREATE |t:5000..5800| SET name = 'created', s = 'c' RETURN NONE",
		"CREATE |t:6000..6300| CONTENT { name: 'content', n: 3 } RETURN AFTER",
	] {
		assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
		let stored = stored_bytes(&serial).await;
		assert!(stored.len() > ROWS / 2, "the dump holds {} entries", stored.len());
		assert_eq!(stored_bytes(&wide).await, stored, "bytes after `{sql}`");
	}
	mark.fanned_out(&wide, "created records stored from the compute half").await;
	assert!(encoded_ahead() > ahead, "no record was stored from bytes encoded ahead of the store");
}

/// A statement that times out leaves no worker running, and the datastore answers the next
/// statement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_timed_out_write_releases_its_workers() {
	let _turn = TURNS.lock().await;
	let wide = TestDb::new_with_config(&setup(), tiny()).await;
	let slow = answered(&wide, "UPDATE t SET name = crypto::sha512(name) TIMEOUT 1ms").await;
	assert!(slow.is_err(), "{slow:?}");
	assert_eq!(
		answered(&wide, "SELECT count() FROM t GROUP ALL").await.expect("the datastore answers"),
		answered(&wide, "SELECT count() FROM t GROUP ALL").await.expect("the datastore answers")
	);
	let rows = answered(&wide, "SELECT VALUE name FROM t WHERE n = 3").await.expect("reads");
	assert_eq!(rows, vec![Value::from("n3")], "the timed out statement wrote nothing");
}

/// The same statements on SurrealKV, whose workers share the node's pool with the transaction they
/// write through.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writes_over_a_scan_fan_out_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let indexed = format!("{} DEFINE INDEX by_n ON t FIELDS n;", setup());
	let serial = TestDb::new_on_surrealkv(&indexed, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(&indexed, wide_dir.path(), tiny()).await;
	let handed =
		same_answers_over(&serial, &wide, PLAIN, &["SELECT n FROM t WITH INDEX by_n WHERE n >= 0"])
			.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// A failure of the compute half, and of the apply half, on SurrealKV.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failures_are_raised_in_order_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let failing = format!(
		"{} DEFINE INDEX uniq ON t FIELDS u UNIQUE; UPDATE t:300 SET s = 'x'; UPDATE t:900 SET s = 'y';",
		setup()
	);
	let serial = TestDb::new_on_surrealkv(&failing, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(&failing, wide_dir.path(), tiny()).await;
	let mark = Mark::of(&wide).await;
	for sql in ["UPDATE t SET i = <int> s", "UPDATE t SET u = 1 WHERE n >= 299"] {
		let expected = answers(&serial, &[sql], &[]).await;
		assert!(expected[0].is_err(), "`{sql}` fails serially");
		assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
		assert_eq!(answers(&wide, &[sql], &[]).await, expected, "`{sql}`");
	}
	mark.fanned_out(&wide, "failing writes on SurrealKV").await;
}

/// The statements of the write benches over a table of the shape of their datasets: each fans out,
/// on the in-memory backend and on SurrealKV, and answers what the serial run answers. The benches'
/// numbers are those of a statement that fanned out.
const BENCH_SETUP: &str = "
	DEFINE TABLE record SCHEMAFULL;
	DEFINE FIELD n ON record TYPE int;
	DEFINE FIELD name ON record TYPE string;
	DEFINE FIELD score ON record TYPE float;
	DEFINE FIELD city ON record TYPE string;
	DEFINE FIELD h ON record TYPE option<string>;
	DEFINE FIELD slug ON record TYPE option<string>;
	DEFINE FIELD w ON record TYPE option<array<string>>;
	FOR $i IN 1..1501 {
		CREATE type::record('record', $i)
			SET n = $i, name = 'n' + <string> $i, score = $i * 0.5, city = ['London', 'Paris', 'Berlin'][$i % 3]
			RETURN NONE;
	};";

const BENCH_STATEMENTS: &[(&str, bool)] = &[
	("UPDATE record SET name = name + ' hi!'", true),
	("UPDATE record SET name = name + ' hi!' RETURN NONE", true),
	("UPDATE record SET name = name + ' hi!', score = score * 2 RETURN NONE", true),
	(
		"UPDATE record SET h = crypto::sha256(name + <string> n), slug = string::slug(name), \
		 w = string::words(name) RETURN NONE",
		true,
	),
	("DELETE record RETURN NONE", true),
];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bench_statements_fan_out_on_the_in_memory_backend() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(BENCH_SETUP, whole()).await;
	let wide = TestDb::new_with_config(BENCH_SETUP, tiny()).await;
	let handed = same_answers_over_table(&serial, &wide, "record", BENCH_STATEMENTS, &[]).await;
	assert!(handed > 0, "no batch went to a worker");
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_bench_statements_fan_out_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_surrealkv(BENCH_SETUP, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(BENCH_SETUP, wide_dir.path(), tiny()).await;
	let handed = same_answers_over_table(&serial, &wide, "record", BENCH_STATEMENTS, &[]).await;
	assert!(handed > 0, "no batch went to a worker");
}

/// The schema of the benches `crud/create_range_100k` and `crud/create_range_100k_indexed`, without
/// and with the index.
const CREATE_BENCH_SETUPS: [&str; 2] = [
	"DEFINE TABLE record SCHEMAFULL;
	 DEFINE FIELD n ON record TYPE int VALUE record::id(id);
	 DEFINE FIELD name ON record TYPE string VALUE 'n' + <string> record::id(id);
	 DEFINE FIELD score ON record TYPE float VALUE record::id(id) * 0.5;
	 DEFINE FIELD tags ON record TYPE array<string>;",
	"DEFINE TABLE record SCHEMAFULL;
	 DEFINE FIELD n ON record TYPE int VALUE record::id(id);
	 DEFINE FIELD name ON record TYPE string VALUE 'n' + <string> record::id(id);
	 DEFINE FIELD score ON record TYPE float VALUE record::id(id) * 0.5;
	 DEFINE FIELD tags ON record TYPE array<string>;
	 DEFINE INDEX by_n ON record FIELDS n;",
];

/// The statement of both create benches, over fewer ids.
const CREATE_BENCH_STATEMENTS: &[(&str, bool)] =
	&[("CREATE |record:1..3000| SET tags = ['a', 'b', 'c'] RETURN NONE", true)];

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_create_bench_statements_fan_out_on_the_in_memory_backend() {
	let _turn = TURNS.lock().await;
	for setup in CREATE_BENCH_SETUPS {
		let serial = TestDb::new_with_config(setup, whole()).await;
		let wide = TestDb::new_with_config(setup, tiny()).await;
		let handed =
			same_answers_over_table(&serial, &wide, "record", CREATE_BENCH_STATEMENTS, &[]).await;
		assert!(handed > 0, "no batch went to a worker");
	}
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_create_bench_statements_fan_out_on_surrealkv() {
	let _turn = TURNS.lock().await;
	for setup in CREATE_BENCH_SETUPS {
		let (serial_dir, wide_dir) = (
			tempfile::tempdir().expect("temporary directory"),
			tempfile::tempdir().expect("temporary directory"),
		);
		let serial = TestDb::new_on_surrealkv(setup, serial_dir.path(), whole()).await;
		let wide = TestDb::new_on_surrealkv(setup, wide_dir.path(), tiny()).await;
		let handed =
			same_answers_over_table(&serial, &wide, "record", CREATE_BENCH_STATEMENTS, &[]).await;
		assert!(handed > 0, "no batch went to a worker");
	}
}

/// The notifications of a live query are sent in the order of the records, as the serial run
/// sends them: the apply half sends them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_notifications_arrive_in_the_order_of_the_records() {
	let _turn = TURNS.lock().await;
	let run = async |config: surrealdb_cnf::ConfigMap, fans: bool| {
		let (send, recv) = crate::channel::bounded(100_000);
		let builder = crate::kvs::Datastore::builder().with_notify(send);
		let db = TestDb::new_on(builder, &setup(), config).await;
		let session = TestDb::owner().with_rt(true);
		let live = db
			.datastore()
			.execute("LIVE SELECT * FROM t", &session, None)
			.await
			.expect("the live query starts");
		assert!(live[0].result.is_ok());
		let sql = "UPDATE t SET name = name + '!' WHERE n >= 1000 RETURN NONE; \
		           DELETE t WHERE n >= 1100 RETURN NONE";
		let mark = Mark::of(&db).await;
		for response in db.datastore().execute(sql, &session, None).await.expect("runs") {
			assert!(response.result.is_ok(), "{:?}", response.result);
		}
		if fans {
			mark.fanned_out(&db, "the writes of a live table").await;
		}
		// Notifications are delivered as the transaction's work finishes: take them until none
		// arrives for a moment.
		let mut seen = Vec::new();
		while let Ok(Ok(notification)) =
			tokio::time::timeout(std::time::Duration::from_millis(500), recv.recv()).await
		{
			seen.push((notification.action.to_string(), format!("{:?}", notification.record)));
		}
		seen
	};
	let expected = run(whole(), false).await;
	assert!(expected.len() >= 300, "{} notifications", expected.len());
	assert_eq!(run(tiny(), true).await, expected);
}

/// A read nested in a write statement is placed serially by the rule that no pipeline of a
/// statement that writes fans out, and the statement runs without the guard that refuses a writing
/// program that plans morsels without a cut.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_read_nested_in_a_write_stays_serial() {
	let _turn = TURNS.lock().await;
	let nested = format!(
		"{} DEFINE TABLE u; FOR $i IN 0..40 {{ CREATE type::record('u', $i) SET n = $i * 5, k = $i % 2 }};",
		setup()
	);
	let serial = TestDb::new_with_config(&nested, whole()).await;
	let wide = TestDb::new_with_config(&nested, tiny()).await;
	let sql = "UPDATE t SET flag = true WHERE n IN (SELECT VALUE n FROM u WHERE k = 1)";
	let placed = verdicts(&wide, sql).await;
	assert_eq!(
		placed,
		vec![
			"serial: a stage the plan marks compute holds an op that only the apply half may run"
				.to_owned(),
			"serial: the pipeline or its statement writes".to_owned()
		]
	);
	assert_eq!(answers(&wide, &[sql], &[]).await, answers(&serial, &[sql], &[]).await);
}

/// A write whose iterations are not independent rows of one table, or whose order is not the
/// updating order alone, has no cut and runs serially, with the reason it names.
#[tokio::test]
async fn a_write_without_a_cut_names_why() {
	let db = TestDb::new_with_config(&setup(), tiny()).await;
	for (sql, reason) in [
		(
			"UPSERT t SET n = n + 1 WHERE n % 2 = 0",
			"the logical plan does not allow the writes to apply in order",
		),
		("UPSERT t SET n = n + 1", "two iterations may write one key"),
		("CREATE |t:5000..5003|, |t:5002..5005| SET n = 1", "two iterations may write one key"),
		("CREATE |t:3| SET n = 1", "two iterations may write one key"),
		(
			"INSERT INTO t (SELECT n, name FROM t) ON DUPLICATE KEY UPDATE n = 1",
			"two iterations may write one key",
		),
	] {
		let (cut, refusals) = cut_and_refusals(&db, sql).await;
		assert_eq!(cut, None, "`{sql}`");
		assert!(
			refusals.iter().any(|refusal| refusal == reason),
			"`{sql}` is refused {refusals:?}"
		);
	}
}

/// A write whose data clause calls a function that writes is not computed apart from the writes
/// (a field's clause may not call one at all): the statement is placed serially, and the
/// function's writes are those of the serial run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_set_that_calls_a_writing_function_runs_serially() {
	let _turn = TURNS.lock().await;
	let functions = format!(
		"{} DEFINE FUNCTION fn::noted($v: any) {{ UPSERT type::record('note', $v) SET seen = true; RETURN $v; }};",
		setup()
	);
	let serial = TestDb::new_with_config(&functions, whole()).await;
	let wide = TestDb::new_with_config(&functions, tiny()).await;
	let sql = "UPDATE t SET x = fn::noted(n % 10) RETURN NONE";
	assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
	assert_eq!(answers(&wide, &[sql], &[]).await, answers(&serial, &[sql], &[]).await);
	assert_eq!(
		answered(&wide, "SELECT count() FROM note GROUP ALL").await,
		answered(&serial, "SELECT count() FROM note GROUP ALL").await
	);
}

/// A field whose clause draws entropy or advances a sequence is not computed apart from the
/// writes: the draws and the advances of the serial run happen in the order of the records, so the
/// statement is placed serially with the reason of a compute half that holds apply work, and the
/// values it stores are those of the serial run under the same seed. The seeded draws run in a
/// process of their own ([`in_a_process_of_its_own`]).
#[test]
fn a_field_whose_clause_draws_entropy_or_advances_a_sequence_runs_serially() {
	in_a_process_of_its_own(
		"a_field_whose_clause_draws_entropy_or_advances_a_sequence_runs_serially_child",
		"entropy-field-checked",
	);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run in a process of its own by a_field_whose_clause_draws_entropy_or_advances_a_sequence_runs_serially"]
async fn a_field_whose_clause_draws_entropy_or_advances_a_sequence_runs_serially_child() {
	let _turn = TURNS.lock().await;
	let reason = "a stage the plan marks compute holds an op that only the apply half may run";
	for define in [
		"DEFINE FIELD x ON t VALUE rand::float();",
		"DEFINE FIELD x ON t VALUE rand::string(8);",
		"DEFINE FIELD x ON t DEFAULT ALWAYS rand::int();",
		"DEFINE FIELD x ON t DEFAULT rand::int();",
		"DEFINE SEQUENCE seq; DEFINE FIELD x ON t VALUE sequence::nextval('seq');",
	] {
		let schema = format!("{} {define}", setup());
		surrealdb_expr::val::rnd::reseed(7);
		let serial = TestDb::new_with_config(&schema, whole()).await;
		surrealdb_expr::val::rnd::reseed(7);
		let wide = TestDb::new_with_config(&schema, tiny()).await;
		let sql = "UPDATE t SET s = 'z' RETURN NONE";
		let (cut, refusals) = cut_and_refusals(&wide, sql).await;
		assert_eq!(cut, None, "`{define}`");
		assert!(refusals.iter().any(|refusal| refusal == reason), "`{define}`: {refusals:?}");
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{define}`: `{sql}` stays serial");
		surrealdb_expr::val::rnd::reseed(11);
		let expected = answers(&serial, &[sql], &[]).await;
		surrealdb_expr::val::rnd::reseed(11);
		let got = answers(&wide, &[sql], &[]).await;
		assert_eq!(got, expected, "`{define}`");
	}
	println!("entropy-field-checked");
}

/// A table whose records feed a view, or that an event writes beside, runs serially: the stored
/// code writes records the plan does not show.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_whose_stored_code_writes_runs_serially() {
	let _turn = TURNS.lock().await;
	let stored =
		format!("{} DEFINE TABLE by_s AS SELECT s, count() AS c FROM t GROUP BY s;", setup());
	let serial = TestDb::new_with_config(&stored, whole()).await;
	let wide = TestDb::new_with_config(&stored, tiny()).await;
	let sql = "UPDATE t SET s = 'z' WHERE n % 7 = 0 RETURN NONE";
	assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
	assert_eq!(
		answers(&wide, &[sql], &["SELECT * FROM by_s ORDER BY s"]).await,
		answers(&serial, &[sql], &["SELECT * FROM by_s ORDER BY s"]).await
	);
}

/// A delete purges the edges of every record it deletes, and the purge deletes the edge records,
/// running the events, views and references of their tables: an event that writes the table the
/// delete reads changes rows the delete has still to judge. So a delete fans out only while no
/// table that can hold edges stores such code: once one does, the delete runs serially and judges
/// each row as the serial run leaves it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_fans_out_only_while_no_edge_table_stores_code() {
	let _turn = TURNS.lock().await;
	let setup = format!(
		"DEFINE TABLE t TYPE NORMAL;
		 DEFINE TABLE knows TYPE RELATION FROM t TO t;
		 FOR $i IN 0..{ROWS} {{ CREATE type::record('t', $i) SET n = $i, friends = 1, gone = false }};
		 RELATE t:100->knows->t:1100;"
	);
	let serial = TestDb::new_with_config(&setup, whole()).await;
	let wide = TestDb::new_with_config(&setup, tiny()).await;
	let sql = "DELETE t WHERE gone OR friends = 0 RETURN VALUE $before.id";
	assert!(
		fans_out(&verdicts(&wide, sql).await),
		"`{sql}` fans out while no edge table stores code"
	);
	let mark = Mark::of(&wide).await;
	assert_eq!(answered(&wide, sql).await, Ok(Vec::new()));
	mark.fanned_out(&wide, sql).await;
	let likes = "DEFINE TABLE likes TYPE RELATION FROM t TO t;
		DEFINE EVENT unlike ON likes WHEN $event = 'DELETE' THEN { UPDATE $before.out SET friends -= 1 };
		RELATE t:100->likes->t:1100;
		UPDATE t:100 SET gone = true;";
	serial.run(likes).await;
	wide.run(likes).await;
	assert!(
		!fans_out(&verdicts(&wide, sql).await),
		"`{sql}` stays serial once an edge table stores code"
	);
	let handed = counts_of(&wide.exec_ctx().await).handed;
	let expected = answered(&serial, sql).await;
	assert_eq!(
		expected,
		answered(&serial, "RETURN [t:100, t:1100]").await,
		"the serial run deletes the record the event emptied"
	);
	assert_eq!(answered(&wide, sql).await, expected);
	assert_eq!(counts_of(&wide.exec_ctx().await).handed, handed, "`{sql}` handed a batch on");
}

/// A table not declared `TYPE NORMAL` can hold edges, which a purge of the records they join
/// deletes: a delete of it may delete records it has still to read, so it runs serially though no
/// table stores code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_of_a_table_that_can_hold_edges_runs_serially() {
	let _turn = TURNS.lock().await;
	let wide = TestDb::new_with_config(
		&setup().replace("DEFINE TABLE t TYPE NORMAL;", "DEFINE TABLE t;"),
		tiny(),
	)
	.await;
	for sql in ["DELETE t WHERE n % 2 = 0", "DELETE t RETURN NONE"] {
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
	}
}

/// A grant of the fan-out lease that is refused leaves the batch to the coordinator, which runs
/// the whole body once every batch before it is applied, and the answers are the serial run's. The
/// refusal revokes the lease: no batch goes to a worker after it, and no worker's state is made
/// past the grants the lease gave. The batches that went out before the refusal may outnumber
/// those grants, since a worker's state is lent to the batches after the one it ran.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_grant_runs_the_rest_of_the_write_on_the_coordinator() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_surrealkv(&setup(), serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(&setup(), wide_dir.path(), tiny()).await;
	let failing = super::morsel::fail_grants(&wide.exec_ctx().await, 3);
	let before = counts_of(&wide.exec_ctx().await);
	let sqls = ["UPDATE t SET name = name + '!'", "DELETE t WHERE n % 3 = 0"];
	assert_eq!(answers(&wide, &sqls, &[]).await, answers(&serial, &sqls, &[]).await);
	assert!(failing.refused() > 0, "no grant was refused");
	assert_eq!(
		failing.handed_after_refusal(),
		0,
		"batches went to a worker after the lease refused a grant"
	);
	drop(failing);
	let after = counts_of(&wide.exec_ctx().await);
	assert!(after.handed > before.handed, "no batch went to a worker before the refusal");
	let states = after.states - before.states;
	assert!(states <= 3, "{states} worker states were made past the grants the lease gave");
}

/// A batch ends after the candidate at which its candidates hold a window, and the rows it did not
/// reach run whole on the statement's task after its candidates: the answers are the serial run's
/// whatever the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn candidates_held_past_a_window_end_their_batch() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(&setup(), whole()).await;
	let narrow = TestDb::new_with_config(
		&setup(),
		tiny()
			.with_key_value("morsel_window_bytes", "1024")
			.with_key_value("morsel_target_bytes", "65536"),
	)
	.await;
	let handed = same_answers_over(&serial, &narrow, PLAIN, &[]).await;
	assert!(handed > 0, "no batch went to a worker");
	let counts = counts_of(&narrow.exec_ctx().await);
	assert!(counts.cut_short > 0, "no batch ended before its last row");
	// The candidates not yet applied are bounded by the batches out, each holding a window and the
	// candidate that passes it, which is smaller than a window here.
	let out = 2 * std::thread::available_parallelism().map_or(1, |cores| cores.get()) + 1;
	assert!(counts.most_held > 0, "no candidate was held");
	assert!(
		counts.most_held <= out * 2 * 1024,
		"{} bytes of candidates were held with {out} batches out of 1024 bytes' window",
		counts.most_held
	);
}

/// A worker's state is made when no batch before it has left one, so a write of many batches makes
/// no more than the batches it holds at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_workers_state_is_made_once_and_lent_to_later_batches() {
	let _turn = TURNS.lock().await;
	let many = setup().replace(&format!("0..{ROWS}"), "0..8000");
	let wide = TestDb::new_with_config(&many, tiny()).await;
	let sql = "UPDATE t SET name = name + '!' RETURN NONE";
	assert!(fans_out(&verdicts(&wide, sql).await));
	let before = counts_of(&wide.exec_ctx().await);
	answered(&wide, sql).await.expect("runs");
	let counts = counts_of(&wide.exec_ctx().await);
	let (handed, states) = (counts.handed - before.handed, counts.states - before.states);
	let at_once = 2 * std::thread::available_parallelism().map_or(1, |cores| cores.get()) + 1;
	assert!(handed > 0 && states >= 1, "{handed} batches, {states} states");
	assert!(states <= at_once && states < handed, "{states} states for {handed} batches");
}

/// A failed `ASSERT` at record 700 is the statement's failure, though record 2900 fails it as well.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_first_failed_assert_is_raised() {
	let _turn = TURNS.lock().await;
	let schema = "DEFINE TABLE t; DEFINE FIELD n ON t ASSERT $value != 700 AND $value != 2900;"
		.to_owned()
		+ "FOR $i IN 0..3000 { CREATE type::record('t', $i) SET n = -$i - 1, name = 'n' + <string> $i };";
	let serial = TestDb::new_with_config(&schema, whole()).await;
	let wide = TestDb::new_with_config(&schema, tiny()).await;
	let sql = "UPDATE t SET n = -n - 1";
	let mark = Mark::of(&wide).await;
	let expected = answers(&serial, &[sql], &[]).await;
	assert!(expected[0].is_err(), "`{sql}` fails serially");
	assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
	assert_eq!(answers(&wide, &[sql], &[]).await, expected);
	mark.fanned_out(&wide, sql).await;
	let message = format!("{:?}", expected[0]);
	assert!(message.contains("t:700`") && !message.contains("t:2900"), "{message}");
}

/// The state a write holds as each record ends is the serial run's, whether the record's compute
/// half ran on a worker or the whole body ran on the statement's task.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resumed_record_leaves_the_write_state_the_serial_run_leaves() {
	let _turn = TURNS.lock().await;
	let schema = format!(
		"{} DEFINE FIELD up ON t VALUE string::uppercase(name); DEFINE INDEX by_n ON t FIELDS n;",
		setup()
	);
	let serial = TestDb::new_with_config(&schema, whole()).await;
	let wide = TestDb::new_with_config(&schema, tiny()).await;
	for sql in [
		"UPDATE t SET name = name + '!' RETURN AFTER",
		"UPDATE t SET n = n + 1 WHERE n % 3 = 0 RETURN DIFF",
		"UPDATE ONLY t SET s = 'only' WHERE string::len(s) > 0 AND s = '11' RETURN BEFORE",
		"DELETE t WHERE n % 4 = 0 RETURN BEFORE",
	] {
		let (before_serial, before_wide) = (
			writes_ended(&serial.exec_ctx().await).len(),
			writes_ended(&wide.exec_ctx().await).len(),
		);
		let mark = Mark::of(&wide).await;
		assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
		mark.fanned_out(&wide, sql).await;
		let expected = writes_ended(&serial.exec_ctx().await);
		let got = writes_ended(&wide.exec_ctx().await);
		assert!(expected.len() > before_serial, "`{sql}` ended no record");
		assert_eq!(got[before_wide..], expected[before_serial..], "state after `{sql}`");
	}
}

/// An event writes records the plan does not show, whichever table it writes: a table that stores
/// one runs serially, with the reason that names the code it stores, and the records its event
/// creates are those of the serial run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_event_that_creates_in_another_table_leaves_the_serial_records() {
	let _turn = TURNS.lock().await;
	let stored = format!(
		"{} DEFINE TABLE log; DEFINE EVENT noted ON t WHEN $event = 'UPDATE' THEN {{ \
		 CREATE type::record('log', $after.n) SET n = $after.n, name = $after.name }};",
		setup()
	);
	let serial = TestDb::new_with_config(&stored, whole()).await;
	let wide = TestDb::new_with_config(&stored, tiny()).await;
	let sql = "UPDATE t SET name = name + '!' WHERE n % 5 = 0 RETURN NONE";
	assert_eq!(
		verdicts(&wide, sql).await,
		vec![
			"serial: the loop body, or the code its table stores, may write a record of the table \
			 its source reads at another key"
				.to_owned()
		]
	);
	let observe = ["SELECT * FROM log ORDER BY id"];
	assert_eq!(answers(&wide, &[sql], &observe).await, answers(&serial, &[sql], &observe).await);
	assert_eq!(answered(&wide, "SELECT count() FROM log GROUP ALL").await.expect("reads").len(), 1);
}

/// The stages a `CREATE` runs are the same for a range of ids, a count of ids drawn at random and
/// an id written out: twelve marked compute, none of which checks that the record exists (the put
/// of `StoreRecordData`, an apply stage, raises that it does), and the rest marked apply. So the
/// compute half of a `CREATE` reads no stored data, and its cut is at `StoreRecordData`.
#[tokio::test]
async fn the_stages_of_a_create_are_marked_as_the_plan_cuts_them() {
	use crate::exec::ir::logical::ops::{WriteMark, WriteStageKind};

	let db = TestDb::new_with_config(&setup(), tiny()).await;
	let ctx = db.exec_ctx().await;
	let mut lists = Vec::new();
	for sql in
		["CREATE |t:5000..5003| SET n = 1", "CREATE |t:3| SET n = 1", "CREATE t:7000 SET n = 1"]
	{
		let mut stages = write_stages(&ctx, &bind(&ctx, sql).await).await;
		assert_eq!(stages.len(), 1, "`{sql}` holds one write loop");
		lists.push(stages.remove(0));
	}
	assert_eq!(lists[0], lists[1]);
	assert_eq!(lists[0], lists[2]);
	let stages = &lists[0];
	let first_apply = stages.iter().position(|(_, mark)| *mark == WriteMark::Apply).expect("apply");
	assert_eq!((first_apply, stages[first_apply].0), (12, WriteStageKind::StoreRecordData));
	assert!(stages[..first_apply].iter().all(|(_, mark)| *mark == WriteMark::Compute));
	assert!(stages[first_apply..].iter().all(|(_, mark)| *mark == WriteMark::Apply));
	assert!(!stages.iter().any(|(stage, _)| *stage == WriteStageKind::CheckRecordExists));
	assert!(stages.iter().any(|(stage, _)| *stage == WriteStageKind::GenerateRecordId));
}

/// A `CREATE` over a range of ids is cut where the record is stored: its compute half holds the
/// stages ahead of the put, reads no stored data, and encodes the record for the stage that stores
/// it.
#[tokio::test]
async fn a_create_over_a_range_is_cut_at_the_store_of_the_record() {
	let db = TestDb::new_with_config("DEFINE TABLE c;", tiny()).await;
	let ctx = db.exec_ctx().await;
	let sql = "CREATE |c:1..20| SET m = 1";
	let (cut, refusals) = cut_and_refusals(&db, sql).await;
	assert_eq!((cut, refusals.len()), (Some(12), 0));
	let decided = decided(&ctx, &bind(&ctx, sql).await).await.expect("the statement plans");
	let cut = decided.iter().find_map(|decided| decided.attr.write_cut()).expect("a cut");
	assert!(cut.encode_at_cut && !cut.compute_reads_data);
}

/// An empty table `c`, which the `CREATE`s over a range of ids fill. The record id is no part of
/// what a `CREATE` evaluates its `SET` against, so the fields that vary from record to record are
/// the table's: `n` and `s` are computed from the id the record is given.
const CREATES: &str = "DEFINE TABLE c;
	DEFINE FIELD n ON c VALUE record::id(id) * 2;
	DEFINE FIELD s ON c VALUE <string> record::id(id);";

/// The statements of a `CREATE` over a range of ids, each with whether it fans out.
const CREATE_RANGES: &[(&str, bool)] = &[
	("CREATE |c:1..3000| RETURN NONE", true),
	("CREATE |c:5000..5600| RETURN id", true),
	("CREATE |c:6000..6600| CONTENT { tags: ['a', 'b'] } RETURN AFTER", true),
	("CREATE |c:7000..7600| SET m = 1 RETURN VALUE n", true),
	("CREATE |c:8000..8300| RETURN BEFORE", true),
	("CREATE ONLY |c:9000..9001| SET m = 1", true),
];

/// A `CREATE` over a range of ids fans out on the in-memory backend, which offers no read view: its
/// compute half reads no stored data. The rows it returns, the records it leaves and the order of
/// both are those of a datastore whose batches never go to a worker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_over_a_range_of_ids_fans_out_without_a_read_view() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(CREATES, whole()).await;
	let wide = TestDb::new_with_config(CREATES, tiny()).await;
	let handed = same_answers_over_table(
		&serial,
		&wide,
		"c",
		CREATE_RANGES,
		&["SELECT count() FROM c GROUP ALL"],
	)
	.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// The same statements on a coordinator that may take no place answer the same.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_over_a_range_runs_on_the_coordinator_alone_when_no_place_is_free() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(CREATES, whole()).await;
	let narrow =
		TestDb::new_with_config(CREATES, tiny().with_key_value("morsel_places", "0")).await;
	let handed = same_answers_over_table(&serial, &narrow, "c", CREATE_RANGES, &[]).await;
	assert_eq!(handed, 0);
}

/// An index and a unique index over the table are maintained by the apply half in the order of the
/// ids, and what they leave is what the serial run leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_indexes_of_a_create_are_maintained_in_the_order_of_the_ids() {
	let _turn = TURNS.lock().await;
	let indexed = format!(
		"{CREATES} DEFINE INDEX by_n ON c FIELDS n; DEFINE INDEX uniq ON c FIELDS s UNIQUE;"
	);
	let serial = TestDb::new_with_config(&indexed, whole()).await;
	let wide = TestDb::new_with_config(&indexed, tiny()).await;
	let handed = same_answers_over_table(
		&serial,
		&wide,
		"c",
		&[
			("CREATE |c:1..1500| RETURN NONE", true),
			("CREATE |c:2000..2300| RETURN id", true),
			("CREATE |c:1400..1600| RETURN NONE", true),
		],
		&[
			"SELECT n FROM c WITH INDEX by_n WHERE n > 100 AND n < 110",
			"SELECT s FROM c WITH INDEX uniq WHERE s = '1499'",
		],
	)
	.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// What a `CREATE` leaves in the change feed is what the serial run leaves, in the order of the
/// ids.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_change_feed_of_a_create_holds_the_records_in_the_order_of_the_ids() {
	let _turn = TURNS.lock().await;
	let feed = CREATES.replace("DEFINE TABLE c;", "DEFINE TABLE c CHANGEFEED 1d;");
	let serial = TestDb::new_with_config(&feed, whole()).await;
	let wide = TestDb::new_with_config(&feed, tiny()).await;
	let sql = "CREATE |c:1..1500| RETURN NONE";
	assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
	let mark = Mark::of(&wide).await;
	assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await);
	mark.fanned_out(&wide, sql).await;
	// The versionstamps differ between datastores: the changes are compared in their order.
	let read = async |db: &TestDb| {
		let rows = answered(db, "SHOW CHANGES FOR TABLE c SINCE 0").await.expect("the feed reads");
		rows.into_iter()
			.filter_map(|row| {
				if let Value::Object(object) = row {
					object.get("changes").cloned()
				} else {
					None
				}
			})
			.collect::<Vec<_>>()
	};
	let (expected, got) = (read(&serial).await, read(&wide).await);
	assert!(!expected.is_empty());
	assert_eq!(got, expected);
}

/// A table whose event writes keeps a `CREATE` over it serial, since the plan does not show what
/// the event's body writes, and the records its events create are those of the serial run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_table_whose_event_writes_keeps_a_create_serial() {
	let _turn = TURNS.lock().await;
	let stored = format!(
		"{CREATES} DEFINE TABLE log;
		 DEFINE EVENT noted ON c WHEN $event = 'CREATE' THEN {{
			CREATE type::record('log', $after.id.id()) SET at = $after.id
		 }};"
	);
	let serial = TestDb::new_with_config(&stored, whole()).await;
	let wide = TestDb::new_with_config(&stored, tiny()).await;
	let sql = "CREATE |c:1..1500| RETURN NONE";
	assert_eq!(
		verdicts(&wide, sql).await,
		vec![
			"serial: the loop body, or the code its table stores, may write a record of the table \
			 its source reads at another key"
				.to_owned()
		]
	);
	let observe = ["SELECT * FROM log ORDER BY id"];
	assert_eq!(
		answers_of(&wide, "c", &[sql], &observe).await,
		answers_of(&serial, "c", &[sql], &observe).await,
	);
}

/// A record that exists is the failure of the first id that names it, raised after every id before
/// it is created, and what the failed statement leaves is what the serial run leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_that_exists_fails_the_create_at_its_id() {
	let _turn = TURNS.lock().await;
	let existing = format!("{CREATES} CREATE c:900 SET m = 0; CREATE c:2400 SET m = 0;");
	let serial = TestDb::new_with_config(&existing, whole()).await;
	let wide = TestDb::new_with_config(&existing, tiny()).await;
	let sql = "CREATE |c:1..3000| RETURN NONE";
	let mark = Mark::of(&wide).await;
	let expected = answers_of(&serial, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await;
	assert!(expected[0].is_err(), "`{sql}` fails serially");
	assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
	assert_eq!(
		answers_of(&wide, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await,
		expected
	);
	mark.fanned_out(&wide, sql).await;
	let message = format!("{:?}", expected[0]);
	assert!(message.contains("c:900"), "the first id that exists is the failure: {message}");
}

/// An assert that fails for the ids 1300 and 2600 fails the create at the first of them, whichever
/// batch of a worker computes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_assert_fails_the_create_at_the_first_id_it_fails_for() {
	let _turn = TURNS.lock().await;
	let asserted = "DEFINE TABLE c;
		DEFINE FIELD n ON c TYPE int VALUE record::id(id) ASSERT $value != 1300 AND $value != 2600;";
	let serial = TestDb::new_with_config(asserted, whole()).await;
	let wide = TestDb::new_with_config(asserted, tiny()).await;
	let sql = "CREATE |c:1..3000| RETURN NONE";
	let mark = Mark::of(&wide).await;
	let expected = answers_of(&serial, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await;
	assert!(expected[0].is_err(), "`{sql}` fails serially");
	assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
	assert_eq!(
		answers_of(&wide, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await,
		expected
	);
	mark.fanned_out(&wide, sql).await;
	let message = format!("{:?}", expected[0]);
	assert!(message.contains("1300"), "the first id that fails is the failure: {message}");
}

/// A count of ids, a range whose body draws entropy and a range whose body advances a sequence run
/// serially with the reason they name, and leave the records the serial run leaves. The draws are
/// the process's: nothing here reseeds the generator, which every test that draws shares, and a
/// serial statement draws the same whatever the configuration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_whose_ids_or_body_draw_runs_serially() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config("DEFINE TABLE c;", whole()).await;
	let wide = TestDb::new_with_config("DEFINE TABLE c;", tiny()).await;
	for (sql, reason) in [
		("CREATE |c:1500| SET n = 1 RETURN NONE", "two iterations may write one key"),
		("CREATE |c:1500| SET r = rand::int() RETURN NONE", "two iterations may write one key"),
		("CREATE |c:1..1500| SET r = rand::int() RETURN NONE", "the statement draws entropy"),
		(
			"CREATE |c:2000..3500| SET s = sequence::nextval('seq') RETURN NONE",
			"the statement advances state that outlives it",
		),
	] {
		assert_eq!(verdicts(&wide, sql).await, vec![format!("serial: {reason}")], "`{sql}`");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
	}
	for sql in [
		"SELECT count() FROM c GROUP ALL",
		"SELECT count() FROM c WHERE type::is_record(id) AND record::tb(id) = 'c' GROUP ALL",
		"SELECT count() FROM c WHERE type::is_int(r) GROUP ALL",
	] {
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
	}
}

/// Ranges that may name one id twice, and a range beside another target, are placed serially.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_that_may_name_one_id_twice_runs_serially() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config("DEFINE TABLE c;", whole()).await;
	let wide = TestDb::new_with_config("DEFINE TABLE c;", tiny()).await;
	for sql in [
		"CREATE |c:1..1500|, |c:1000..2500| SET n = 1 RETURN NONE",
		"CREATE |c:1..1500|, c:9000 SET n = 1 RETURN NONE",
		"CREATE c:9100, |c:3000..4500| SET n = 1 RETURN NONE",
	] {
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
	}
}

/// A `CREATE` over a range of ids on SurrealKV, whose workers share the node's pool with the
/// transaction they write through.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_create_over_a_range_of_ids_fans_out_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let indexed = format!("{CREATES} DEFINE INDEX by_n ON c FIELDS n;");
	let serial = TestDb::new_on_surrealkv(&indexed, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(&indexed, wide_dir.path(), tiny()).await;
	let handed = same_answers_over_table(
		&serial,
		&wide,
		"c",
		CREATE_RANGES,
		&["SELECT n FROM c WITH INDEX by_n WHERE n >= 0 AND n < 20"],
	)
	.await;
	assert!(handed > 0, "no batch went to a worker");
}

/// A record that exists and a failed assert on SurrealKV.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_failures_of_a_create_are_raised_in_order_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let failing = "DEFINE TABLE c;
		DEFINE FIELD n ON c TYPE int VALUE record::id(id) ASSERT $value != 2600;
		CREATE c:900 SET m = 0;";
	let serial = TestDb::new_on_surrealkv(failing, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(failing, wide_dir.path(), tiny()).await;
	let mark = Mark::of(&wide).await;
	for sql in ["CREATE |c:1..3000| RETURN NONE", "CREATE |c:1000..3000| RETURN NONE"] {
		let expected = answers_of(&serial, "c", &[sql], &[]).await;
		assert!(expected[0].is_err(), "`{sql}` fails serially");
		assert!(fans_out(&verdicts(&wide, sql).await), "`{sql}` fans out");
		assert_eq!(answers_of(&wide, "c", &[sql], &[]).await, expected, "`{sql}`");
	}
	mark.fanned_out(&wide, "failing creates on SurrealKV").await;
}

/// The schema of the tables a count of ids creates: a field that holds what the id is, and an
/// index over it.
const COUNTED: &str = "DEFINE TABLE c;
	DEFINE FIELD n ON c VALUE string::len(record::id(id));
	DEFINE INDEX by_n ON c FIELDS n;";

/// Why a `CREATE` over a count of ids is not fanned out: the ids are drawn at random, so two
/// iterations may name one key.
const COUNT_REFUSAL: &str = "two iterations may write one key";

/// A count of ids runs on the statement's task alone, whatever the backend, and leaves the records
/// the serial run leaves. The ids are random, so the cases compare what the records are: as many
/// as were asked for, each under an id of the table, no two under one, and the fields and the
/// index they hold.
async fn a_count_of_ids_runs_as_the_serial_run(serial: &TestDb, wide: &TestDb) {
	let handed = counts_of(&wide.exec_ctx().await).handed;
	for sql in [
		"CREATE |c:1500| SET m = 1 RETURN NONE",
		"CREATE |c:600| CONTENT { tags: ['a', 'b'] } RETURN VALUE n",
		"CREATE |c:200| RETURN BEFORE",
	] {
		assert_eq!(verdicts(wide, sql).await, vec![format!("serial: {COUNT_REFUSAL}")], "`{sql}`");
		assert_eq!(answered(wide, sql).await, answered(serial, sql).await, "`{sql}`");
	}
	let sql = "CREATE |c:700| RETURN id";
	assert!(!fans_out(&verdicts(wide, sql).await), "`{sql}` stays serial");
	for db in [serial, wide] {
		let ids = answered(db, sql).await.expect("the statement answers");
		assert_eq!(ids.len(), 700);
		assert_eq!(ids.iter().collect::<std::collections::BTreeSet<_>>().len(), 700, "an id twice");
	}
	assert_eq!(counts_of(&wide.exec_ctx().await).handed, handed, "a create handed a batch on");
	for sql in [
		"SELECT count() FROM c GROUP ALL",
		"SELECT count() FROM c WHERE type::is_record(id) AND record::tb(id) = 'c' GROUP ALL",
		"SELECT n, count() FROM c GROUP BY n",
		"SELECT count() FROM c WITH INDEX by_n WHERE n = 20 GROUP ALL",
		"SELECT count() FROM c WHERE m = 1 GROUP ALL",
	] {
		assert_eq!(answered(wide, sql).await, answered(serial, sql).await, "`{sql}`");
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_of_ids_runs_serially() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(COUNTED, whole()).await;
	let wide = TestDb::new_with_config(COUNTED, tiny()).await;
	a_count_of_ids_runs_as_the_serial_run(&serial, &wide).await;
}

#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_of_ids_runs_serially_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let (serial_dir, wide_dir) = (
		tempfile::tempdir().expect("temporary directory"),
		tempfile::tempdir().expect("temporary directory"),
	);
	let serial = TestDb::new_on_surrealkv(COUNTED, serial_dir.path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(COUNTED, wide_dir.path(), tiny()).await;
	a_count_of_ids_runs_as_the_serial_run(&serial, &wide).await;
}

/// A count of ids over a table whose stored clauses draw entropy runs serially, as a range over
/// it does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_of_ids_over_a_field_that_draws_runs_serially() {
	let _turn = TURNS.lock().await;
	let schema = "DEFINE TABLE c; DEFINE FIELD r ON c VALUE rand::int();";
	let serial = TestDb::new_with_config(schema, whole()).await;
	let wide = TestDb::new_with_config(schema, tiny()).await;
	for (sql, reason) in [
		("CREATE |c:1500| RETURN NONE", COUNT_REFUSAL),
		(
			"CREATE |c:1..1500| RETURN NONE",
			"a stage the plan marks compute holds an op that only the apply half may run",
		),
	] {
		let (cut, refusals) = cut_and_refusals(&wide, sql).await;
		assert_eq!(cut, None, "`{sql}`");
		assert!(refusals.iter().any(|refusal| refusal == reason), "`{sql}`: {refusals:?}");
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
	}
	let sql = "SELECT count() FROM c WHERE type::is_int(r) GROUP ALL";
	assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await);
}

/// A table `c` whose event body draws entropy and writes nothing.
const DRAWING_EVENT: &str =
	"DEFINE TABLE c; DEFINE EVENT e ON c WHEN true THEN { LET $r = rand::int(); };";

/// Why a write over a table that stores an event is not fanned out.
const EVENT_REFUSAL: &str = "the loop body, or the code its table stores, may write a record of the table its source reads at another key";

/// A count of ids over a table whose event body draws entropy runs serially with no cut, refused
/// for the code the table stores as a range over it is, since the plan states no footprint for an
/// event's body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_count_of_ids_over_an_event_that_draws_runs_serially() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(DRAWING_EVENT, whole()).await;
	let wide = TestDb::new_with_config(DRAWING_EVENT, tiny()).await;
	for sql in ["CREATE |c:1500| RETURN NONE", "CREATE |c:1..1500| RETURN NONE"] {
		let (cut, refusals) = cut_and_refusals(&wide, sql).await;
		assert_eq!(cut, None, "`{sql}`");
		assert!(refusals.iter().any(|refusal| refusal == EVENT_REFUSAL), "`{sql}`: {refusals:?}");
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
		assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await, "`{sql}`");
	}
}

/// A write over a count of ids that is no `CREATE` is not cut: it runs serially.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_over_a_count_of_ids_that_is_no_create_runs_serially() {
	let _turn = TURNS.lock().await;
	let wide = TestDb::new_with_config(COUNTED, tiny()).await;
	for sql in [
		"UPDATE |c:1500| SET m = 1 RETURN NONE",
		"UPSERT |c:1500| SET m = 2 RETURN NONE",
		"DELETE |c:1500| RETURN NONE",
	] {
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
	}
}

/// A count of ids stays serial, so that under one seed a datastore that hands every batch it can
/// on gives the same ids, the same records and the same next draw as one that never does: for a
/// count alone, over a table whose event draws, and for a count that fails at a record whose id
/// its field rejects. The check asserts the count is not fanned out; a placement that fans a count
/// out has to leave the generator where a failure at that record leaves it in the serial run. The
/// generator is the process's, so any other test that draws record ids between the reseed and the
/// last draw would shift the stream; the check runs in a process of its own
/// ([`in_a_process_of_its_own`]).
#[test]
fn a_count_of_ids_draws_them_in_the_serial_order_under_a_seed() {
	in_a_process_of_its_own(
		"a_count_of_ids_draws_them_in_the_serial_order_child",
		"seeded-ids-checked",
	);
}

/// A table `c` whose field rejects an id that starts with `a`.
const REJECTING: &str = "DEFINE TABLE c;
	DEFINE FIELD k ON c TYPE string VALUE record::id(id) ASSERT !string::starts_with($value, 'a');";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "run in a process of its own by a_count_of_ids_draws_them_in_the_serial_order_under_a_seed"]
async fn a_count_of_ids_draws_them_in_the_serial_order_child() {
	let _turn = TURNS.lock().await;
	for schema in ["DEFINE TABLE c;", DRAWING_EVENT] {
		let serial = TestDb::new_with_config(schema, whole()).await;
		let wide = TestDb::new_with_config(schema, tiny()).await;
		let sql = "CREATE |c:5000| RETURN id";
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
		surrealdb_expr::val::rnd::reseed(11);
		let expected = answers_of(&serial, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await;
		surrealdb_expr::val::rnd::reseed(11);
		let got = answers_of(&wide, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await;
		assert_eq!(got, expected);
		assert_eq!(expected[0].as_ref().expect("the statement answers").len(), 5000);
	}
	let serial = TestDb::new_with_config(REJECTING, whole()).await;
	let wide = TestDb::new_with_config(REJECTING, tiny()).await;
	let sql = "CREATE |c:5000| RETURN NONE";
	let draw = "RETURN rand::int()";
	assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
	surrealdb_expr::val::rnd::reseed(11);
	let first = answered(&serial, draw).await;
	surrealdb_expr::val::rnd::reseed(11);
	let expected = answers_of(&serial, "c", &[sql], &[draw]).await;
	surrealdb_expr::val::rnd::reseed(11);
	let got = answers_of(&wide, "c", &[sql], &[draw]).await;
	assert_eq!(got, expected);
	assert!(expected[0].is_err(), "the count fails at a rejected id: {:?}", expected[0]);
	assert_ne!(expected[2], first, "the ids drawn before the failure moved the generator");
	println!("seeded-ids-checked");
}

/// The ids that the transaction of `db` holds in the table `c` after it ran `sql`, and what `sql`
/// answered. The transaction stays open between the two: it is neither committed nor, for a
/// statement that failed, rolled back by an executor, so what it holds is what the statement's own
/// applies left.
async fn ids_held_after(db: &TestDb, sql: &str) -> (Result<Vec<Value>, String>, Vec<i64>) {
	let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
	let answer = outcome(&statement(&ctx, sql).await, &ctx).await;
	let read = statement(&ctx, "SELECT VALUE record::id(id) FROM c ORDER BY id").await;
	let ids = outcome(&read, &ctx).await.expect("the transaction reads the table");
	ctx.txn().cancel().await.expect("the transaction ends");
	let ids = ids
		.into_iter()
		.map(|id| {
			let Value::Number(Number::Int(id)) = id else {
				panic!("an id that is no integer: {id:?}");
			};
			id
		})
		.collect();
	(answer, ids)
}

/// A failed `CREATE` over a range holds, in its own transaction, the records of the ids before the
/// failing one and none of the ids past it, whatever width its compute half runs at: the apply
/// half takes the failing id's error at its position, and no id after it is applied.
async fn failed_create_holds_no_id_past_its_failure(
	serial: &TestDb,
	wide: &TestDb,
	sql: &str,
	fails_at: i64,
	existing: &[i64],
) {
	let mark = Mark::of(wide).await;
	let (expected, expected_ids) = ids_held_after(serial, sql).await;
	assert!(expected.is_err(), "`{sql}` fails serially");
	let (got, got_ids) = ids_held_after(wide, sql).await;
	assert_eq!(got, expected, "`{sql}`");
	assert_eq!(got_ids, expected_ids, "the ids `{sql}` leaves in its transaction");
	assert!((1..fails_at).all(|id| got_ids.contains(&id)), "every id before {fails_at} is held");
	assert!(
		got_ids.iter().all(|id| *id <= fails_at || existing.contains(id)),
		"an id past {fails_at} is held: {got_ids:?}"
	);
	mark.fanned_out(wide, sql).await;
}

/// The failure of a record that exists, and of a failed assert, on the in-memory backend.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_create_leaves_no_id_past_its_failure_in_its_transaction() {
	let _turn = TURNS.lock().await;
	let existing = format!("{CREATES} CREATE c:900 SET m = 0; CREATE c:2400 SET m = 0;");
	let serial = TestDb::new_with_config(&existing, whole()).await;
	let wide = TestDb::new_with_config(&existing, tiny()).await;
	failed_create_holds_no_id_past_its_failure(
		&serial,
		&wide,
		"CREATE |c:1..3000| RETURN NONE",
		900,
		&[2400],
	)
	.await;
	let asserted = "DEFINE TABLE c;
		DEFINE FIELD n ON c TYPE int VALUE record::id(id) ASSERT $value != 1300 AND $value != 2600;";
	let serial = TestDb::new_with_config(asserted, whole()).await;
	let wide = TestDb::new_with_config(asserted, tiny()).await;
	failed_create_holds_no_id_past_its_failure(
		&serial,
		&wide,
		"CREATE |c:1..3000| RETURN NONE",
		1300,
		&[],
	)
	.await;
}

/// The same on SurrealKV.
#[cfg(feature = "kv-surrealkv")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_create_leaves_no_id_past_its_failure_in_its_transaction_on_surrealkv() {
	let _turn = TURNS.lock().await;
	let dirs: Vec<_> = (0..4).map(|_| tempfile::tempdir().expect("temporary directory")).collect();
	let existing = format!("{CREATES} CREATE c:900 SET m = 0; CREATE c:2400 SET m = 0;");
	let serial = TestDb::new_on_surrealkv(&existing, dirs[0].path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(&existing, dirs[1].path(), tiny()).await;
	failed_create_holds_no_id_past_its_failure(
		&serial,
		&wide,
		"CREATE |c:1..3000| RETURN NONE",
		900,
		&[2400],
	)
	.await;
	let asserted = "DEFINE TABLE c;
		DEFINE FIELD n ON c TYPE int VALUE record::id(id) ASSERT $value != 1300 AND $value != 2600;";
	let serial = TestDb::new_on_surrealkv(asserted, dirs[2].path(), whole()).await;
	let wide = TestDb::new_on_surrealkv(asserted, dirs[3].path(), tiny()).await;
	failed_create_holds_no_id_past_its_failure(
		&serial,
		&wide,
		"CREATE |c:1..3000| RETURN NONE",
		1300,
		&[],
	)
	.await;
}

/// A range that names no id writes nothing, and `ONLY` over a range that names several fails, at
/// every width, as the serial run does and with the records it leaves.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_range_of_no_ids_and_an_only_over_several_answer_as_the_serial_run() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(CREATES, whole()).await;
	let wide = TestDb::new_with_config(CREATES, tiny()).await;
	let statements = [
		"CREATE |c:5..5| RETURN NONE",
		"CREATE |c:9..5| RETURN id",
		"CREATE |c:6..=6| RETURN id",
		"CREATE ONLY |c:100..103|",
		"CREATE ONLY |c:1000..2000|",
		"CREATE ONLY |c:200..201|",
	];
	let observe = ["SELECT count() FROM c GROUP ALL"];
	let expected = answers_of(&serial, "c", &statements, &observe).await;
	let got = answers_of(&wide, "c", &statements, &observe).await;
	assert_eq!(got, expected);
	assert_eq!(expected[0], Ok(vec![]), "a range of no id writes no record");
	assert_eq!(expected[3], Ok(vec![]), "a reversed range writes no record");
	assert_eq!(
		expected.iter().filter(|answer| answer.is_err()).count(),
		2,
		"the two ONLY statements over several ids fail: {expected:?}"
	);
}

/// A record user creates into a table through its create permission, judged for each id by the
/// compute half with the session of the statement: the ids it may not create are left out, a table
/// that allows none creates none, and the answers and records are the serial run's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_users_create_permission_is_judged_for_each_id_of_a_range() {
	let _turn = TURNS.lock().await;
	let setup = "DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		CREATE user:alice;
		DEFINE TABLE c SCHEMALESS PERMISSIONS FOR select FULL, FOR create WHERE record::id(id) > 1200;
		DEFINE TABLE d SCHEMALESS PERMISSIONS FOR select FULL, FOR create NONE;
		DEFINE TABLE e SCHEMALESS PERMISSIONS FULL;";
	let session = || {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "alice"));
		crate::dbs::Session::for_record("test", "test", "user", rid)
	};
	let serial = TestDb::new_with_auth_and_config(setup, whole()).await;
	let wide = TestDb::new_with_auth_and_config(setup, tiny()).await;
	let run = async |db: &TestDb, sql: &str| -> Vec<Result<Value, String>> {
		db.datastore()
			.execute(sql, &session(), None)
			.await
			.expect("runs")
			.into_iter()
			.map(|response| match response.result {
				Ok(value) => Ok(Value::from(value)),
				Err(e) => Err(e.message().to_owned()),
			})
			.collect()
	};
	let mark = Mark::of(&wide).await;
	for sql in [
		"CREATE |c:1..3000| RETURN NONE",
		"SELECT count() FROM c GROUP ALL",
		"CREATE |c:1201..1500| RETURN NONE",
		"CREATE |d:1..3000| RETURN NONE",
		"SELECT count() FROM d GROUP ALL",
		"CREATE |e:1..3000| RETURN id",
		"SELECT count() FROM e GROUP ALL",
	] {
		assert_eq!(run(&wide, sql).await, run(&serial, sql).await, "`{sql}`");
	}
	assert_eq!(
		answered(&wide, "SELECT count() FROM c GROUP ALL").await,
		answered(&serial, "SELECT count() FROM c GROUP ALL").await
	);
	mark.fanned_out(&wide, "the creates of a record user").await;
}

/// A write over a range of ids that is no `CREATE` is not cut: it runs serially, and answers what
/// the serial run answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_over_a_range_that_is_no_create_runs_serially() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config(CREATES, whole()).await;
	let wide = TestDb::new_with_config(CREATES, tiny()).await;
	for sql in [
		"UPDATE |c:1..1500| SET m = 1 RETURN NONE",
		"UPSERT |c:2000..3500| SET m = 2 RETURN NONE",
		"DELETE |c:1..1500| RETURN NONE",
	] {
		assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
		assert_eq!(
			answers_of(&wide, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await,
			answers_of(&serial, "c", &[sql], &["SELECT count() FROM c GROUP ALL"]).await,
			"`{sql}`"
		);
	}
}

/// The `CREATE` over a count of the placement corpus is placed serially and leaves the records the
/// serial run leaves: three, each under an id of the table. Its ids are random, so the records are
/// compared by what they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_count_create_of_the_placement_corpus_answers_as_the_serial_run() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config("DEFINE TABLE person;", whole()).await;
	let wide = TestDb::new_with_config("DEFINE TABLE person;", tiny()).await;
	let sql = "CREATE |person:3|";
	assert!(!fans_out(&verdicts(&wide, sql).await), "`{sql}` stays serial");
	for db in [&serial, &wide] {
		let created = answered(db, sql).await.expect("the statement answers");
		assert_eq!(created.len(), 3);
	}
	let sql = "SELECT count() FROM person WHERE type::is_record(id) AND record::tb(id) = 'person' GROUP ALL";
	assert_eq!(answered(&wide, sql).await, answered(&serial, sql).await);
}

/// The `CREATE` over a range of the placement corpus is placed as an ordered apply over batches and
/// answers what the serial run answers, with the records it leaves. Its three ids fit one batch,
/// so no batch goes to a worker: the cases over larger ranges show the hand-off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_range_create_of_the_placement_corpus_answers_as_the_serial_run() {
	let _turn = TURNS.lock().await;
	let serial = TestDb::new_with_config("DEFINE TABLE person;", whole()).await;
	let wide = TestDb::new_with_config("DEFINE TABLE person;", tiny()).await;
	same_answers_over_table(
		&serial,
		&wide,
		"person",
		&[("CREATE |person:1..4|", true)],
		&["SELECT count() FROM person GROUP ALL"],
	)
	.await;
}

/// A record a write creates in the table it reads, sorting after every record the write read
/// before creating it, is never visited by the write. Each shape here creates the record from the
/// apply half of an earlier record: its `RETURN` clause, directly, through a function, or through
/// the event of another table a nested write triggers. The judgement of whether a write fans out
/// reads the effects of the write's `RETURN` clause, which writes, so each shape runs serially, and
/// the serial run's gate passes over the record. A record whose compute half would fail is passed
/// over before anything of it runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_record_the_write_creates_in_the_table_it_reads_is_never_visited() {
	let _turn = TURNS.lock().await;
	let spawn = "(CREATE t:99999 SET n = 99999).id";
	let event_on_u = "DEFINE TABLE u; CREATE u:1; \
		DEFINE EVENT spawn ON u WHEN $event = 'UPDATE' THEN { CREATE t:99999 SET n = 99999 };";
	let shapes: [(&str, String, String, &str); 6] = [
		(
			"a RETURN subquery",
			String::new(),
			format!("UPDATE t SET touched = true RETURN (IF id = t:1 {{ {spawn} }} ELSE {{ NONE }})"),
			"SELECT VALUE touched FROM t:99999",
		),
		(
			"a DELETE whose RETURN subquery creates",
			String::new(),
			format!("DELETE t RETURN (IF $before.id = t:1 {{ {spawn} }} ELSE {{ NONE }})"),
			"SELECT VALUE id FROM t:99999",
		),
		(
			"a function its RETURN calls",
			"DEFINE FUNCTION fn::spawn($id: any) { IF $id = t:1 { CREATE t:99999 SET n = 99999 }; RETURN 1; };".to_owned(),
			"UPDATE t SET touched = true RETURN fn::spawn(id)".to_owned(),
			"SELECT VALUE touched FROM t:99999",
		),
		(
			"the event of another table a nested write triggers",
			event_on_u.to_owned(),
			"UPDATE t SET touched = true RETURN (IF id = t:1 { (UPDATE u:1 SET y = 1).id } ELSE { NONE })".to_owned(),
			"SELECT VALUE touched FROM t:99999",
		),
		(
			"the event of another table a function's write triggers",
			format!(
				"{event_on_u} DEFINE FUNCTION fn::poke($id: any) {{ IF $id = t:1 {{ UPDATE u:1 SET y = 1 }}; RETURN 1; }};"
			),
			"UPDATE t SET touched = true RETURN fn::poke(id)".to_owned(),
			"SELECT VALUE touched FROM t:99999",
		),
		(
			"a record whose compute half would fail",
			String::new(),
			format!(
				"UPDATE t SET touched = true, k = <int> (IF n > 5000 {{ 'x' }} ELSE {{ n }}) \
				 RETURN (IF id = t:1 {{ {spawn} }} ELSE {{ NONE }})"
			),
			"SELECT VALUE touched FROM t:99999",
		),
	];
	for (what, extra, sql, check) in &shapes {
		let schema = format!("{} {extra}", setup());
		let serial = TestDb::new_with_config(&schema, whole()).await;
		let wide = TestDb::new_with_config(&schema, tiny()).await;
		same_answers_over(&serial, &wide, &[(sql.as_str(), false)], &[check]).await;
		// The created record is left as its creation left it: unwritten by the statement, and not
		// deleted by a `DELETE`.
		let untouched = if check.contains("touched") {
			"RETURN NONE"
		} else {
			"RETURN t:99999"
		};
		assert_eq!(
			answered(&wide, check).await,
			answered(&wide, untouched).await,
			"{what}: the write visited the record it created"
		);
	}
}
