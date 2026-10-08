//! Graph and reference lookups whose last hop has clauses — a projection,
//! `SPLIT`, `GROUP`, `ORDER`, `LIMIT`, `START` — run as the statement those
//! clauses make over the records the hop reaches, and answer their values, for
//! the owner and for a record user whose reads are checked.

use super::golden::Golden;
use super::tests::{bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	DEFINE TABLE person PERMISSIONS FOR select WHERE hidden != true;
	DEFINE TABLE knows TYPE RELATION PERMISSIONS FOR select WHERE secret != true;
	DEFINE FIELD note ON knows PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE TABLE post PERMISSIONS FULL;
	DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE;
	CREATE person:a SET name = 'a', n = 1;
	CREATE person:b SET name = 'b', n = 2;
	CREATE person:c SET name = 'c', n = 3;
	CREATE person:h SET name = 'h', hidden = true;
	RELATE person:a->knows:k1->person:b SET since = 2020, note = 'n1', tag = 'x';
	RELATE person:a->knows:k2->person:h SET since = 2021, tag = 'y';
	RELATE person:a->knows:k3->person:c SET since = 1990, tag = 'x';
	RELATE person:a->knows:k4->person:b SET since = 2000, secret = true, tag = 'x';
	RELATE person:b->knows:k5->person:c SET since = 2010, tag = 'z';
	CREATE post:1 SET author = person:a, n = 5;
	CREATE post:2 SET author = person:a, n = 1;
	CREATE post:3 SET author = person:b, n = 3;
";

/// Lookups whose clauses run as a statement: a hop with clauses taken per
/// element of a path runs as one over the element it is taken from, `ONLY` on
/// a hop before it unwraps what the path answers, and a hop with clauses
/// whose base is another hop with clauses runs as a statement over the
/// statement the base makes.
const LOOKUPS: &[&str] = &[
	"SELECT VALUE ->(SELECT since FROM knows) FROM person:a",
	"SELECT VALUE ->(SELECT since, note FROM knows WHERE since > 1995) FROM person:a",
	"SELECT VALUE ->(SELECT * FROM knows ORDER BY since DESC) FROM person:a",
	"SELECT VALUE ->(SELECT * FROM knows ORDER BY since LIMIT 2) FROM person:a",
	"SELECT VALUE ->(SELECT * FROM knows LIMIT 1 START 1) FROM person:a",
	"SELECT VALUE ->(SELECT * FROM knows WHERE tag = 'x' ORDER BY since LIMIT 1) FROM person:a",
	"SELECT VALUE ->(SELECT tag, count() AS c FROM knows GROUP BY tag) FROM person:a",
	"SELECT VALUE ->(SELECT count() FROM knows GROUP ALL) FROM person",
	"SELECT VALUE ->(SELECT * FROM knows SPLIT tag) FROM person:a",
	"SELECT VALUE ->(SELECT VALUE id FROM knows ORDER BY since LIMIT 2) FROM person:a",
	"SELECT VALUE ->(SELECT id FROM knows ORDER BY since LIMIT 2) FROM person:a",
	"SELECT VALUE ->(SELECT VALUE out FROM knows ORDER BY since) FROM person:a",
	"SELECT VALUE ->(SELECT VALUE since FROM ONLY knows LIMIT 1) FROM person:a",
	"SELECT VALUE ->(SELECT since FROM knows WHERE since > $parent.n * 2000) FROM person",
	"SELECT id, ->(SELECT * FROM knows ORDER BY since DESC LIMIT 1)->person.name AS latest FROM person ORDER BY id",
	"SELECT VALUE ->knows->(SELECT * FROM person ORDER BY n DESC) FROM person:a",
	"SELECT VALUE <~(SELECT n FROM post ORDER BY n) FROM person:a",
	"SELECT VALUE <~(SELECT * FROM post LIMIT 1) FROM person",
	"RETURN person:a->(SELECT * FROM knows ORDER BY since LIMIT 2)",
	"SELECT VALUE ->(SELECT * FROM knows) FROM person:a",
	"SELECT VALUE ->(SELECT VALUE id FROM ONLY knows LIMIT 1)->person FROM ONLY person:a",
	"SELECT VALUE ->knows->(SELECT VALUE id FROM ONLY person LIMIT 1) FROM ONLY person:a",
	"SELECT VALUE ->(SELECT VALUE id FROM ONLY knows LIMIT 1)->(SELECT VALUE id FROM ONLY person LIMIT 1) FROM ONLY person:a",
	"SELECT VALUE array::len(->knows->person->(SELECT VALUE out FROM knows LIMIT 1)) FROM ONLY person:a",
	"SELECT VALUE ->(SELECT * FROM knows WHERE since > 2000) FROM person:a",
	"SELECT VALUE ->(SELECT * FROM knows)->person FROM person:a",
	"SELECT VALUE <~(SELECT * FROM post) FROM person:a",
	"SELECT VALUE ->(SELECT * FROM knows LIMIT $limit) FROM person:a",
];

#[tokio::test]
async fn lookups_with_clauses_answer_their_values() {
	let mut golden = Golden::new("lookups_with_clauses_answer_their_values");
	let db = TestDb::new_with_auth(SETUP).await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		let ctx = ctx.with_param("limit", crate::val::Value::from(2));
		for sql in LOOKUPS {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}

/// Lookups with clauses over many origins. Run with `cargo test --release -p surrealdb-core --lib
/// expr2_lookup_clause_throughput -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn expr2_lookup_clause_throughput() {
	let db = TestDb::new_with_auth(
		"DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
		 CREATE |person:1..2000| SET n = rand::int(1, 100);
		 FOR $i IN 1..=2000 { LET $a = type::record('person', $i); FOR $j IN 1..=5 { LET $b = type::record('person', rand::int(1, 2000)); RELATE $a->knows->$b SET since = rand::int(1990, 2024); }; };",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT ->(SELECT * FROM knows ORDER BY since LIMIT 1) AS k FROM person",
		"SELECT ->(SELECT since FROM knows WHERE since > 2000) AS k FROM person",
		"SELECT ->(SELECT count() FROM knows GROUP ALL) AS k FROM person",
	] {
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
		let mut best = std::time::Duration::MAX;
		let mut answers = Vec::new();
		for _ in 0..7 {
			let started = web_time::Instant::now();
			answers.push(outcome(&op, &ctx).await);
			best = best.min(started.elapsed());
		}
		eprintln!("{sql}: {best:?}");
		assert!(answers.windows(2).all(|w| w[0] == w[1]), "{sql}");
	}
}

/// A lookup with clauses run over and over on the VM, for a profiler to
/// sample.
#[tokio::test]
#[ignore = "a profiling load, not a check"]
async fn expr2_lookup_clause_profile() {
	let db = TestDb::new_with_auth(
		"DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
		 CREATE |person:1..2000| SET n = rand::int(1, 100);
		 FOR $i IN 1..=2000 { LET $a = type::record('person', $i); FOR $j IN 1..=5 { LET $b = type::record('person', rand::int(1, 2000)); RELATE $a->knows->$b SET since = rand::int(1990, 2024); }; };",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let sql = std::env::var("EXPR2_SQL").unwrap_or_else(|_| {
		"SELECT ->(SELECT since FROM knows WHERE since > 2000) AS k FROM person".to_owned()
	});
	let sql = sql.as_str();
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("the VM runs it");
	eprintln!("PROFILE READY");
	let started = web_time::Instant::now();
	while started.elapsed() < std::time::Duration::from_secs(20) {
		outcome(&op, &ctx).await.expect("runs");
	}
}

/// Graph and reference lookups over a range of their edges' or referencing
/// records' keys, each bound evaluated with no row when the hop is taken, are
/// lookups of the program and answer their values.
#[tokio::test]
async fn ranged_lookups_are_instructions_and_answer_their_values() {
	let mut golden = Golden::new("ranged_lookups_are_instructions_and_answer_their_values");
	const RANGED_SETUP: &str = "
		RELATE a:1->edge:1->a:2;
		RELATE a:1->edge:2->a:2;
		RELATE a:1->edge:3->a:3;
		RELATE b:1->ce:[5]->a:2;
		RELATE b:1->ce:[6]->a:2;
		RELATE b:1->ce:[7]->a:3;
		DEFINE FIELD chat ON message TYPE option<record<chat>> REFERENCE;
		CREATE chat:bla;
		CREATE message:1 SET chat = chat:bla;
		CREATE message:2 SET chat = chat:bla;
		CREATE message:3 SET chat = chat:bla;
		DEFINE PARAM $num VALUE 9;
	";
	const RANGED: &[&str] = &[
		"a:1->edge:1..=2",
		"a:1->edge:2..",
		"b:1->ce:[6]..=[$num - 2]",
		"b:1->ce:[6]..",
		"a:1->edge:1..=2->a",
		"a:1->(SELECT id FROM edge:1..=2).*",
		"a:1->(SELECT id FROM edge:..=3 START 1 LIMIT 1).*",
		"chat:bla<~(message:1..=2 FIELD chat)",
		"chat:bla<~(message:1>.. FIELD chat).id",
	];
	let db = TestDb::new(RANGED_SETUP).await;
	for sql in RANGED {
		let ctx = db.exec_ctx().await;
		let bound = bind(&ctx, sql).await;
		let vm = super::plan(&planner(&ctx), &bound).await.expect("the VM covers the lookup");
		golden.rows(sql, &outcome(&vm, &ctx).await);
	}
	golden.check();
}
