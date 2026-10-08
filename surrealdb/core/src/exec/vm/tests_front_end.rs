//! A plan a front end emits rather than a statement — a GQL `MATCH`, a
//! PostgreSQL or MySQL `SELECT` — runs as a VM program and answers what the
//! operator tree compiled from the same plan answers. A plan the VM does not
//! cover fails; nothing else compiles it.

use surrealdb_types::ToSql;

use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;
use crate::kvs::request::{Dialect, QueryRequest, QuerySource};

const SETUP: &str = "
	CREATE person:1 SET name = 'A', city = 'London' RETURN NONE;
	CREATE person:2 SET name = 'B', city = 'Paris' RETURN NONE;
	CREATE person:3 SET name = 'C', city = 'London' RETURN NONE;
	INSERT RELATION INTO knows [
		{ id: knows:k12, in: person:1, out: person:2 },
		{ id: knows:k23, in: person:2, out: person:3 }
	] RETURN NONE;
	CREATE users:1 SET user_id = 1, name = 'Alice' RETURN NONE;
	CREATE users:2 SET user_id = 2, name = 'Bob' RETURN NONE;
	CREATE orders:1 SET user_id = 1, total = 100 RETURN NONE;
	CREATE orders:2 SET user_id = 1, total = 200 RETURN NONE;
	CREATE orders:3 SET user_id = 2, total = 150 RETURN NONE;
";

/// What `text`, in `dialect`, answers over `setup` for
/// `session`, with how many programs the VM ran for it. Running the
/// plan compiles nothing once its program has started.
fn run(
	setup: &str,
	dialect: Dialect,
	text: &str,
	session: &Session,
) -> (Vec<Result<String, String>>, usize) {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let db = TestDb::new(setup).await;
		let before = crate::exec::compiles::programs();
		let compiled = crate::exec::compiles::count();
		let results = db
			.datastore()
			.run(QueryRequest::new(QuerySource::in_dialect(text, dialect), session))
			.await
			.expect("the query runs");
		assert_eq!(
			crate::exec::compiles::count(),
			compiled,
			"`{text}` compiled while a program ran"
		);
		let answers = results
			.into_iter()
			.map(|result| result.result.map(|value| value.to_sql()).map_err(|e| e.to_string()))
			.collect();
		(answers, crate::exec::compiles::programs() - before)
	})
}

#[cfg(feature = "gql")]
#[test]
fn a_match_runs_on_the_vm() {
	let (answers, planned) = run(
		SETUP,
		Dialect::Gql,
		"MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a, b.name AS b ORDER BY a",
		&TestDb::owner(),
	);
	assert_eq!(answers, vec![Ok("[{ a: 'A', b: 'B' }, { a: 'B', b: 'C' }]".to_owned())]);
	assert_eq!(planned, 1);
}

#[cfg(feature = "postgres")]
#[test]
fn a_join_aggregate_and_union_run_on_the_vm() {
	for (text, expected) in [
		(
			"SELECT u.name, o.total FROM users u JOIN orders o ON u.user_id = o.user_id \
			 WHERE o.total >= 150 ORDER BY o.total ASC",
			"[{ name: 'Bob', total: 150 }, { name: 'Alice', total: 200 }]",
		),
		(
			"SELECT u.name, SUM(o.total) AS spent FROM users u JOIN orders o \
			 ON u.user_id = o.user_id GROUP BY u.name HAVING SUM(o.total) > 200 ORDER BY u.name",
			"[{ name: 'Alice', spent: 300 }]",
		),
		(
			"SELECT user_id FROM orders UNION SELECT user_id FROM users ORDER BY user_id",
			"[{ user_id: 1 }, { user_id: 2 }]",
		),
	] {
		let (answers, planned) = run(SETUP, Dialect::PgSql, text, &TestDb::owner());
		assert_eq!(answers, vec![Ok(expected.to_owned())], "`{text}`");
		assert_eq!(planned, 1, "`{text}`");
	}
}

/// The GQL queries a MATCH runs natively, each answered as the operator tree
/// answers it: the rows of the bare tree compiled from the plan are the rows
/// the program answers, in order.
#[cfg(feature = "gql")]
#[tokio::test(flavor = "multi_thread")]
async fn a_native_match_answers_as_the_operator_tree_does() {
	use crate::exec::operators::test_util::try_collect;

	let db = TestDb::new(&format!(
		"{SETUP}
		 CREATE person:4 SET name = 'D', city = 'Paris' RETURN NONE;
		 CREATE person:5 SET name = 'E' RETURN NONE;
		 INSERT RELATION INTO knows [
			{{ id: knows:k34, in: person:3, out: person:4, since: 2020 }},
			{{ id: knows:k31, in: person:3, out: person:1, since: 2021 }},
			{{ id: knows:k14, in: person:1, out: person:4, since: 2019 }}
		] RETURN NONE;
		 CREATE company:1 SET name = 'X' RETURN NONE;
		 INSERT RELATION INTO works_at [
			{{ id: works_at:w1, in: person:1, out: company:1 }},
			{{ id: works_at:w2, in: person:2, out: company:1 }}
		] RETURN NONE;"
	))
	.await;
	let ctx = db.exec_ctx().await;
	let planner = super::tests::planner(&ctx);
	let queries = [
		"MATCH (n:person) RETURN n.name AS name ORDER BY name",
		"MATCH (n:person) WHERE n.city = 'London' RETURN n.name AS name ORDER BY name DESC",
		"MATCH (n:person) RETURN n.name AS name ORDER BY name SKIP 1 LIMIT 2",
		"MATCH (n:person) RETURN DISTINCT n.city AS city ORDER BY city",
		"MATCH (n:person) RETURN n.city AS city, count(*) AS c GROUP BY n.city ORDER BY city",
		"MATCH (n:person) RETURN count(*) AS c",
		"MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS name, count(*) AS c GROUP BY a ORDER BY name",
		"MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH (a:person)<-[:knows]-(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH (a:person)-[k:knows]->(b:person) WHERE k.since > 2019 RETURN a.name AS a, b.name AS b, k.since AS s ORDER BY s",
		"MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN a.name AS a, c.name AS c ORDER BY a, c",
		"MATCH (a:person)-[:knows]->(b:person), (b)-[:works_at]->(c:company) RETURN a.name AS a, c.name AS c ORDER BY a",
		"MATCH (a:person)-[:knows]->(b), (a)-[:works_at]->(c:company) RETURN a.name AS a, b.name AS b, c.name AS c ORDER BY a, b",
		"MATCH (a:person)-[:knows]->(b:person) MATCH (b)-[:knows]->(c:person) RETURN a.name AS a, c.name AS c ORDER BY a, c",
		"MATCH (a:person), (c:company) RETURN a.name AS a, c.name AS c ORDER BY a",
		"MATCH (a:person) OPTIONAL MATCH (a)-[:works_at]->(c:company) RETURN a.name AS a, c.name AS c ORDER BY a",
		"MATCH (a:person) OPTIONAL MATCH (a)-[:knows]->(b:person) RETURN a.name AS a, count(b) AS n GROUP BY a.name ORDER BY a",
		"MATCH ()-[k:knows]->() RETURN k.since AS since ORDER BY since",
		"MATCH (a:person)-[k:knows]->(b:person) RETURN a, k, b ORDER BY a.name, b.name",
		"MATCH (a:person)-[:knows]->(b:person)<-[:knows]-(c:person) RETURN a.name AS a, c.name AS c ORDER BY a, c",
		"MATCH (a:person)-[:knows]->{1,2}(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH (a:person)-[:knows]->+(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH (a:person)-[:knows]->*(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH (a:person)-[g:knows]->{2}(b:person) RETURN a.name AS a, b.name AS b, g AS grp ORDER BY a, b",
		"MATCH p = (a:person)-[:knows]->{1,3}(b:person) RETURN a.name AS a, p AS pth ORDER BY a, pth",
		"MATCH (b:person WHERE b.name = 'C'), p = (a)-[:knows]->+(b) RETURN a.name AS a, p AS pth ORDER BY a",
		"MATCH ALL SHORTEST (a:person WHERE a.name = 'A')-[:knows]->+(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH ANY SHORTEST (a:person WHERE a.name = 'A')-[:knows]->+(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH SHORTEST 2 (a:person)-[:knows]->+(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH SIMPLE (a:person)-[:knows]->+(b:person) RETURN a.name AS a, b.name AS b ORDER BY a, b",
		"MATCH (a:person)-[:knows]->{1,2}(b:person) RETURN a.name AS a, b.name AS b LIMIT 2",
	];
	for query in queries {
		let plan = crate::gql::parse_with_capabilities(
			query,
			&crate::dbs::Capabilities::all(),
			&db.datastore().parser_config(),
		)
		.unwrap_or_else(|e| panic!("`{query}` does not parse: {e:?}"))
		.0;
		let tree = planner.plan_logical(plan.clone()).await.expect("the tree plans");
		let expected = try_collect(&tree, &ctx).await.expect("the tree runs");
		let before = super::NATIVE_MATCHES.with(|n| n.get());
		let vm = super::plan_front_end(&planner, plan).await.expect("the VM plans");
		let native = super::NATIVE_MATCHES.with(|n| n.get()) - before;
		assert_eq!(native, 1, "`{query}` did not run natively");
		let answered = try_collect(&vm, &ctx).await.expect("the program runs");
		assert_eq!(answered, expected, "`{query}`");
	}
}

/// The state a write leaves, as the tables a `MATCH` reads hold it.
async fn tables_after(ctx: &crate::exec::ExecutionContext) -> Vec<crate::val::Value> {
	use crate::exec::operators::test_util::{collect, plan_statement};

	let mut state = Vec::new();
	for table in [
		"SELECT name, city, seen FROM person ORDER BY name",
		"SELECT in.name AS a, out.name AS b FROM knows ORDER BY a, b",
		"SELECT in.name AS a, out.name AS b FROM likes ORDER BY a, b",
	] {
		state.extend(collect(&plan_statement(table, ctx).await, ctx).await);
	}
	state
}

/// The GQL writes a MATCH runs natively, each answered as the operator tree
/// answers it, over a database of its own: the rows handed on and the state
/// the write leaves are those of the bare tree.
#[cfg(feature = "gql")]
#[tokio::test(flavor = "multi_thread")]
async fn a_native_write_answers_and_leaves_what_the_operator_tree_does() {
	use crate::exec::operators::test_util::try_collect;
	use crate::kvs::TransactionType;

	let setup = format!(
		"{SETUP}
		 DEFINE TABLE likes TYPE RELATION IN person OUT person;
		 CREATE person:4 SET name = 'D', city = 'Paris' RETURN NONE;
		 INSERT RELATION INTO knows [
			{{ id: knows:k34, in: person:3, out: person:4 }}
		 ] RETURN NONE;"
	);
	let queries = [
		"MATCH (n:person WHERE n.name = 'A') SET n.seen = true RETURN n.name AS name, n.seen AS seen",
		"MATCH (n:person WHERE n.city = 'London') SET n.seen = true",
		"MATCH (n:person WHERE n.name = 'A') SET n = { name: 'A2' } RETURN n.name AS name",
		"MATCH (n:person WHERE n.name = 'A') REMOVE n.city RETURN n.name AS name",
		"MATCH (n:person WHERE n.name = 'D') DETACH DELETE n",
		"MATCH (a:person)-[k:knows]->(b:person) DELETE k RETURN a.name AS a ORDER BY a",
		"MATCH (a:person WHERE a.name = 'A') MATCH (b:person WHERE b.name = 'C') INSERT (a)-[:likes]->(b) RETURN a.name AS src, b.name AS dst",
		"INSERT (p:person {name: 'Z', city: 'Rome'}) RETURN p.name AS name",
		"MATCH (n:person) SET n.seen = true RETURN n.name AS name ORDER BY name",
		"MATCH (n:person WHERE n.name = 'C') SET n.seen = true MATCH (m:person WHERE m.seen = true) RETURN m.name AS name ORDER BY name",
		"MATCH (n:person WHERE n.name = 'C') SET n.seen = true OPTIONAL MATCH (m:person WHERE m.seen = true) RETURN n.name AS n, m.name AS m",
		"MATCH (n:person WHERE n.name = 'C') SET n.seen = true MATCH (m:person)-[:knows]->(o:person) RETURN m.name AS m, o.name AS o, n.seen AS seen ORDER BY m",
	];
	for query in queries {
		let mut answers = Vec::new();
		for native in [false, true] {
			let db = TestDb::new(&setup).await;
			let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
			let planner = super::tests::planner(&ctx);
			let plan = crate::gql::parse_with_capabilities(
				query,
				&crate::dbs::Capabilities::all(),
				&db.datastore().parser_config(),
			)
			.unwrap_or_else(|e| panic!("`{query}` does not parse: {e:?}"))
			.0;
			let op = if native {
				let before = super::NATIVE_MATCHES.with(|n| n.get());
				let op = super::plan_front_end(&planner, plan).await.expect("the VM plans");
				let planned = super::NATIVE_MATCHES.with(|n| n.get()) - before;
				assert_eq!(planned, 1, "`{query}` did not run natively");
				op
			} else {
				planner.plan_logical(plan).await.expect("the tree plans")
			};
			let rows = try_collect(&op, &ctx).await.expect("the plan runs");
			answers.push((rows, tables_after(&ctx).await));
		}
		assert_eq!(answers[0], answers[1], "`{query}`");
	}
}

/// Wall-clock cost of running a front end's plan, over a graph large enough
/// for the per-row cost of the engine to dominate: the rows of a `MATCH`
/// handed on by the bare operator tree and by the VM program that runs the
/// plan, run alternately in one process so a drift of the machine moves both.
///
/// Not part of the suite. Run with
/// `cargo test -p surrealdb-core --lib -- --ignored --nocapture match_throughput`,
/// with `MATCH_ROWS` (default 50000), `MATCH_ROUNDS` (default 7) and
/// `MATCH_QUERY` (`scan` or `expand`) to change what it measures.
#[cfg(feature = "gql")]
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn match_throughput() {
	use std::time::Instant;

	use crate::exec::operators::test_util::try_collect;

	let rows: usize =
		std::env::var("MATCH_ROWS").ok().and_then(|v| v.parse().ok()).unwrap_or(50_000);
	let rounds: usize =
		std::env::var("MATCH_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(7);
	let (query, edges) = match std::env::var("MATCH_QUERY").as_deref() {
		Ok("expand") => (
			"MATCH (a:person)-[:knows]->(b:person) RETURN a.name AS a, b.name AS b",
			format!(
				"FOR $i IN 0..{rows} {{
					RELATE (type::record('person', $i))->knows->(type::record('person', ($i + 1) % {rows})) RETURN NONE
				 }};"
			),
		),
		_ => ("MATCH (n:person) RETURN n.name AS name, n.age AS age", String::new()),
	};
	let db = TestDb::new(&format!(
		"DEFINE TABLE person SCHEMALESS;
		 DEFINE TABLE knows TYPE RELATION IN person OUT person;
		 FOR $i IN 0..{rows} {{
			CREATE type::record('person', $i) SET name = 'p' + <string> $i, age = $i % 90 RETURN NONE
		 }};
		 {edges}"
	))
	.await;
	let ctx = db.exec_ctx().await;
	let plan = crate::gql::parse_with_capabilities(
		query,
		&crate::dbs::Capabilities::all(),
		&db.datastore().parser_config(),
	)
	.expect("the query parses")
	.0;
	let planner = super::tests::planner(&ctx);
	let mut bare: Vec<f64> = Vec::new();
	let mut vm: Vec<f64> = Vec::new();
	for round in 0..rounds {
		// Alternate which runs first, so neither owns the warm cache.
		for variant in [round % 2, 1 - round % 2] {
			let op = if variant == 0 {
				planner.plan_logical(plan.clone()).await.expect("the tree plans")
			} else {
				super::plan_front_end(&planner, plan.clone()).await.expect("the VM plans")
			};
			let started = Instant::now();
			let answered = try_collect(&op, &ctx).await.expect("the plan runs");
			let took = started.elapsed().as_secs_f64() * 1000.0;
			assert_eq!(answered.len(), rows);
			if variant == 0 {
				bare.push(took);
			} else {
				vm.push(took);
			}
		}
	}
	let median = |samples: &mut Vec<f64>| {
		samples.sort_by(|a, b| a.total_cmp(b));
		samples[samples.len() / 2]
	};
	let (mut bare_sorted, mut vm_sorted) = (bare.clone(), vm.clone());
	println!("match_throughput rows={rows} query={query}");
	println!("  bare tree ms: {bare:.1?}  median {:.1}", median(&mut bare_sorted));
	println!("  VM        ms: {vm:.1?}  median {:.1}", median(&mut vm_sorted));
}

/// Every shape of a relational plan the SQL front ends emit — a scan, a join
/// of either match, an aggregate with a `HAVING`, `DISTINCT`, a union, a
/// subquery and a limit over them — is planned as one VM program.
#[cfg(feature = "postgres")]
#[test]
fn every_shape_of_a_sql_plan_is_one_vm_program() {
	for text in [
		"SELECT name FROM users ORDER BY name",
		"SELECT u.name, o.total FROM users u JOIN orders o ON u.user_id = o.user_id",
		"SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.user_id = o.user_id",
		"SELECT u.name, o.total FROM users u JOIN orders o ON o.total > u.user_id * 90",
		"SELECT u.name FROM users u CROSS JOIN orders o",
		"SELECT u.name, SUM(o.total) AS spent FROM users u JOIN orders o \
		 ON u.user_id = o.user_id GROUP BY u.name HAVING SUM(o.total) > 100",
		"SELECT DISTINCT user_id FROM orders",
		"SELECT user_id FROM orders UNION ALL SELECT user_id FROM users",
		"SELECT user_id FROM orders UNION SELECT user_id FROM users",
		"SELECT s.name FROM (SELECT name FROM users) s ORDER BY s.name LIMIT 1",
		"SELECT u.name, o.total FROM users u JOIN orders o ON u.user_id = o.user_id \
		 ORDER BY o.total DESC LIMIT 2",
	] {
		let (answers, planned) = run(SETUP, Dialect::PgSql, text, &TestDb::owner());
		assert!(answers.iter().all(Result::is_ok), "`{text}` answered {answers:?}");
		assert_eq!(planned, 1, "`{text}`");
	}
}

/// A left join pairs a probe row no build row matches once, with `NULL` under
/// the build side's alias, and a build row the join condition leaves out does
/// not pair with a probe row it is keyed to.
#[cfg(feature = "postgres")]
#[test]
fn a_left_join_keeps_the_probe_rows_it_finds_no_build_row_for() {
	for (text, expected) in [
		(
			"SELECT u.name, o.total FROM users u LEFT JOIN orders o \
			 ON u.user_id = o.user_id AND o.total > 150 ORDER BY u.name",
			"[{ name: 'Alice', total: 200 }, { name: 'Bob', total: NONE }]",
		),
		(
			"SELECT u.name, o.total FROM users u JOIN orders o \
			 ON u.user_id = o.user_id AND o.total > 150 ORDER BY u.name",
			"[{ name: 'Alice', total: 200 }]",
		),
		(
			"SELECT o.total, u.name FROM orders o LEFT JOIN users u ON o.user_id = u.user_id \
			 ORDER BY o.total",
			"[{ name: 'Alice', total: 100 }, { name: 'Bob', total: 150 }, \
			 { name: 'Alice', total: 200 }]",
		),
	] {
		let (answers, planned) = run(SETUP, Dialect::PgSql, text, &TestDb::owner());
		assert_eq!(answers, vec![Ok(expected.to_owned())], "`{text}`");
		assert_eq!(planned, 1, "`{text}`");
	}
}

/// `DISTINCT` keeps the first of each set of equal rows it is given, which
/// are the rows before the projection, and a union of either kind keeps the
/// rows of its inputs in order.
#[cfg(feature = "postgres")]
#[test]
fn distinct_and_union_keep_the_first_of_each_set_of_equal_rows() {
	for (text, expected) in [
		(
			"SELECT DISTINCT s.user_id FROM (SELECT user_id FROM orders) s ORDER BY s.user_id",
			"[{ user_id: 1 }, { user_id: 2 }]",
		),
		(
			"SELECT user_id FROM orders UNION ALL SELECT user_id FROM users",
			"[{ user_id: 1 }, { user_id: 1 }, { user_id: 2 }, { user_id: 1 }, { user_id: 2 }]",
		),
		(
			"SELECT user_id FROM orders UNION SELECT user_id FROM users",
			"[{ user_id: 1 }, { user_id: 2 }]",
		),
	] {
		let (answers, planned) = run(SETUP, Dialect::PgSql, text, &TestDb::owner());
		assert_eq!(answers, vec![Ok(expected.to_owned())], "`{text}`");
		assert_eq!(planned, 1, "`{text}`");
	}
}

/// A join whose limit is spent reads no more of its probe side: the build side
/// is read whole, and the probe's records no further than its first batch.
#[cfg(feature = "postgres")]
#[test]
fn a_limit_over_a_join_stops_reading_its_probe() {
	let setup = format!("{SETUP} CREATE |nums:2000| SET user_id = 1 RETURN NONE;");
	let scanned = || super::host::RECORDS_SCANNED.with(std::cell::Cell::get);
	let before = scanned();
	let (answers, _) = run(
		&setup,
		Dialect::PgSql,
		"SELECT n.user_id FROM nums n JOIN users u ON n.user_id = u.user_id LIMIT 1",
		&TestDb::owner(),
	);
	assert_eq!(answers, vec![Ok("[{ user_id: 1 }]".to_owned())]);
	let read = scanned() - before;
	assert!(read < 1000, "a limit of one read {read} records of a join's sides");
}

/// A GQL mutation plans each synthetic write it applies once for the step,
/// not once per row: every row runs the one plan with its own record and
/// values bound, and leaves what a write per row leaves.
#[cfg(feature = "gql")]
#[tokio::test]
async fn a_mutation_plans_its_write_once_for_every_row() {
	use crate::exec::operators::test_util::{collect, plan_statement, try_collect};
	let db = TestDb::new(
		"DEFINE TABLE person SCHEMALESS;
		 FOR $i IN 0..20 { CREATE type::record('person', $i) SET n = $i RETURN NONE };",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (query, writes) in [
		("MATCH (a:person) SET a.doubled = a.n * 2 RETURN a.n AS n", 1),
		("MATCH (a:person WHERE a.n < 5) DETACH DELETE a", 1),
		("INSERT (:person {n: 100}), (:person {n: 101})", 1),
	] {
		let plan = crate::gql::parse_with_capabilities(
			query,
			&crate::dbs::Capabilities::all(),
			&db.datastore().parser_config(),
		)
		.unwrap_or_else(|e| panic!("`{query}` does not parse: {e:?}"))
		.0;
		let planner = super::tests::planner(&ctx);
		let vm = super::plan_front_end(&planner, plan).await.expect("the VM plans");
		let before = crate::exec::operators::mutate::WRITES_PLANNED.with(std::cell::Cell::get);
		try_collect(&vm, &ctx).await.unwrap_or_else(|e| panic!("`{query}` runs: {e:?}"));
		let planned =
			crate::exec::operators::mutate::WRITES_PLANNED.with(std::cell::Cell::get) - before;
		assert_eq!(planned, writes, "`{query}` planned {planned} writes");
	}
	let state = collect(
		&plan_statement("SELECT VALUE [n, doubled] FROM person ORDER BY n", &ctx).await,
		&ctx,
	)
	.await;
	let expected: Vec<crate::val::Value> = (5..20)
		.map(|n| {
			crate::val::Value::from(vec![
				crate::val::Value::from(n),
				crate::val::Value::from(n * 2),
			])
		})
		.chain([100, 101].map(|n| {
			crate::val::Value::from(vec![crate::val::Value::from(n), crate::val::Value::None])
		}))
		.collect();
	assert_eq!(state, expected);
}
