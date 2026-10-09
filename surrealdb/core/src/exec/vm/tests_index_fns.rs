//! Statements that read through full-text and KNN access paths, and the
//! index functions and `MATCHES` operators that report against them. Each the
//! VM covers answers its rows, for the owner and for record users under every
//! kind of table permission, and compiles nothing while it runs; the shapes it
//! does not cover are listed with the reason.

use super::golden::Golden;
use super::program::{Instr, Program};
use super::tests::{bind, outcome, planner};
use super::tests_sources::{program_of, run, session};
use crate::exec::operators::test_util::TestDb;

/// Three tables of the same shape under a full, a conditional and a denied
/// table permission. Each holds three full-text indexes (three analyzers, one
/// of them stemming and one of n-grams), a vector index, plain fields, a field
/// a checked caller may not read and a computed one.
fn tables() -> String {
	let mut setup = String::from(
		"DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,ascii;
		 DEFINE ANALYZER stem TOKENIZERS blank,class FILTERS lowercase,snowball(english);
		 DEFINE ANALYZER grams TOKENIZERS blank FILTERS lowercase,ngram(2,3);\n",
	);
	for (table, permission) in [
		("open", "FULL"),
		("mine", "FOR select WHERE owner = $auth OR owner = $auth.id"),
		("locked", "FOR select NONE"),
	] {
		setup.push_str(&format!(
			"DEFINE TABLE {table} SCHEMALESS PERMISSIONS {permission};
			 DEFINE FIELD secret ON {table} PERMISSIONS FOR select NONE;
			 DEFINE FIELD label ON {table} COMPUTED 'row ' + <string> n;
			 DEFINE INDEX {table}_title ON {table} FIELDS title
				 FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
			 DEFINE INDEX {table}_body ON {table} FIELDS body
				 FULLTEXT ANALYZER stem BM25 HIGHLIGHTS;
			 DEFINE INDEX {table}_name ON {table} FIELDS name
				 FULLTEXT ANALYZER grams BM25 HIGHLIGHTS;
			 DEFINE INDEX {table}_vec ON {table} FIELDS emb HNSW DIMENSION 3 DISTANCE EUCLIDEAN;
			 DEFINE INDEX {table}_n ON {table} FIELDS n;
			 FOR $i IN 1..=24 {{
				 CREATE type::record('{table}', $i) SET
					 owner = IF $i % 2 = 0 {{ user:alice }} ELSE {{ user:bob }},
					 title = IF $i % 3 = 0 {{ 'Alpha Beta guide' }} ELSE {{ 'Beta gamma notes' }},
					 body = IF $i % 4 = 0 {{ 'running dogs and cats' }} ELSE {{ 'the dog runs home' }},
					 name = IF $i % 5 = 0 {{ 'Helloworld' }} ELSE {{ 'Goodbye' }},
					 emb = [<float> $i, 0.0, 0.0],
					 n = $i % 6,
					 tag = IF $i % 2 = 0 {{ 'a' }} ELSE {{ 'b' }},
					 secret = 's';
			 }};\n"
		));
	}
	setup.push_str(
		"DEFINE TABLE bare SCHEMALESS;
		 FOR $i IN 1..=8 { CREATE type::record('bare', $i) SET emb = [<float> $i, 0.0, 0.0], n = $i; };",
	);
	setup
}

/// Statements against table `t` the VM covers.
const COVERED: &[&str] = &[
	// Each operator, over each analyzer.
	"SELECT id FROM t WHERE title @1@ 'alpha' ORDER BY id",
	"SELECT id FROM t WHERE title @@ 'beta' ORDER BY id",
	"SELECT id FROM t WHERE title @AND@ 'alpha guide' ORDER BY id",
	"SELECT id FROM t WHERE title @OR@ 'alpha notes' ORDER BY id",
	"SELECT id FROM t WHERE title @1,AND@ 'beta guide' ORDER BY id",
	"SELECT id FROM t WHERE body @1@ 'run' ORDER BY id",
	"SELECT id FROM t WHERE body @1@ 'dogs' ORDER BY id",
	"SELECT id FROM t WHERE name @1@ 'ell' ORDER BY id",
	// An empty query and one the analyzer empties.
	"SELECT id FROM t WHERE title @1@ '' ORDER BY id",
	"SELECT id FROM t WHERE title @1@ '!!' ORDER BY id",
	// Index functions, in projections, orderings and nested expressions.
	"SELECT id, search::score(1) AS s FROM t WHERE title @1@ 'alpha' ORDER BY id",
	"SELECT id, search::score(1) AS s FROM t WHERE title @1@ 'beta' ORDER BY s DESC, id",
	"SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM t WHERE title @1@ 'beta' ORDER BY id",
	"SELECT id, search::highlight('<b>', '</b>', 1, true) AS h FROM t \
	 WHERE name @1@ 'ell' ORDER BY id",
	"SELECT id, search::offsets(1) AS o FROM t WHERE title @1@ 'beta' ORDER BY id",
	"SELECT id, search::offsets(1, true) AS o FROM t WHERE name @1@ 'ell' ORDER BY id",
	"SELECT id, <string> search::score(1) AS s, search::score(1) * 2 AS twice FROM t \
	 WHERE title @1@ 'beta' ORDER BY id",
	"SELECT VALUE search::score(1) FROM t WHERE title @1@ 'alpha' ORDER BY id",
	"SELECT id, { score: search::score(1), at: [search::offsets(1)] } AS x FROM t \
	 WHERE title @1@ 'alpha' ORDER BY id",
	"SELECT id, IF search::score(1) > 0 { 'hit' } ELSE { 'miss' } AS k FROM t \
	 WHERE title @1@ 'alpha' ORDER BY id",
	"SELECT id FROM t WHERE title @1@ 'alpha' AND search::score(1) > 0 ORDER BY id",
	// Functions as keys, as aggregate arguments and over a statement in `FROM`.
	"SELECT id, search::score(1) AS s FROM t WHERE title @1@ 'beta' ORDER BY s DESC, id LIMIT 3",
	"SELECT tag, math::max(search::score(1)) AS best FROM t WHERE title @1@ 'beta' \
	 GROUP BY tag ORDER BY tag",
	"SELECT * FROM (SELECT id, search::score(1) AS s FROM t WHERE title @1@ 'alpha') ORDER BY id",
	"SELECT id, vector::distance::knn() AS d FROM t WHERE emb <|6, 40|> [4.75, 0.0, 0.0] \
	 ORDER BY d, id LIMIT 2",
	// A function bound to a clause that matches nothing.
	"SELECT id, search::score(1) AS s FROM t WHERE title @1@ 'nothing' ORDER BY id",
	"SELECT id, search::score(1) AS s FROM t WHERE n > 100 AND title @1@ 'alpha' ORDER BY id",
	// Several clauses, each with its own reference.
	"SELECT id, search::score(1) AS a, search::score(2) AS b FROM t \
	 WHERE title @1@ 'beta' AND body @2@ 'dog' ORDER BY id",
	"SELECT id, search::highlight('[', ']', 2) AS h FROM t \
	 WHERE title @1@ 'beta' AND body @2@ 'dog' ORDER BY id",
	"SELECT id FROM t WHERE title @1@ 'alpha' AND body @2@ 'dog' ORDER BY id",
	"SELECT id FROM t WHERE title @1@ 'alpha' AND body @2@ 'zebra' ORDER BY id",
	// A clause no index serves, judged per row.
	"SELECT id FROM t WHERE title @1@ 'alpha' OR n = 2 ORDER BY id",
	"SELECT id FROM t WHERE title @1@ 'alpha' OR body @2@ 'dog' ORDER BY id",
	"SELECT id FROM t WHERE (title @1@ 'alpha') = true ORDER BY id",
	"SELECT id FROM t WHERE !(title @1@ 'alpha') ORDER BY id",
	"SELECT id FROM t WHERE title @1@ 'alpha' AND (n = 3 OR body @2@ 'dog') ORDER BY id",
	"SELECT id FROM t WHERE n > 0 AND title @@ 'beta' AND body @@ 'dogs' ORDER BY id",
	"SELECT id, search::score(1) AS s FROM t WHERE title @1@ 'beta' OR n = 2 ORDER BY id",
	// A clause beside scalar filters, paging, grouping and counting.
	"SELECT id FROM t WHERE title @1@ 'beta' AND n > 2 AND tag = 'a' ORDER BY id",
	"SELECT id FROM t WHERE title @1@ 'beta' ORDER BY id LIMIT 3 START 2",
	"SELECT tag, count() AS c FROM t WHERE title @1@ 'beta' GROUP BY tag ORDER BY tag",
	"SELECT count() FROM t WHERE title @1@ 'beta' GROUP ALL",
	"SELECT id, label, secret FROM t WHERE title @1@ 'alpha' ORDER BY id",
	"SELECT *, search::score(1) AS s FROM t WHERE title @1@ 'alpha' ORDER BY id",
	// A clause the statement's `WHERE` does not register, or has no `WHERE`
	// for, and rows no index answers for.
	"SELECT id, title @@ 'beta' AS m FROM t ORDER BY id",
	"SELECT id, title @1@ 'beta' AS m FROM t WHERE title @1@ 'beta' ORDER BY id",
	"SELECT id FROM t:1..5 WHERE title @@ 'beta'",
	"SELECT id, search::score(1) AS s FROM t:3 WHERE title @1@ 'alpha'",
	// A statement nested in the clause's, with its own clause.
	"SELECT id, (SELECT VALUE search::score(1) FROM t WHERE title @1@ 'alpha' ORDER BY id LIMIT 1) \
	 AS x FROM t:1",
	"SELECT id FROM t WHERE title @1@ 'alpha' AND id IN \
	 (SELECT VALUE id FROM t WHERE body @1@ 'dog') ORDER BY id",
	// The nearest neighbours of a vector, with their distances.
	"SELECT id FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] ORDER BY id",
	"SELECT id, vector::distance::knn() AS d FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] \
	 ORDER BY d, id",
	"SELECT id, vector::distance::knn() AS d FROM t WHERE emb <|6, 40|> [4.75, 0.0, 0.0] \
	 ORDER BY id",
	"SELECT id, vector::distance::knn() * 2 AS d, label FROM t \
	 WHERE emb <|3, 40|> [1.0, 0.0, 0.0] ORDER BY id",
	"SELECT VALUE vector::distance::knn() FROM t WHERE emb <|3, 40|> [9.0, 0.0, 0.0] ORDER BY id",
	"SELECT *, vector::distance::knn() AS d FROM t WHERE emb <|3, 40|> [9.0, 0.0, 0.0] ORDER BY id",
	"SELECT count() FROM t WHERE emb <|5, 40|> [2.0, 0.0, 0.0] GROUP ALL",
	// A condition beside the search is judged inside it: the rows are the
	// nearest that satisfy it.
	"SELECT id FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] AND tag = 'a' ORDER BY id",
	"SELECT id, vector::distance::knn() AS d FROM t \
	 WHERE emb <|4, 40|> [4.75, 0.0, 0.0] AND n > 1 ORDER BY id",
	"SELECT id FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] AND title @1@ 'beta' ORDER BY id",
	"SELECT id FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] AND n = 3 ORDER BY id",
	"SELECT id FROM t WHERE emb <|3, 40|> [4.75, 0.0, 0.0] AND n IN [1, 2] AND tag = 'b' \
	 ORDER BY id",
	"SELECT id, search::score(1) AS s, vector::distance::knn() AS d FROM t \
	 WHERE emb <|4, 40|> [4.75, 0.0, 0.0] AND title @1@ 'beta' AND n > 0 ORDER BY id",
	// A brute-force search ranks the rows the other conditions leave.
	"SELECT id FROM bare WHERE emb <|4, EUCLIDEAN|> [4.75, 0.0, 0.0] ORDER BY id",
	"SELECT id FROM t WHERE emb <|4, EUCLIDEAN|> [4.75, 0.0, 0.0] AND tag = 'a' ORDER BY id",
	"SELECT id, vector::distance::knn() AS d FROM t \
	 WHERE emb <|4, MANHATTAN|> [4.75, 0.0, 0.0] AND n > 1 ORDER BY d, id",
];

/// Statements a `KNN` operator or a `MATCHES` clause the planning cannot place
/// makes the statement refuse, before it reads a row.
const KNN_REFUSED: &[&str] = &[
	// A clause the statement's `WHERE` does not register.
	"SELECT id FROM t WHERE array::len([title @@ 'beta']) > 0",
	"SELECT id FROM t WHERE emb <|4, 40|> [4.75, 0.0, 0.0] OR tag = 'a' ORDER BY id",
	"SELECT id FROM bare WHERE emb <|4, EUCLIDEAN|> [4.75, 0.0, 0.0] OR n = 1 ORDER BY id",
	"SELECT id FROM t WHERE !(emb <|4, 40|> [4.75, 0.0, 0.0]) ORDER BY id",
];

/// Statements the VM does not cover, each with why.
const NO_CLAUSE: &[(&str, &str)] = &[
	// An index function the statement declares no operator for.
	(
		"SELECT id, search::score(1) AS s FROM t ORDER BY id",
		"Invalid query: Index function 'search::score': no MATCHES clause found in WHERE condition",
	),
	(
		"SELECT id, vector::distance::knn() AS d FROM t ORDER BY id",
		"Invalid query: Index function 'vector::distance::knn': no KNN operator found in WHERE condition",
	),
];

/// `sql` with table `t` replaced by `table`.
fn over(sql: &str, table: &str) -> String {
	sql.replace(" t ", &format!(" {table} "))
		.replace(" t:", &format!(" {table}:"))
		.replace(" t\n", &format!(" {table}\n"))
}

/// Whether `sql` searches the vectors of a table a checked caller may not
/// select from, which the VM does not plan.
fn locked_search(who: &str, sql: &str) -> bool {
	who != "owner" && sql.contains("FROM locked") && sql.contains("<|")
}

/// Every statement answers its rows, for the owner and for two record users,
/// and compiles nothing while it runs.
#[tokio::test]
async fn index_statements_answer_their_rows_and_compile_nothing() {
	let mut golden = Golden::new("index_statements_answer_their_rows_and_compile_nothing");
	let db = TestDb::new_with_auth(&tables()).await;
	let mut declined = Vec::new();
	let mut compiled = Vec::new();
	let mut nonempty = 0;
	for (who, who_session) in
		[("owner", TestDb::owner()), ("alice", session("alice")), ("bob", session("bob"))]
	{
		let ctx = db.exec_ctx_as(&who_session, crate::kvs::TransactionType::Write).await;
		for table in ["open", "mine", "locked"] {
			for sql in COVERED.iter().map(|sql| over(sql, table)) {
				let Some((vm, compiles)) = run(&ctx, &sql).await else {
					if !locked_search(who, &sql) {
						declined.push(format!("`{sql}` for {who}"));
					}
					continue;
				};
				golden.rows(format!("{who}: {sql}"), &vm);
				if matches!(&vm, Ok(rows) if !rows.is_empty()) {
					nonempty += 1;
				}
				if compiles > 0 {
					compiled.push(format!("`{sql}` for {who}: {compiles} compilations"));
				}
			}
		}
	}
	assert!(compiled.is_empty(), "{compiled:#?}");
	assert!(declined.is_empty(), "the VM declined {declined:#?}");
	assert!(nonempty > 150, "the statements read too little: {nonempty}");
	golden.check();
}

/// A `KNN` operator no scan or search consumes is refused as the statement is
/// planned.
#[tokio::test]
async fn a_knn_operator_nothing_consumes_is_refused() {
	let mut golden = Golden::new("a_knn_operator_nothing_consumes_is_refused");
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in KNN_REFUSED {
		let sql = over(sql, "open");
		let bound = bind(&ctx, &sql).await;
		let vm = super::plan_checked(&planner(&ctx), &bound, None).await;
		let vm = vm.err().unwrap_or_else(|| panic!("`{sql}` was planned")).to_string();
		golden.record(&sql, &vm);
	}
	golden.check();
}

/// An index function whose statement declares no operator for it is refused
/// as the statement is planned, with an error naming the missing operator.
#[tokio::test]
async fn an_index_function_no_operator_answers_is_refused() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (sql, error) in NO_CLAUSE {
		let sql = over(sql, "open");
		let bound = bind(&ctx, &sql).await;
		let refused = super::plan_checked(&planner(&ctx), &bound, None)
			.await
			.err()
			.unwrap_or_else(|| panic!("`{sql}` was planned"))
			.to_string();
		assert_eq!(&refused, error, "`{sql}`");
	}
}

/// A condition beside a vector search is judged inside the search, so the
/// rows are the nearest that satisfy it.
#[tokio::test]
async fn a_condition_beside_a_vector_search_is_judged_inside_the_search() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	// The four nearest rows to the first vector are 1 to 4; those tagged `a`
	// among them are 2 and 4, and the four nearest tagged `a` are 2, 4, 6, 8.
	let sql = "SELECT VALUE id FROM open WHERE emb <|4, 40|> [1.0, 0.0, 0.0] AND tag = 'a' \
	           ORDER BY id";
	let (vm, _) = run(&ctx, sql).await.expect("the VM covers the search it gates");
	let mut golden = Golden::new("a_condition_beside_a_vector_search_is_judged_inside_the_search");
	golden.rows(sql, &vm);
	golden.check();
	assert_eq!(vm.as_ref().expect("reads").len(), 4);
}

/// An index function and a `MATCHES` operator are instructions: no compiled
/// expression stands behind them.
#[tokio::test]
async fn index_functions_and_matches_operators_are_instructions() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut instructions = 0;
	for sql in COVERED {
		if !(sql.contains("search::") || sql.contains("distance::knn") || sql.contains('@')) {
			continue;
		}
		let sql = over(sql, "open");
		let program = program_of(&ctx, &sql).await;
		let named = |program: &Program| {
			program.pipelines.iter().chain(program.host_code.iter()).any(|pipeline| {
				pipeline
					.code
					.iter()
					.any(|i| matches!(i, Instr::CallIndexFn { .. } | Instr::Matches { .. }))
			})
		};
		instructions += usize::from(named(&program));
	}
	assert!(instructions > 25, "{instructions}");
}

/// A function reads the distances and the clauses of its own statement: a
/// statement nested in another keeps a search of its own.
#[tokio::test]
async fn a_nested_statement_reads_the_distances_of_its_own_search() {
	let mut golden = Golden::new("a_nested_statement_reads_the_distances_of_its_own_search");
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT id, vector::distance::knn() AS d, \
		 (SELECT VALUE vector::distance::knn() FROM open WHERE emb <|2, 40|> [20.25, 0.0, 0.0] \
		 ORDER BY id) AS inner FROM open WHERE emb <|3, 40|> [1.0, 0.0, 0.0] ORDER BY id",
		"SELECT id, search::score(1) AS s, \
		 (SELECT VALUE search::score(1) FROM open WHERE body @1@ 'dog' ORDER BY id LIMIT 2) \
		 AS inner FROM open WHERE title @1@ 'alpha' ORDER BY id",
	] {
		let (vm, _) = run(&ctx, sql).await.expect("covered");
		assert!(matches!(&vm, Ok(rows) if !rows.is_empty()), "`{sql}`: {vm:?}");
		golden.rows(sql, &vm);
	}
	golden.check();
}

/// A function that reports against the clause of the statement it is nested
/// in, and not one of its own, reports against that clause.
#[tokio::test]
async fn a_function_that_inherits_its_clause_from_an_enclosing_statement_reports_against_it() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let sql = "SELECT id, (SELECT VALUE search::score(1) FROM ONLY open:3) AS inner \
	           FROM open WHERE title @1@ 'alpha' ORDER BY id";
	let (vm, _) = run(&ctx, sql).await.expect("the VM covers the inherited scope");
	assert!(vm.is_ok(), "{vm:?}");
	let mut golden = Golden::new(
		"a_function_that_inherits_its_clause_from_an_enclosing_statement_reports_against_it",
	);
	golden.rows(sql, &vm);
	golden.check();
}

/// The distances and the clause resolutions of a statement are shared by the
/// workers a region hands its rows to.
#[tokio::test]
async fn workers_share_the_distances_and_the_resolutions_of_their_statement() {
	let mut golden =
		Golden::new("workers_share_the_distances_and_the_resolutions_of_their_statement");
	let setup = "
		DEFINE ANALYZER simple TOKENIZERS blank,class FILTERS lowercase,ascii;
		DEFINE TABLE w SCHEMALESS;
		DEFINE INDEX w_title ON w FIELDS title FULLTEXT ANALYZER simple BM25 HIGHLIGHTS;
		DEFINE INDEX w_vec ON w FIELDS emb HNSW DIMENSION 3 DISTANCE EUCLIDEAN;
		FOR $i IN 0..600 {
			CREATE type::record('w', $i) SET
				title = IF $i % 2 = 0 { 'alpha beta' } ELSE { 'beta gamma' },
				emb = [<float> $i, 1.0, 0.0],
				n = $i;
		};
	";
	let serial = TestDb::new_with_config(
		setup,
		surrealdb_cnf::ConfigMap::empty()
			.with_key_value("region_row_threshold", usize::MAX.to_string()),
	)
	.await;
	let region = TestDb::new_with_config(
		setup,
		surrealdb_cnf::ConfigMap::empty().with_key_value("region_row_threshold", "1"),
	)
	.await;
	let handed = super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed);
	for sql in [
		"SELECT id, search::score(1) AS s, search::highlight('<', '>', 1) AS h FROM w \
		 WHERE title @1@ 'alpha' ORDER BY id",
		"SELECT id, vector::distance::knn() AS d FROM w WHERE emb <|400, 600|> [0.0, 1.0, 0.0] \
		 ORDER BY id",
		"SELECT id FROM w WHERE n >= 0 AND (title @1@ 'alpha' OR n = 3) ORDER BY id",
	] {
		let mut answers = Vec::new();
		for db in [&serial, &region] {
			let ctx = db.exec_ctx().await;
			let (vm, _) = run(&ctx, sql).await.expect("covered");
			answers.push(vm);
		}
		assert!(matches!(&answers[0], Ok(rows) if rows.len() > 100), "`{sql}`: {:?}", answers[0]);
		assert_eq!(answers[0], answers[1], "`{sql}` serial against region");
		golden.rows(sql, &answers[0]);
	}
	assert!(
		super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed) > handed,
		"no batch ran on a worker"
	);
	// Planning is not running.
	let ctx = region.exec_ctx().await;
	let bound = bind(&ctx, "SELECT id FROM w WHERE title @1@ 'alpha'").await;
	let op = super::plan(&planner(&ctx), &bound).await.expect("covered");
	assert!(outcome(&op, &ctx).await.is_ok());
	golden.check();
}

/// Explaining a statement that reports against its clauses renders the plan
/// the VM runs it by, and names the access path of its clauses.
#[tokio::test]
async fn explaining_a_statement_with_index_functions_names_its_access_paths() {
	let db = TestDb::new_with_auth(&tables()).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for (sql, scan) in [
		("EXPLAIN SELECT id FROM open WHERE title @1@ 'alpha'", "FullTextScan"),
		(
			"EXPLAIN SELECT id, search::score(1) AS s FROM open WHERE title @1@ 'alpha'",
			"FullTextScan",
		),
		("EXPLAIN SELECT id FROM open WHERE title @1@ 'alpha' OR n = 2", "BitmapFullTextScan"),
		(
			"EXPLAIN SELECT id, vector::distance::knn() AS d FROM open \
			 WHERE emb <|3, 40|> [1.0, 0.0, 0.0]",
			"KnnScan",
		),
		(
			"EXPLAIN ANALYZE SELECT id, search::highlight('<b>', '</b>', 1) AS h FROM open \
			 WHERE title @1@ 'alpha'",
			"FullTextScan",
		),
	] {
		let bound = bind(&ctx, sql).await;
		let op = super::plan(&planner(&ctx), &bound)
			.await
			.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
		let rows = outcome(&op, &ctx).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"));
		let [crate::val::Value::String(plan)] = rows.as_slice() else {
			panic!("`{sql}` answers {rows:?}");
		};
		assert!(plan.contains(scan), "`{sql}` renders\n{plan}");
	}
}
