//! A write statement's scan never visits a record the statement created,
//! however few records each of its batches reads. A batch holds no more
//! records than the thread's fuel left, so a small `vm_quantum` reads a table
//! a record at a time and writes between every two reads.

use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// The quanta a shape runs under unless it says otherwise: a batch of one, two and three records,
/// and a batch holding the whole table.
const QUANTA: [usize; 4] = [1, 2, 3, 4096];

/// A write over a table that creates a record in it, and the records the
/// table holds once it has run.
struct Shape {
	name: &'static str,
	setup: &'static str,
	write: &'static str,
	/// What the statement left, as a query.
	check: &'static str,
	/// What `check` answers when the write visited none of its own records,
	/// as an expression.
	expected: &'static str,
	/// The quanta the shape runs under.
	quanta: &'static [usize],
}

/// A lightweight relation of 1,100 edges, from `v:0` to each of `v:1` to
/// `v:1100`.
const LIGHTWEIGHT: &str = "DEFINE TABLE v; DEFINE TABLE lw TYPE RELATION IN v OUT v LIGHTWEIGHT; CREATE v:0, v:2000; FOR $i IN 1..=1100 { RELATE v:0->lw->(CREATE ONLY type::record('v', $i)) };";

const SHAPES: &[Shape] = &[
	Shape {
		name: "UPDATE, ahead of the cursor",
		setup: "CREATE ahead:1; CREATE ahead:2;",
		write: "UPDATE ahead SET touched = true, spawn = (IF id = ahead:1 { (CREATE ahead:9).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM ahead WHERE touched ORDER BY id",
		expected: "[ahead:1, ahead:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE, behind the cursor",
		setup: "CREATE behind:5; CREATE behind:6;",
		write: "UPDATE behind SET touched = true, spawn = (IF id = behind:5 { (CREATE behind:4).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM behind WHERE touched ORDER BY id",
		expected: "[behind:5, behind:6]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE with a condition",
		setup: "CREATE cond:1 SET n = 1; CREATE cond:2 SET n = 2;",
		write: "UPDATE cond SET touched = true, spawn = (IF id = cond:1 { (CREATE cond:9 SET n = 9).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM cond WHERE touched ORDER BY id",
		expected: "[cond:1, cond:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE through an index",
		setup: "DEFINE INDEX by_n ON indexed FIELDS n; CREATE indexed:1 SET n = 1; CREATE indexed:2 SET n = 2;",
		write: "UPDATE indexed SET touched = true, spawn = (IF n = 1 { (CREATE indexed:9 SET n = 9).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM indexed WHERE touched ORDER BY id",
		expected: "[indexed:1, indexed:2]",
		quanta: &QUANTA,
	},
	// A B-tree index reads a thousand entries a batch whatever the fuel, so
	// this index holds more and runs under two quanta.
	Shape {
		name: "UPDATE through an index past its first batch",
		setup: "DEFINE INDEX by_n ON indexed_many FIELDS n; FOR $i IN 1..=1100 { CREATE type::record('indexed_many', $i) SET n = $i };",
		write: "UPDATE indexed_many SET touched = true, spawn = (IF n = 1 { (CREATE indexed_many:9999 SET n = 9999).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM indexed_many WHERE touched = NONE",
		expected: "[indexed_many:9999]",
		quanta: &[1, 4096],
	},
	// A bitmap, a full-text index and a vector index answer every record
	// they match before the write writes the first: these pin that a record
	// the write creates matching them stays unvisited.
	Shape {
		name: "UPDATE through a bitmap union of indexes",
		setup: "DEFINE INDEX by_n ON bor FIELDS n; DEFINE INDEX by_m ON bor FIELDS m; CREATE bor:1 SET n = 1, m = 0; CREATE bor:2 SET n = 0, m = 2;",
		write: "UPDATE bor SET touched = true, spawn = (IF id = bor:1 { (CREATE bor:9 SET n = 1, m = 0).id } ELSE { NONE }) WHERE n = 1 OR m = 2 RETURN NONE",
		check: "SELECT VALUE id FROM bor WHERE touched ORDER BY id",
		expected: "[bor:1, bor:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE through a bitmap intersection of indexes",
		setup: "DEFINE INDEX by_n ON band FIELDS n; DEFINE INDEX by_m ON band FIELDS m; CREATE band:1 SET n = 1, m = 1; CREATE band:2 SET n = 2, m = 2;",
		write: "UPDATE band SET touched = true, spawn = (IF id = band:1 { (CREATE band:9 SET n = 9, m = 9).id } ELSE { NONE }) WHERE n >= 1 AND m >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM band WHERE touched ORDER BY id",
		expected: "[band:1, band:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE through a full-text index",
		setup: "DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase; DEFINE INDEX ft ON fts FIELDS body FULLTEXT ANALYZER simple BM25; CREATE fts:1 SET body = 'hello'; CREATE fts:2 SET body = 'hello';",
		write: "UPDATE fts SET touched = true, spawn = (IF id = fts:1 { (CREATE fts:9 SET body = 'hello').id } ELSE { NONE }) WHERE body @@ 'hello' RETURN NONE",
		check: "SELECT VALUE id FROM fts WHERE touched ORDER BY id",
		expected: "[fts:1, fts:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE through a vector index",
		setup: "DEFINE INDEX hv ON vec FIELDS v HNSW DIMENSION 2 DIST EUCLIDEAN; CREATE vec:1 SET v = [0.0, 0.0]; CREATE vec:2 SET v = [1.0, 1.0];",
		write: "UPDATE vec SET touched = true, spawn = (IF id = vec:1 { (CREATE vec:9 SET v = [0.1, 0.1]).id } ELSE { NONE }) WHERE v <|3, 40|> [0.0, 0.0] RETURN NONE",
		check: "SELECT VALUE id FROM vec WHERE touched ORDER BY id",
		expected: "[vec:1, vec:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a table a value names",
		setup: "CREATE named:1 SET n = 1; CREATE named:2 SET n = 2;",
		write: "UPDATE type::table('named') SET touched = true, spawn = (IF id = named:1 { (CREATE named:9 SET n = 9).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM named WHERE touched ORDER BY id",
		expected: "[named:1, named:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a table beside a subquery naming a record it creates",
		setup: "CREATE nn:1; CREATE nn:2;",
		write: "UPDATE nn, (SELECT VALUE nn:9 FROM ONLY nn:1) SET touched = true, spawn = (IF id = nn:1 { (CREATE nn:9).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM nn WHERE touched ORDER BY id",
		// The subquery names `nn:9`, so it is written although the table's
		// read never visits it.
		expected: "[nn:1, nn:2, nn:9]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a range beside a range an expression yields",
		setup: "CREATE rr:[1], rr:[2], rr:[5], rr:[6];",
		write: "UPDATE rr:[1]..=[2], (IF rand::int(1, 1) = 1 { rr:[5].. } ELSE { NONE }) SET touched = true, spawn = (IF id = rr:[1] { (CREATE rr:[7]).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM rr WHERE touched ORDER BY id",
		// `rr:[7]` is created while the statement reads `rr`, so the second
		// range's read passes over it.
		expected: "[rr:[1], rr:[2], rr:[5], rr:[6]]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPSERT with a condition",
		setup: "CREATE upserted:1 SET n = 1; CREATE upserted:2 SET n = 2;",
		write: "UPSERT upserted SET touched = true, spawn = (IF id = upserted:1 { (CREATE upserted:9 SET n = 9).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM upserted WHERE touched ORDER BY id",
		expected: "[upserted:1, upserted:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "DELETE whose event creates",
		setup: "DEFINE EVENT spawn ON deleted WHEN $event = 'DELETE' AND $before.id = deleted:1 THEN { CREATE deleted:9 }; CREATE deleted:1; CREATE deleted:2;",
		write: "DELETE deleted RETURN NONE",
		check: "SELECT VALUE id FROM deleted",
		expected: "[deleted:9]",
		quanta: &QUANTA,
	},
	// An ignored INSERT creates its record inside a save point and is rolled
	// back by its unique index: the note of that create is undone with it
	// (the save-point journal is pinned in `kvs::creations`).
	Shape {
		name: "UPDATE whose ignored INSERT is rolled back",
		setup: "DEFINE INDEX uq ON rolled FIELDS u UNIQUE; CREATE rolled:1; CREATE rolled:2; CREATE rolled:5 SET u = 'taken';",
		write: "UPDATE rolled SET touched = true, spawn = (IF id = rolled:1 { (INSERT IGNORE INTO rolled { id: rolled:9, u: 'taken' }) } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM rolled ORDER BY id",
		expected: "[rolled:1, rolled:2, rolled:5]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a range whose bound creates in it",
		setup: "CREATE ranged:[2]; CREATE ranged:[3];",
		write: "UPDATE ranged:[(CREATE ONLY ranged:[1] RETURN VALUE 1)].. SET touched = true, spawn = (IF id = ranged:[2] { (CREATE ranged:[9]).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM ranged WHERE touched ORDER BY id",
		expected: "[ranged:[1], ranged:[2], ranged:[3]]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of two ranges",
		setup: "CREATE unioned:[1]; CREATE unioned:[2]; CREATE unioned:[6]; CREATE unioned:[7];",
		write: "UPDATE unioned:[1]..[5], unioned:[5].. SET touched = true, spawn = (IF id = unioned:[1] { [(CREATE unioned:[3]).id, (CREATE unioned:[8]).id] } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM unioned WHERE touched ORDER BY id",
		expected: "[unioned:[1], unioned:[2], unioned:[6], unioned:[7]]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of two tables with a condition",
		setup: "CREATE ta:1 SET n = 1; CREATE ta:2 SET n = 2; CREATE tb:1 SET n = 1;",
		write: "UPDATE ta, tb SET touched = true, spawn = (IF id = ta:1 { (CREATE tb:9 SET n = 9).id } ELSE { NONE }) WHERE n >= 1 RETURN NONE",
		check: "SELECT VALUE id FROM ta, tb WHERE touched ORDER BY id",
		expected: "[ta:1, ta:2, tb:1, tb:9]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of two tables",
		setup: "CREATE tc:1; CREATE tc:2; CREATE td:1;",
		write: "UPDATE tc, td SET touched = true, spawn = (IF id = tc:1 { (CREATE td:9).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM tc, td WHERE touched ORDER BY id",
		expected: "[tc:1, tc:2, td:1, td:9]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a table and a record its read creates",
		setup: "CREATE nm:1; CREATE nm:2;",
		write: "UPDATE nm, nm:9 SET touched = true, spawn = (IF id = nm:1 { (CREATE nm:9).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM nm WHERE touched ORDER BY id",
		expected: "[nm:1, nm:2, nm:9]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a table and a record a value names",
		setup: "CREATE np:1; CREATE np:2;",
		write: "UPDATE np, type::record('np', rand::int(9, 9)) SET touched = true, spawn = (IF id = np:1 { (CREATE np:9).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM np WHERE touched ORDER BY id",
		expected: "[np:1, np:2, np:9]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a table and a table a value names",
		setup: "CREATE pa:1; CREATE pb:1; CREATE pb:2;",
		write: "UPDATE pa, type::table(rand::enum('pb')) SET touched = true, spawn = (IF id = pb:1 { (CREATE pb:9).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM pa, pb WHERE touched ORDER BY id",
		expected: "[pa:1, pb:1, pb:2]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE of a range and a range a value names",
		setup: "CREATE rr:[1]; CREATE rr:[2]; CREATE rr:[5]; CREATE rr:[6];",
		write: "UPDATE rr:[1]..=[2], array::first([rr:[5].., rand::int(1, 1)]) SET touched = true, spawn = (IF id = rr:[1] { (CREATE rr:[7]).id } ELSE { NONE }) RETURN NONE",
		check: "SELECT VALUE id FROM rr WHERE touched ORDER BY id",
		expected: "[rr:[1], rr:[2], rr:[5], rr:[6]]",
		quanta: &QUANTA,
	},
	Shape {
		name: "UPDATE whose source record creates a row of a view it reads",
		setup: "DEFINE TABLE src; DEFINE TABLE vw AS SELECT * FROM src WHERE touched = true; CREATE src:[7];",
		write: "UPDATE vw:[1]..[2], src:[7], vw:[5].. SET touched = true RETURN NONE",
		check: "SELECT VALUE id FROM vw",
		expected: "[vw:[7]]",
		quanta: &QUANTA,
	},
	// A lightweight relation's scan reads a thousand edges a batch whatever
	// the fuel, so these relations hold more and run under one quantum.
	Shape {
		name: "DELETE of a lightweight relation that relates",
		setup: LIGHTWEIGHT,
		write: "DELETE lw RETURN (IF $before.out = v:1 { (RELATE v:2000->lw->v:0).id } ELSE { NONE })",
		check: "SELECT VALUE [in, out] FROM lw",
		expected: "[[v:2000, v:0]]",
		quanta: &[4096],
	},
	Shape {
		name: "DELETE of a lightweight relation that relates an edge it holds",
		setup: LIGHTWEIGHT,
		write: "DELETE lw RETURN (IF $before.out = v:1 { (RELATE v:0->lw->v:1100).id } ELSE { NONE })",
		check: "SELECT VALUE [in, out] FROM lw",
		expected: "[]",
		quanta: &[4096],
	},
	Shape {
		name: "DELETE of a lightweight range whose bound relates in it",
		setup: LIGHTWEIGHT,
		write: "DELETE lw:[v:0, (RELATE ONLY v:0->lw->v:2000 RETURN VALUE out)]..=[v:0, v:2000] RETURN NONE",
		check: "SELECT VALUE [in, out] FROM lw WHERE out = v:2000",
		expected: "[]",
		quanta: &[4096],
	},
	Shape {
		name: "DELETE of a lightweight range that relates in it",
		setup: LIGHTWEIGHT,
		write: "DELETE lw:[v:0, v:1]..=[v:0, v:2000] RETURN (IF $before.out = v:1 { (RELATE v:0->lw->v:2000).id } ELSE { NONE })",
		check: "SELECT VALUE [in, out] FROM lw",
		expected: "[[v:0, v:2000]]",
		quanta: &[4096],
	},
];

/// The value of the last statement of `sql`, failing the test on any error.
async fn last(db: &TestDb, sql: &str) -> Value {
	let results = db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
	let mut values = results.into_iter().map(|response| {
		response.result.map(Value::from).unwrap_or_else(|e| panic!("`{sql}` fails: {e}"))
	});
	values.next_back().expect("a statement ran")
}

#[tokio::test]
async fn a_write_never_visits_a_record_it_created() {
	let mut failures = Vec::new();
	for shape in SHAPES {
		for &quantum in shape.quanta {
			let config =
				surrealdb_cnf::ConfigMap::empty().with_key_value("vm_quantum", quantum.to_string());
			let db = TestDb::new_with_config(shape.setup, config).await;
			last(&db, shape.write).await;
			let left = last(&db, shape.check).await;
			let expected = last(&db, &format!("RETURN {}", shape.expected)).await;
			if left != expected {
				failures.push(format!(
					"{} under vm_quantum {quantum}: left {left:?}, expected {expected:?}",
					shape.name
				));
			}
		}
	}
	assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// A context over a write transaction of `db`'s, which the test holds after
/// the statements it plans have run.
async fn write_ctx(db: &TestDb) -> crate::exec::ExecutionContext {
	db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await
}

/// Run `sql`, one statement, on `ctx`'s transaction, failing the test on any
/// error.
async fn run_on(ctx: &crate::exec::ExecutionContext, sql: &str) {
	let plan = crate::exec::operators::test_util::plan_statement(sql, ctx).await;
	crate::exec::operators::test_util::try_collect(&plan, ctx)
		.await
		.unwrap_or_else(|e| panic!("`{sql}` fails: {e:?}"));
}

/// What the query on `ctx` holds for its transaction's creation log, and the
/// most it has held.
fn creations_held(ctx: &crate::exec::ExecutionContext) -> (usize, usize) {
	ctx.ctx().memory().expect("a datastore context has query memory").creations_held()
}

#[tokio::test]
async fn a_creating_write_charges_its_creation_log_and_releases_it() {
	const RECORDS: usize = 500;
	let setup = format!(
		"DEFINE TABLE other; FOR $i IN 1..={RECORDS} {{ CREATE type::record('charged', $i) }};"
	);
	let db = TestDb::new(&setup).await;
	let ctx = write_ctx(&db).await;
	run_on(&ctx, "UPDATE charged SET c = (CREATE charged).id RETURN NONE").await;
	let (held, peak) = creations_held(&ctx);
	// Each note holds at least its record's key, which holds the table's name
	// and a generated id of twenty characters.
	assert!(peak >= RECORDS * 20, "the log of {RECORDS} creations was charged {peak} bytes");
	assert_eq!(held, 0, "the charge is released once the statement's reads end");
	let txn = ctx.txn();
	assert_eq!((txn.creation_watches(), txn.creation_log_bytes()), (0, 0));

	// A write creating records only in a table it does not read notes none.
	let ctx = write_ctx(&db).await;
	run_on(&ctx, "UPDATE charged SET d = (CREATE other).id RETURN NONE").await;
	assert_eq!(creations_held(&ctx), (0, 0));
}

/// A block keeps the write state of a statement it ran: a write left by a
/// `BREAK` or a `CONTINUE` while it reads a table closes its watch on it all
/// the same, so the creations of the statements after it are not noted.
#[tokio::test]
async fn a_write_left_early_closes_its_watches() {
	let db = TestDb::new("CREATE early:1, early:2, early:3;").await;
	let mut failures = Vec::new();
	for (how, exit) in [("BREAK", "BREAK"), ("CONTINUE", "CONTINUE")] {
		let block = format!(
			"{{ FOR $i IN [1] {{ UPDATE early SET n = IF id = early:2 {{ {exit} }} ELSE {{ 1 }} RETURN NONE; }}; CREATE early:100; CREATE early:101; }}"
		);
		let ctx = write_ctx(&db).await;
		run_on(&ctx, &block).await;
		let (_, noted) = creations_held(&ctx);
		if noted != 0 {
			failures.push(format!("a write left by {how} kept noting creations: {noted} bytes"));
		}
	}
	assert!(failures.is_empty(), "{}", failures.join("\n"));
}
