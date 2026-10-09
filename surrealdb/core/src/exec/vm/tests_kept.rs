//! A block is compiled in full when the query arrives: a loop lowers its body
//! once, whatever its iterations, and a block whose catalog changes it cannot
//! know ahead is declined whole — either way answering its rows, and compiling
//! nothing once it runs.

use surrealdb_types::ToSql;

use super::golden::Golden;
use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

/// The results of a run, one per line.
fn render_results(results: &[Result<Value, String>]) -> String {
	results
		.iter()
		.map(|result| match result {
			Ok(value) => value.to_sql().replace('\n', "\\n"),
			Err(message) => format!("error: {}", message.replace('\n', "\\n")),
		})
		.collect::<Vec<_>>()
		.join("\n")
}

/// Run `setup` then each of `statements` on a fresh database, and answer each
/// statement's result and how many programs the statements ran, the
/// speculative runs of their schema statements aside. Running them compiles
/// nothing once a program has started ([`crate::exec::compiles`]).
fn run(setup: &str, statements: &[&str]) -> (Vec<Result<Value, String>>, usize) {
	run_compiling(setup, statements, false)
}

/// [`run`], letting the statements compile once their programs started where
/// `compiles`: a late-bound statement compiles what its table's catalog
/// version holds the first time it reads it, as a cache miss.
fn run_compiling(
	setup: &str,
	statements: &[&str],
	compiles: bool,
) -> (Vec<Result<Value, String>>, usize) {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let db = TestDb::new(setup).await;
		let session = TestDb::owner();
		let before = crate::exec::compiles::programs();
		let speculated = super::speculate::SPECULATED.with(std::cell::Cell::get);
		let compiled = crate::exec::compiles::count();
		let mut out = Vec::new();
		for sql in statements {
			let mut results = db.datastore().execute(sql, &session, None).await.expect("runs");
			for result in results.drain(..) {
				out.push(result.result.map(Value::from).map_err(|e| e.to_string()));
			}
		}
		if !compiles {
			assert_eq!(
				crate::exec::compiles::count(),
				compiled,
				"{statements:?} compiled while a program ran"
			);
		}
		let speculated = super::speculate::SPECULATED.with(std::cell::Cell::get) - speculated;
		(out, crate::exec::compiles::programs() - before - speculated)
	})
}

/// How many lowerings `sql` makes on the VM over a fresh database set up by
/// `setup`, with the result it answers; running it compiles nothing once its
/// program has started.
fn lowerings(setup: &str, sql: &str) -> (usize, Vec<Result<Value, String>>) {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let db = TestDb::new(setup).await;
		let session = TestDb::owner();
		let before = super::lower::LOWERINGS.with(std::cell::Cell::get);
		let compiled = crate::exec::compiles::count();
		let results = db.datastore().execute(sql, &session, None).await.expect("runs");
		assert_eq!(crate::exec::compiles::count(), compiled, "`{sql}` compiled while it ran");
		let lowered = super::lower::LOWERINGS.with(std::cell::Cell::get) - before;
		let out = results
			.into_iter()
			.map(|result| result.result.map(Value::from).map_err(|e| e.to_string()))
			.collect();
		(lowered, out)
	})
}

/// A loop's body statement is planned once, not once per iteration, and the
/// loop leaves its rows.
#[test]
fn a_loop_plans_its_body_once() {
	let mut golden = Golden::new("a_loop_plans_its_body_once");
	let setup = "DEFINE TABLE t; DEFINE FIELD n ON t TYPE int;";
	for body in [
		"FOR $i IN 0..50 { CREATE t SET n = $i }",
		"FOR $i IN 0..50 { CREATE type::record('t', $i) SET n = $i }",
		"FOR $i IN 0..50 { UPSERT type::record('t', $i % 5) SET n = $i }",
		"FOR $i IN 0..50 { LET $x = $i * 2; CREATE t SET n = $x }",
	] {
		let statements = [body, "SELECT n FROM t ORDER BY n"];
		let (vm, programs) = run(setup, &statements);
		golden.record(body, render_results(&vm));
		assert!((1..20).contains(&programs), "`{body}` ran {programs} programs for 50 iterations");
	}
	golden.check();
}

/// A loop's body is lowered once, when the query arrives, for every
/// iteration the loop then runs: a loop of fifty iterations lowers as much as
/// one of two, and neither compiles anything once it runs. The body's
/// statements read the names the loop binds through the program's own
/// registers, so no iteration needs a plan of its own.
#[test]
fn a_loop_body_is_lowered_once_when_the_query_arrives() {
	let setup = "DEFINE TABLE t; DEFINE FIELD n ON t TYPE int; DEFINE INDEX by_n ON t FIELDS n;";
	for body in [
		"FOR $i IN 0..{n} { CREATE t SET n = $i }",
		"FOR $i IN 0..{n} { CREATE type::record('t', $i) SET n = $i }",
		"FOR $i IN 0..{n} { UPSERT type::record('t', $i % 3) SET n = $i }",
		"FOR $i IN 0..{n} { LET $x = $i * 2; CREATE t SET n = $x; SELECT * FROM t WHERE n = $x }",
		"FOR $i IN 0..{n} { IF $i % 2 = 0 { CREATE t SET n = $i } ELSE { UPDATE t SET n += 1 WHERE n < $i } }",
		"FOR $i IN 0..{n} { FOR $j IN 0..2 { CREATE t SET n = $i * 10 + $j } }",
		"FOR $i IN 0..{n} { CREATE t SET n = $i, at = time::unix() }",
		// A membership the logical rules lift into a semi join is lowered as
		// the membership it was lifted from, once for every iteration.
		"FOR $i IN 0..{n} { CREATE t SET n = $i; LET $r = SELECT VALUE n FROM t WHERE n IN (SELECT VALUE n FROM t) }",
		"FOR $i IN 0..{n} { CREATE t SET n = $i; LET $r = SELECT VALUE n FROM t WHERE n NOT IN (SELECT VALUE n + 1 FROM t) }",
		// `EXPLAIN ANALYZE` runs the statement it explains as a group of the
		// program.
		"FOR $i IN 0..{n} { CREATE t SET n = $i; LET $r = (EXPLAIN ANALYZE SELECT * FROM t) }",
	] {
		let short = body.replace("{n}", "2");
		let long = body.replace("{n}", "50");
		let (few, few_out) = lowerings(setup, &short);
		let (many, many_out) = lowerings(setup, &long);
		assert!(few > 0, "`{short}` did not run on the VM");
		assert_eq!(few, many, "`{body}` lowered per iteration: {few} for 2, {many} for 50");
		assert!(few_out.iter().chain(&many_out).all(Result::is_ok), "`{body}` failed");
	}
}

/// A block holding a statement that holds a schema statement inside it plans
/// what follows the statement when it reaches it, and answers its rows.
#[test]
fn a_block_holding_a_nested_schema_statement_plans_what_follows_it_when_reached() {
	let mut golden =
		Golden::new("a_block_holding_a_nested_schema_statement_plans_what_follows_it_when_reached");
	let setup = "DEFINE TABLE t; DEFINE FIELD n ON t TYPE int;";
	for body in [
		"FOR $i IN 0..3 { CREATE t SET n = $i, d = (DEFINE FIELD OVERWRITE m ON t VALUE $i) }",
		"{ CREATE t:1 SET n = 1; UPDATE t:1 SET d = (DEFINE FIELD OVERWRITE m ON t VALUE 2); RETURN 1 }",
	] {
		let statements = [body, "SELECT n, m, d FROM t ORDER BY n"];
		let (vm, programs) = run(setup, &statements);
		golden.record(body, render_results(&vm));
		// The read after the block runs as a program, and so does each statement
		// the block plans as it reaches it.
		assert!(programs >= 1, "`{body}` ran no program");
	}
	golden.check();
}

/// A loop whose iterations change the catalog its body reads — by a statement
/// of the body, or by an event a write of it fires — is declined whole and
/// answers its rows.
#[test]
fn a_loop_changing_the_catalog_its_body_reads_answers_its_rows() {
	let mut golden = Golden::new("a_loop_changing_the_catalog_its_body_reads_answers_its_rows");
	for (setup, body) in [
		(
			"DEFINE TABLE t;",
			"FOR $i IN 0..4 { IF $i = 1 { DEFINE FIELD m ON t VALUE 9 }; CREATE t SET n = $i }",
		),
		(
			"DEFINE TABLE t;
			 DEFINE EVENT e ON t WHEN $event = 'CREATE' AND $after.n = 0 THEN {
				DEFINE FIELD m ON t VALUE 7
			 };",
			"FOR $i IN 0..4 { CREATE t SET n = $i }",
		),
		(
			"DEFINE TABLE t;
			 DEFINE EVENT e ON t WHEN $event = 'CREATE' AND $after.n = 1 THEN {
				DEFINE FIELD n ON t TYPE string
			 };",
			"FOR $i IN 0..4 { CREATE type::record('t', $i) SET n = $i }",
		),
		(
			"DEFINE TABLE t;
			 DEFINE EVENT e ON t WHEN $event = 'CREATE' AND $after.n = 1 THEN {
				DEFINE FIELD secret ON t PERMISSIONS FOR select NONE
			 };",
			"FOR $i IN 0..4 { CREATE t SET n = $i, secret = 's' RETURN secret }",
		),
	] {
		let statements = [body, "SELECT n, m FROM t ORDER BY n"];
		golden.record(body, render_results(&run(setup, &statements).0));
	}
	golden.check();
}

/// A block that changes the catalog answers its rows: a field defined in the
/// block is enforced by the statements after it, a table a loop's first write
/// defines is read by the rest, and control flow, bindings and errors are those
/// of the statements as written.
#[test]
fn a_block_changing_the_catalog_answers_its_rows() {
	let mut golden = Golden::new("a_block_changing_the_catalog_answers_its_rows");
	let setup = "DEFINE TABLE t; DEFINE TABLE person;";
	for body in [
		"{ DEFINE FIELD age ON person TYPE int ASSERT $value > 0; CREATE person:1 SET age = -1 }",
		"{ DEFINE FIELD age ON person TYPE int ASSERT $value > 0; CREATE person:2 SET age = 3 }",
		"{ LET $a = 2; DEFINE FIELD k ON t VALUE 5; CREATE t:1 SET n = $a }",
		"FOR $i IN 0..5 { CREATE fresh SET n = $i }",
		"FOR $i IN 0..5 { CREATE type::record('fresh', $i) SET n = $i }",
		"FOR $i IN 0..4 { FOR $j IN 0..4 { IF $j > $i { BREAK }; IF $j = 1 { CONTINUE }; CREATE t SET n = $i * 10 + $j } }",
		"{ FOR $i IN 0..10 { IF $i = 3 { RETURN $i }; CREATE t SET n = $i }; CREATE t SET n = 99 }",
		"FOR $i IN 0..5 { CREATE t SET n = $i; IF $i = 3 { THROW 'stop' } }",
		"FOR $i IN 0..3 { CREATE t SET n = $i; DEFINE FIELD m ON t VALUE $i; CREATE t SET n = $i + 100 }",
	] {
		let statements = [
			body,
			"SELECT n, k, m FROM t ORDER BY n",
			"SELECT * FROM person ORDER BY id",
			"SELECT n FROM fresh ORDER BY n",
		];
		golden.record(body, render_results(&run(setup, &statements).0));
	}
	golden.check();
}

/// A block's schema statements are run speculatively while it is planned, so
/// the statements after them are planned against what they define and the
/// block runs as one program lowered when the query arrives. A write that
/// defines the table it writes to leaves the table the statements after it
/// were lowered against — the open table with no definition — so nothing
/// after it needs lowering again either.
#[test]
fn a_block_compiles_what_follows_a_change_once_as_one_program() {
	let mut golden = Golden::new("a_block_compiles_what_follows_a_change_once_as_one_program");
	let setup = "DEFINE TABLE t; DEFINE TABLE person;";
	for body in [
		"{ DEFINE FIELD age ON person TYPE int; CREATE person:1 SET age = 1; \
		   CREATE person:2 SET age = 2; UPDATE person SET age += 1; SELECT * FROM person }",
		"{ DEFINE FIELD a ON t VALUE 1; CREATE t:1 SET n = 1; DEFINE FIELD b ON t VALUE 2; \
		   CREATE t:2 SET n = 2; DEFINE FIELD c ON t VALUE 3; CREATE t:3 SET n = 3 }",
		"FOR $i IN 0..20 { CREATE fresh SET n = $i }",
	] {
		let statements_after = ["SELECT * FROM t ORDER BY id", "SELECT * FROM person ORDER BY id"];
		let statements = [&[body][..], &statements_after[..]].concat();
		let (vm, programs) = run(setup, &statements);
		golden.record(body, render_results(&vm));
		// The block and each read after it, every one a program of its own.
		assert_eq!(programs, statements.len(), "`{body}` ran apart from one program");
	}
	golden.check();
}

/// What `statements` answer run one after another by `session` over a fresh
/// database set up by `setup` with record access for `user:reader`.
fn run_as(setup: &str, statements: &[&str], record_user: bool) -> Vec<Result<Value, String>> {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let db = TestDb::new_with_auth(setup).await;
		let session = if record_user {
			let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new(
				"user", "reader",
			));
			crate::dbs::Session::for_record("test", "test", "user", rid)
		} else {
			TestDb::owner()
		};
		let compiled = crate::exec::compiles::count();
		let mut out = Vec::new();
		for sql in statements {
			let mut results = db.datastore().execute(sql, &session, None).await.expect("runs");
			for result in results.drain(..) {
				out.push(result.result.map(Value::from).map_err(|e| e.to_string()));
			}
		}
		assert_eq!(
			crate::exec::compiles::count(),
			compiled,
			"{statements:?} compiled while a program ran"
		);
		out
	})
}

/// A loop of writes reads its tables' definitions when it arrives, not once
/// per iteration: whatever the loop's writes maintain — fields, indexes,
/// views, events, cascades, permissions — and however its iterations change
/// the catalog, each iteration answers and leaves its rows, for the owner and
/// for a checked record user.
#[test]
fn nest_a_write_loop_keeps_its_catalog_for_the_block() {
	let mut golden = Golden::new("nest_a_write_loop_keeps_its_catalog_for_the_block");
	let setup = "
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
		CREATE user:reader;
		DEFINE TABLE t PERMISSIONS FULL;
		DEFINE FIELD n ON t TYPE int;
		DEFINE INDEX by_n ON t FIELDS n UNIQUE;
		DEFINE TABLE sale PERMISSIONS FULL;
		DEFINE TABLE by_region AS SELECT region, count() AS c, math::sum(amount) AS total,
			math::max(amount) AS high FROM sale GROUP BY region PERMISSIONS FULL;
		DEFINE TABLE log PERMISSIONS FULL;
		DEFINE EVENT noted ON t WHEN $event = 'CREATE' THEN (CREATE log SET of = $after.id, at = $i);
		DEFINE TABLE root PERMISSIONS FULL;
		DEFINE TABLE mid PERMISSIONS FULL;
		DEFINE FIELD up ON mid TYPE option<record<root>> REFERENCE ON DELETE CASCADE;
		DEFINE TABLE shut PERMISSIONS FOR select FULL, FOR create WHERE n < 3, FOR update, delete NONE;
		FOR $i IN 0..6 { CREATE type::record('root', $i); CREATE type::record('mid', $i) SET up = type::record('root', $i) };
	";
	let after = [
		"SELECT * FROM t ORDER BY id",
		"SELECT * FROM by_region ORDER BY id",
		"SELECT at, of FROM log ORDER BY of",
		"SELECT * FROM mid ORDER BY id",
		"SELECT * FROM shut ORDER BY id",
		"SELECT * FROM fresh ORDER BY id",
	];
	for body in [
		"FOR $i IN 0..8 { CREATE type::record('t', $i) SET n = $i }",
		"FOR $i IN 0..8 { CREATE type::record('t', $i) SET n = $i % 3 }",
		"FOR $i IN 0..8 { UPSERT type::record('sale', $i % 3) SET region = <string> ($i % 2), amount = $i }",
		"FOR $i IN 0..6 { DELETE type::record('root', $i) }",
		"FOR $i IN 0..6 { CREATE type::record('shut', $i) SET n = $i }",
		"FOR $i IN 0..5 { CREATE type::record('fresh', $i) SET n = $i }",
		"FOR $i IN 0..6 { CREATE type::record('t', $i) SET n = $i; DEFINE FIELD OVERWRITE m ON t VALUE $i * 10 }",
		"FOR $i IN 0..6 { IF $i = 3 { DEFINE INDEX by_m ON t FIELDS m UNIQUE }; CREATE type::record('t', $i) SET n = $i, m = $i }",
		"FOR $i IN 0..6 { IF $i = 3 { DEFINE INDEX by_m ON t FIELDS m UNIQUE }; CREATE type::record('t', $i) SET n = $i, m = $i % 2 }",
		"FOR $i IN 0..6 { CREATE type::record('t', $i) SET n = $i; IF $i = 2 { REMOVE EVENT noted ON t } }",
	] {
		// A record user may not change the catalog.
		let users: &[bool] = if body.contains("DEFINE") || body.contains("REMOVE") {
			&[false]
		} else {
			&[false, true]
		};
		for &record_user in users {
			let statements = [&[body][..], &after[..]].concat();
			let rows = run_as(setup, &statements, record_user);
			golden.record(format!("{body} (record user: {record_user})"), render_results(&rows));
		}
	}
	golden.check();
}

/// Write statements that are groups of one program run each as itself: two
/// loops writing records of one table named as they arrive write what their
/// own bodies say, whether or not a schema statement of the block came
/// before them.
#[test]
fn nest_write_groups_of_one_program_keep_their_own_bodies() {
	let mut golden = Golden::new("nest_write_groups_of_one_program_keep_their_own_bodies");
	let loops = "FOR $i IN 1..=3 { CREATE type::record('item', $i) SET v = true, p = $i + 1 }; \
		FOR $i IN 4..=6 { CREATE type::record('item', $i) SET v = false, p = 10 }";
	for (setup, body) in [
		("DEFINE TABLE item;", format!("{{ {loops}; RETURN 'OK' }}")),
		("", format!("{{ DEFINE TABLE item; {loops}; RETURN 'OK' }}")),
		(
			"",
			format!(
				"{{ DEFINE TABLE item SCHEMAFULL; DEFINE FIELD v ON item TYPE bool; \
				   DEFINE FIELD p ON item TYPE int; DEFINE INDEX iv ON item FIELDS v; {loops}; RETURN 'OK' }}"
			),
		),
		(
			"",
			"{ DEFINE FIELD a ON item TYPE option<int>; CREATE item:1 SET a = 1; CREATE item:2 SET b = 2; \
			   UPDATE item:1 SET c = 3; SELECT * FROM item ORDER BY id }"
				.to_owned(),
		),
	] {
		let statements = [body.as_str(), "SELECT * FROM item ORDER BY id"];
		let (vm, _) = run(setup, &statements);
		golden.record(&body, render_results(&vm));
	}
	golden.check();
}

/// A name bound around a block statement — a loop parameter, a `LET` before
/// it — is read when the statement runs, never folded into its program, so a
/// body lowered once answers each iteration for the value bound then,
/// wherever the statement reads it: a condition, a source, a data clause.
#[test]
fn a_lowered_body_reads_the_names_bound_around_it_each_iteration() {
	let mut golden = Golden::new("a_lowered_body_reads_the_names_bound_around_it_each_iteration");
	let setup = "
		DEFINE TABLE t;
		FOR $i IN 0..6 { CREATE type::record('t', $i) SET n = $i, hits = 0 };
		DEFINE TABLE u;
		CREATE u:1 SET kind = 'u';
	";
	for body in [
		"FOR $i IN 0..4 { UPDATE t SET hits += 1 WHERE n = $i }",
		"FOR $i IN [1, 3] { DELETE t WHERE n = $i }",
		"FOR $i IN 0..3 { UPDATE t SET hits += $i WHERE n >= $i AND n < $i + 2 }",
		"LET $x = 5; FOR $i IN 0..3 { LET $x = $i; UPDATE t SET seen = $x WHERE n = $x }",
		"LET $tb = type::table('u'); FOR $i IN 0..2 { LET $tb = type::table('t'); UPDATE $tb SET last = $i }",
		"FOR $i IN 0..3 { UPSERT type::record('t', $i + 10) SET n = $i RETURN n }",
	] {
		let statements = [body, "SELECT * FROM t ORDER BY n", "SELECT * FROM u"];
		golden.record(body, render_results(&run(setup, &statements).0));
	}
	golden.check();
}

/// How many statements `control` runs, in the order the block runs them.
fn statements_of(control: &super::plan::Control) -> usize {
	use super::plan::Control;
	match control {
		Control::Seq(stmts) => stmts.iter().map(statements_of).sum(),
		Control::Let {
			value,
			..
		}
		| Control::Return {
			value,
		}
		| Control::Throw {
			value,
		} => statements_of(value),
		Control::For {
			range,
			body,
			..
		} => statements_of(range) + statements_of(body),
		Control::If {
			branches,
			otherwise,
		} => {
			branches
				.iter()
				.map(|(condition, branch)| statements_of(condition) + statements_of(branch))
				.sum::<usize>()
				+ otherwise.iter().map(|branch| statements_of(branch)).sum::<usize>()
		}
		Control::Statement {
			..
		} => 1,
		Control::Value(_)
		| Control::Sleep {
			..
		}
		| Control::Break
		| Control::Continue => 0,
	}
}

/// A block name the block's statements read only through the program's own
/// instructions is bound in a slot alone; one something else may read — a
/// write's stages, a compiled expression — is bound in the frame too.
#[tokio::test]
async fn flat_block_names_bind_in_slots() {
	use super::program::{BlockInstr, Instr};
	use super::tests::{bind, planner};
	let db = TestDb::new("DEFINE TABLE t; DEFINE FIELD n ON t TYPE int;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (sql, in_frame) in [
		(
			"{ LET $x = 1; FOR $i IN 0..3 { LET $y = $i + $x; SELECT * FROM t WHERE n = $y } }",
			false,
		),
		("FOR $i IN 0..3 { CREATE t SET n = $i }", true),
		("FOR $i IN 0..3 { SELECT * FROM t WHERE n = string::len(<string> $i) }", true),
	] {
		let bound = bind(&ctx, sql).await;
		let physical = super::map::map(&planner(&ctx), &bound).await.expect("maps");
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		let binds: Vec<bool> = program
			.pipelines
			.iter()
			.flat_map(|p| &p.code)
			.filter_map(|i| match i {
				Instr::Block(BlockInstr::Bind {
					frame,
					..
				}) => Some(*frame),
				_ => None,
			})
			.collect();
		assert!(!binds.is_empty(), "`{sql}`");
		assert!(binds.iter().all(|frame| *frame == in_frame), "`{sql}`: {binds:?}");
		let slot_reads = program
			.pipelines
			.iter()
			.flat_map(|p| &p.code)
			.filter(|i| matches!(i, Instr::LoadSlot { .. }))
			.count();
		assert!(in_frame || slot_reads > 0, "`{sql}` reads no slot");
	}
}

/// A block's statements are bound and mapped with the program into the
/// control flow of its physical plan, and its loops lower to jumps inside one
/// program: every statement is a group of the program, lowered when the query
/// arrives. A statement reading the clock before the block's first schema
/// statement is bound then, when the block is planned; a statement nesting a statement is compiled
/// with the program like any other, and a `LET` of an expression binds the value the control flow
/// evaluates.
#[tokio::test]
async fn a_block_is_control_flow_over_statements_planned_with_it() {
	use super::tests::{bind, planner};
	let db = TestDb::new("DEFINE TABLE t; DEFINE FIELD n ON t TYPE int;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (sql, statements) in [
		("FOR $i IN 0..3 { CREATE t SET n = $i }", 1),
		("{ LET $x = 1; FOR $i IN 0..3 { LET $y = $i + $x; CREATE t SET n = $y } }", 1),
		("FOR $i IN 0..3 { FOR $j IN 0..3 { UPDATE t SET n = $i * $j WHERE n = $j } }", 1),
		("FOR $i IN 0..3 { CREATE t SET n = time::unix() }", 1),
		("FOR $i IN 0..3 { CREATE t SET n = (SELECT VALUE n FROM t LIMIT 1)[0] }", 1),
		// A statement's tail is compiled with it.
		("FOR $i IN 0..3 { LET $x = SELECT * FROM ONLY t:1; CREATE t SET n = $i TIMEOUT 1s }", 2),
		// A `BREAK` with no loop of the program around it leaves it as a
		// signal.
		("{ IF rand::bool() { BREAK }; CREATE t SET n = 1 }", 1),
	] {
		let bound = bind(&ctx, sql).await;
		let physical = super::map::map(&planner(&ctx), &bound).await.expect("maps");
		let super::plan::Plan::Control {
			control,
		} = &physical.root
		else {
			panic!("`{sql}` is no control flow: {:?}", physical.root);
		};
		assert_eq!(statements_of(control), statements, "`{sql}`");
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		// One control pipeline, whatever the nesting: each loop an iterator
		// slot of its own and a jump back to its head. Every other pipeline
		// is a statement's, compiled into the program.
		let control: Vec<_> = program.pipelines.iter().filter(|p| p.group.is_none()).collect();
		assert_eq!(control.len(), 1, "`{sql}`");
		// Every statement mapped with the program is compiled into it.
		assert_eq!(program.groups.len(), statements, "`{sql}`");
		let loops = sql.matches("FOR ").count();
		let heads = control[0]
			.code
			.iter()
			.filter(|i| {
				matches!(
					i,
					super::program::Instr::Block(super::program::BlockInstr::IterNext { .. })
				)
			})
			.count();
		assert_eq!(heads, loops, "`{sql}`");
	}
	// A schema statement every iteration runs, which raises after the first,
	// before a write reading what it defines: the block is declined whole.
	let sql = "FOR $i IN 0..3 { DEFINE FIELD m ON t VALUE 1; CREATE t SET n = $i }";
	let bound = bind(&ctx, sql).await;
	let mapped = super::map::map(&planner(&ctx), &bound).await;
	assert_eq!(
		mapped.err().map(|why| why.reason),
		Some(super::speculate::DATA_DEPENDENT_DDL.to_owned()),
		"`{sql}`"
	);
	// A statement after a schema statement whose binding folds the clock is
	// bound when the block reaches it: the block is not mapped.
	let sql = "{ DEFINE FIELD m ON t VALUE 1; SELECT * FROM t WHERE n < time::unix() }";
	let bound = bind(&ctx, sql).await;
	assert_eq!(
		super::map::map(&planner(&ctx), &bound).await.err().map(|why| why.reason),
		Some("a block statement after a schema statement whose binding reads the clock".to_owned()),
		"`{sql}`"
	);
	// A statement holding a schema statement inside it is not covered, and
	// neither is the statement holding the block.
	let sql =
		"FOR $i IN 0..3 { CREATE t SET n = $i, d = (DEFINE FIELD OVERWRITE m ON t VALUE $i) }";
	let bound = bind(&ctx, sql).await;
	assert_eq!(
		super::map::map(&planner(&ctx), &bound).await.err().map(|why| why.reason),
		Some("a block statement holding a schema statement".to_owned()),
		"`{sql}`"
	);
	// A membership the logical rules lift into a semi join is mapped as the
	// membership it was lifted from: the statement is a group of the program,
	// and the subquery a group nested in it.
	let sql =
		"FOR $i IN 0..3 { LET $r = SELECT VALUE n FROM t WHERE n IN (SELECT VALUE n FROM t) }";
	let bound = bind(&ctx, sql).await;
	let physical = super::map::map(&planner(&ctx), &bound).await.expect("maps");
	let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
	assert_eq!(program.groups.len(), 2, "`{sql}`");
	let block_steps: Vec<_> = program
		.pipelines
		.iter()
		.flat_map(|p| &p.code)
		.filter_map(|i| match i {
			super::program::Instr::Block(
				step @ (super::program::BlockInstr::RunEmbedded {
					..
				}
				| super::program::BlockInstr::Nested {
					..
				}
				| super::program::BlockInstr::Member {
					..
				}),
			) => Some(std::mem::discriminant(step)),
			_ => None,
		})
		.collect();
	assert_eq!(block_steps.len(), 2, "`{sql}`: {block_steps:?}");
	assert_ne!(block_steps[0], block_steps[1], "`{sql}` runs one statement and nests one");
	// An analysed statement rendered as JSON maps with the block.
	let sql = "FOR $i IN 0..3 { LET $r = (EXPLAIN ANALYZE FORMAT JSON SELECT * FROM t) }";
	let bound = bind(&ctx, sql).await;
	assert!(super::map::map(&planner(&ctx), &bound).await.is_ok(), "`{sql}`");
	ctx.txn().cancel().await.expect("cancel");
}

/// A statement reading the clock is bound once, when the block arrives —
/// before the block's first schema statement, and in a loop with none — and the
/// block runs on the VM, compiling nothing once it runs. After a schema
/// statement whose binding would fold the clock the statement is planned
/// late-bound, its clock read evaluated when it runs: it reads the clock as it
/// stands after the write before it, and each late-bound statement runs as
/// served for the catalog the block meets.
#[test]
fn a_statement_reading_the_clock_is_bound_when_the_block_arrives_or_reaches_it() {
	let mut golden =
		Golden::new("a_statement_reading_the_clock_is_bound_when_the_block_arrives_or_reaches_it");
	let setup = "DEFINE TABLE t; DEFINE FIELD n ON t TYPE int;";
	let count = "SELECT VALUE count() FROM t GROUP ALL";
	for (body, late) in [
		("FOR $i IN 0..3 { CREATE t SET n = time::unix() }", false),
		(
			"FOR $i IN 0..3 { CREATE t SET n = $i, at = time::now(); SELECT * FROM t WHERE at < time::now() }",
			false,
		),
		(
			"{ DEFINE FIELD OVERWRITE m ON t VALUE 1; CREATE t:9 SET n = 1, at = time::now(); \
			   RETURN SELECT VALUE id FROM t WHERE at < time::now() }",
			true,
		),
	] {
		let statements = [body, count];
		let (vm, programs) = run(setup, &statements);
		assert!(vm[0].is_ok(), "`{body}`: {vm:?}");
		golden.record(body, render_results(&vm[1..]));
		// A late-bound statement runs as served from the compile cache for the
		// catalog the block meets, a program of its own: the CREATE and the
		// SELECT after the schema statement.
		let served = if late {
			2
		} else {
			0
		};
		assert_eq!(programs, 2 + served, "`{body}` did not run as one program");
		if late {
			// The clock the late-bound statement reads is the one after the
			// write that stamped the record.
			assert!(format!("{:?}", vm[0]).contains("Number(9)"), "`{body}`: {vm:?}");
		}
	}
	golden.check();
}

/// The blocks [`a_read_by_data_reads_the_catalog_the_block_leaves_and_compiles_nothing`]
/// runs, each with whether the VM lowers it whole: a read of a table only the
/// data names reads a table the block defines by writing to it. The
/// program's dispatch loads each table's read unit when the run reads it, so
/// a record a value in `FROM` names is read as the catalog stands after the
/// block's schema statements before it; a block whose schema statements
/// change what a value only the data names, or a `FETCH`, may read is
/// declined whole by a lowering that models them ahead, and planned with what
/// follows them late-bound.
const READ_BY_DATA: &[(&str, bool)] = &[
	(
		"{ CREATE fresh:1 SET n = 1; LET $t = (SELECT VALUE 'fresh' FROM ONLY t:1); RETURN SELECT * FROM type::table($t) }",
		true,
	),
	(
		"{ CREATE fresh:1; FOR $i IN ['t', 'fresh'] { CREATE log SET r = (SELECT * FROM type::table($i)) }; RETURN SELECT VALUE r FROM log ORDER BY r }",
		true,
	),
	(
		"{ LET $a = SELECT * FROM [t:1]; DEFINE FIELD c ON t COMPUTED 7; RETURN [$a, (SELECT * FROM [t:1])] }",
		true,
	),
	// An index changes no row a read of its table yields.
	(
		"{ DEFINE INDEX by_n ON t FIELDS n; LET $t = (SELECT VALUE 't' FROM ONLY t:1); RETURN SELECT * FROM type::table($t) }",
		true,
	),
	(
		"{ DEFINE TABLE fresh; CREATE fresh:1; LET $t = (SELECT VALUE 'fresh' FROM ONLY t:1); RETURN SELECT * FROM type::table($t) }",
		false,
	),
	(
		"{ LET $t = (SELECT VALUE 't' FROM ONLY t:1); DEFINE FIELD c ON t COMPUTED 7; RETURN SELECT * FROM type::table($t) }",
		false,
	),
	(
		"{ DEFINE FIELD c ON t COMPUTED 7; LET $r = (SELECT VALUE id FROM ONLY t:1); RETURN SELECT * FROM $r }",
		false,
	),
	(
		"{ DEFINE FIELD c ON t COMPUTED 7; CREATE log:1 SET p = t:1; RETURN SELECT * FROM log FETCH p }",
		false,
	),
];

const READ_BY_DATA_SETUP: &str =
	"DEFINE TABLE t; DEFINE FIELD n ON t TYPE int; CREATE t:1 SET n = 1;";

/// Each of [`READ_BY_DATA`] answers its rows. One the VM lowers whole
/// compiles nothing once it runs; one whose schema statements change what its
/// reads by data find plans them late-bound, and their first read of the table
/// compiles what its new catalog version holds, a compile-cache miss.
#[test]
fn a_read_by_data_reads_the_catalog_the_block_leaves_and_compiles_nothing() {
	let mut golden =
		Golden::new("a_read_by_data_reads_the_catalog_the_block_leaves_and_compiles_nothing");
	for (body, on_vm) in READ_BY_DATA {
		let (vm, _) = run_compiling(READ_BY_DATA_SETUP, &[body], !on_vm);
		golden.record(body, render_results(&vm));
	}
	golden.check();
}

/// Each of [`READ_BY_DATA`] the VM lowers whole is one program; each other
/// is declined whole, for the schema statement that changes what its reads
/// by data find.
#[tokio::test]
async fn a_read_by_data_after_a_schema_statement_changing_it_is_declined_whole() {
	use super::tests::{bind, planner};
	let db = TestDb::new(READ_BY_DATA_SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (body, on_vm) in READ_BY_DATA {
		let bound = bind(&ctx, body).await;
		let physical = super::map::map(&planner(&ctx), &bound).await.expect("maps");
		let lowered = super::lower::lower(&planner(&ctx), &physical).await;
		match on_vm {
			true => assert!(lowered.is_ok(), "`{body}`: {:?}", lowered.err()),
			false => assert_eq!(
				lowered.err().map(|why| why.reason),
				Some(
					"a block that reads by data the tables its schema statements change".to_owned()
				),
				"`{body}`"
			),
		}
	}
	ctx.txn().cancel().await.expect("cancel");
}

/// `EXPLAIN ANALYZE` of a statement a block runs is a group of the block's
/// program: it renders the plan the VM explains for the statement, as the
/// same `EXPLAIN ANALYZE` at the top of a query renders it, with what the
/// statement's run measured.
#[test]
fn an_analyzed_block_statement_renders_what_a_top_level_one_renders() {
	let setup =
		"DEFINE TABLE t; DEFINE FIELD n ON t TYPE int; CREATE t:1 SET n = 1; CREATE t:2 SET n = 2;";
	for statement in
		["SELECT * FROM t", "SELECT VALUE n FROM t WHERE n > 1", "SELECT * FROM ONLY t:1"]
	{
		let analyzed = format!("EXPLAIN ANALYZE {statement}");
		let in_block = format!("{{ LET $x = 1; RETURN ({analyzed}) }}");
		let (block, programs) = run(setup, &[&in_block]);
		assert_eq!(programs, 1, "`{statement}` did not run as one program");
		let (top, _) = run(setup, &[&analyzed]);
		let text = |answer: &[Result<Value, String>]| match answer {
			[Ok(Value::String(text))] => text.as_str().to_owned(),
			other => panic!("`{statement}` answered {other:?}"),
		};
		let (block, top) = (text(&block), text(&top));
		// The plan's lines without their metrics, which time the run, and
		// the rows the run yielded.
		let shape = |text: &str| -> Vec<String> {
			text.lines().map(|line| line.split(" {").next().unwrap_or(line).to_owned()).collect()
		};
		assert_eq!(shape(&block), shape(&top), "`{statement}`");
		assert_eq!(block.lines().last(), top.lines().last(), "`{statement}`");
	}
}
