//! Destructures, `@`, mocks and projection functions called for their value
//! are instructions and host steps of the program, and answer their values,
//! for the owner and for a record user whose reads are checked.

use super::golden::Golden;
use super::tests::{bind, outcome, planner};
use crate::exec::ExecutionContext;
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	DEFINE TABLE person PERMISSIONS FOR select WHERE hidden != true;
	DEFINE TABLE comment PERMISSIONS FULL;
	DEFINE FIELD pw ON person PERMISSIONS FOR select WHERE $auth.id = user:x;
	CREATE person:1 SET name = 'a', n = 1, pw = 'p1', tags = ['x', 'y'], nest = { c: 1, d: 2 }, friend = person:2;
	CREATE person:2 SET name = 'b', n = 2, hidden = true, friend = person:3;
	CREATE person:3 SET name = 'c', n = 3, pw = 'p3', nest = { c: 3 }, friend = person:1;
	CREATE comment:1 SET author = person:1, text = 'one';
	CREATE comment:2 SET author = person:2, text = 'two';
	CREATE comment:3 SET author = person:9, text = 'three';
";

/// Statements over the paths, each a program of instructions alone.
const READS: &[&str] = &[
	"SELECT VALUE id.{ name, n } FROM person ORDER BY id",
	"SELECT VALUE id.{ name, pw } FROM person ORDER BY id",
	"[person:1, person:2, person:3, person:9].{ name, x: n * 2 }",
	"(SELECT * FROM person ORDER BY id).{ id, label: name + '!' }",
	"SELECT VALUE author.{ name, pw } FROM comment ORDER BY id",
	"SELECT VALUE $this.{ author.*, text } FROM comment ORDER BY id",
	"SELECT VALUE { text: text, a: author.{ name } } FROM comment ORDER BY id",
	"person:1.{ nest.{ c, e }, tags }",
	"{ a: NONE, b: [1, { c: 2 }] }.{ a, b, z }",
	"[1, [person:1, 'x'], { name: 'o' }].{ name }",
	"5.{ a }",
	"SELECT VALUE tags.{ a } FROM person ORDER BY id",
	"SELECT VALUE [type::field('name'), type::field('nest.c')] FROM person ORDER BY id",
	"SELECT VALUE id FROM person WHERE type::field('n') > 1 ORDER BY id",
	"SELECT VALUE [type::fields(['name', 'n'])] FROM person ORDER BY id",
	"SELECT VALUE [@.id, n] FROM person ORDER BY id",
	"SELECT VALUE [@.name, n] FROM person ORDER BY id",
	"SELECT VALUE [author, @.author.name] FROM comment ORDER BY id",
	"SELECT VALUE comments[WHERE @.id = comment:1] FROM (SELECT id, (SELECT * FROM comment) AS comments FROM person:1)",
	"SELECT VALUE nest.?.c FROM person ORDER BY id",
	"SELECT VALUE [nest.?.c, nest.?.d.e, pw.?.len()] FROM person ORDER BY id",
	"SELECT VALUE author.?.name FROM comment ORDER BY id",
	"[NONE, NULL, { a: 1 }, 5].map(|$v| $v.?.a)",
	"{ LET $o = { inc: |$x| $x + 1, v: 2 }; RETURN [$o.inc(1), $o.inc($o.v)] }",
	"{ LET $o = { v: 2 }; RETURN $o.nope(1) }",
	"RETURN (5).nope()",
	"RETURN { f: |$x| $x * 2 }.f(3)",
	"SELECT * FROM (BREAK)",
	"RETURN [1, CONTINUE]",
	"FOR $i IN [1, 2, 3] { IF $i = 2 { BREAK }; CREATE person SET n = $i; }",
	"{ FOR $i IN [1, 2, 3] { LET $x = [$i, IF $i = 2 { CONTINUE } ELSE { $i }]; }; RETURN 1 }",
	"RETURN person:1.{1..3}.friend",
	"RETURN person:1.{..}.friend",
	"RETURN person:1.{1..3+collect}.friend",
	"RETURN person:1.{1..3+collect+inclusive}.friend",
	"RETURN person:1.{1..3+path}.friend",
	"RETURN person:1.{..+shortest=person:3}.friend",
	"RETURN person:1.{2}.friend.name",
	"RETURN person:1.{1..2}(.friend).name",
	"SELECT VALUE id.{1..3+collect}.friend FROM person ORDER BY id",
	"RETURN person:1.{1..4}.{ id, next: friend }.next",
	"RETURN |person:1..3|",
	"RETURN array::len(|t:5|)",
	"SELECT VALUE [|t:1..2|, n] FROM person ORDER BY id",
	"RETURN |t:100000000000|",
];

fn reader() -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

async fn lowered(ctx: &ExecutionContext, sql: &str) -> super::program::Program {
	let bound = bind(ctx, sql).await;
	let physical = super::optimize::optimize(
		super::map::map(&planner(ctx), &bound).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}")),
	);
	super::lower::lower(&planner(ctx), &physical).await.unwrap_or_else(|e| panic!("`{sql}`: {e:?}"))
}

#[tokio::test]
async fn paths_as_instructions_answer_their_values() {
	let mut golden = Golden::new("paths_as_instructions_answer_their_values");
	let db = TestDb::new_with_auth(SETUP).await;
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in READS {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}

/// Recursions, each strategy with its bounds, over a ring with a dead end and
/// a value that is no record id, so that the errors the strategies raise are
/// answered too.
const RECURSIONS: &[&str] = &[
	// default
	"RETURN person:1.{1..3}.friend",
	"RETURN person:1.{3}.friend",
	"RETURN person:1.{2}(.friend).name",
	"RETURN person:1.{5..9}.friend",
	"RETURN person:1.{..}.friend",
	"RETURN person:1.{1..2}(.friend.friend)",
	"RETURN person:1.{1..3}(.n)",
	"RETURN person:9.{1..3}.friend",
	"RETURN person:4.{1..3}.friend",
	"SELECT VALUE id.{1..3}.friend FROM person ORDER BY id",
	"SELECT VALUE id.{1..2}(.friend).name FROM person ORDER BY id",
	// collect
	"RETURN person:1.{1..3+collect}.friend",
	"RETURN person:1.{1..3+collect+inclusive}.friend",
	"RETURN person:1.{2..3+collect}.friend",
	"RETURN person:1.{..+collect}.friend",
	"RETURN person:1.{1..3+collect}(.n)",
	"RETURN person:4.{1..3+collect}.friend",
	"SELECT VALUE id.{1..3+collect}.friend FROM person ORDER BY id",
	// path
	"RETURN person:1.{1..3+path}.friend",
	"RETURN person:1.{1..3+path+inclusive}.friend",
	"RETURN person:1.{2..3+path}.friend",
	"RETURN person:1.{..+path}.friend",
	"RETURN person:1.{1..3+path}(.n)",
	"SELECT VALUE id.{1..3+path}.friend FROM person ORDER BY id",
	// shortest
	"RETURN person:1.{..+shortest=person:3}.friend",
	"RETURN person:1.{..+shortest=person:3+inclusive}.friend",
	"RETURN person:1.{1..2+shortest=person:3}.friend",
	"RETURN person:1.{1..3+shortest=person:77}.friend",
	"RETURN person:1.{..+shortest=person:3}(.n)",
	// a recursion in the body of another
	"RETURN person:1.{1..2}(.friend.{1..2}(.friend))",
	"RETURN person:1.{1..2+collect}(.friend.{1..2+collect}(.friend))",
	"SELECT VALUE id.{1..2}(.friend.{1..2}(.friend)) FROM person ORDER BY id",
];

#[tokio::test]
async fn a_recursion_answers_its_values_under_each_strategy() {
	let mut golden = Golden::new("a_recursion_answers_its_values_under_each_strategy");
	let db = TestDb::new_with_auth(SETUP).await;
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in RECURSIONS {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}

/// Whether `program` holds a recursion whose body is a unit of its own table,
/// and no unit holds units of its own.
fn recursion_bodies_are_units(program: &super::program::Program) -> bool {
	use super::program::Instr;
	let units = &program.units;
	let recursions: Vec<_> = std::iter::once(program)
		.chain(units.iter().map(|unit| unit.as_ref()))
		.flat_map(|p| p.pipelines.iter().chain(&p.host_code))
		.flat_map(|pipeline| &pipeline.code)
		.filter_map(|instr| match instr {
			Instr::Recurse {
				spec,
				..
			} => Some(spec),
			_ => None,
		})
		.collect();
	!recursions.is_empty()
		&& recursions.iter().all(|spec| spec.body.is_none_or(|id| (id.0 as usize) < units.len()))
		&& units.iter().all(|unit| unit.units.is_empty())
}

#[tokio::test]
async fn a_recursion_body_is_a_unit_of_the_root_program() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in RECURSIONS {
		let program = lowered(&ctx, sql).await;
		assert!(recursion_bodies_are_units(&program), "`{sql}`");
	}
	let nested = lowered(&ctx, "RETURN person:1.{1..2}(.friend.{1..2}(.friend))").await;
	assert_eq!(nested.units.len(), 2, "the inner body is a unit of the root, not of the outer");
	assert!(nested.units.iter().all(|unit| unit.units.is_empty() && unit.unit.is_some()));
}

/// The outcome of the recursion `sql` over `program` edited by `edit`, run in
/// `ctx`.
async fn run_with_unit_frame(
	ctx: &ExecutionContext,
	sql: &str,
	edit: impl FnOnce(&mut super::program::UnitFrame),
) -> Result<Vec<crate::val::Value>, String> {
	let mut program = lowered(ctx, sql).await;
	let units = std::sync::Arc::get_mut(&mut program.units).expect("a program's own units");
	let unit = std::sync::Arc::get_mut(&mut units[0]).expect("a unit held by its root alone");
	edit(&mut unit.unit.as_mut().expect("a unit holds its frame").frame);
	let op: std::sync::Arc<dyn crate::exec::ExecOperator> =
		std::sync::Arc::new(super::host::VmOperator::new(program));
	outcome(&op, ctx).await
}

#[tokio::test]
async fn a_unit_run_under_a_frame_other_than_its_own_fails_closed() {
	let db = TestDb::new_with_auth(SETUP).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	let sql = "SELECT VALUE id.{1..3}.friend FROM person ORDER BY id";
	let untouched = run_with_unit_frame(&ctx, sql, |_| {}).await;
	assert!(untouched.is_ok(), "{untouched:?}");
	let refused = |outcome: Result<Vec<crate::val::Value>, String>, what: &str| {
		let Err(message) = outcome else {
			panic!("a unit lowered for another {what} ran");
		};
		assert!(message.contains("lowered for a frame other than"), "{what}: {message}");
	};
	refused(run_with_unit_frame(&ctx, sql, |frame| frame.class_digest ^= 2).await, "principal");
	refused(
		run_with_unit_frame(&ctx, sql, |frame| frame.ns = Some("elsewhere".to_owned())).await,
		"namespace",
	);
	refused(
		run_with_unit_frame(&ctx, sql, |frame| frame.db = Some("elsewhere".to_owned())).await,
		"database",
	);
	// A unit lowered with enforcement suppressed runs in no frame that
	// enforces it, and one lowered for an unchecked frame in no checked one.
	refused(
		run_with_unit_frame(&ctx, sql, |frame| frame.perms_suppressed = true).await,
		"suppression",
	);
	refused(run_with_unit_frame(&ctx, sql, |frame| frame.checked = false).await, "check");
	// A unit lowered for no principal serves any.
	let anyone = run_with_unit_frame(&ctx, sql, |frame| frame.class_digest = 0).await;
	assert_eq!(anyone, untouched);
}

/// Recursions and destructures over many rows. Run with `cargo test --release -p surrealdb-core
/// --lib expr2_path_throughput -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "a measurement, not a check"]
async fn expr2_path_throughput() {
	let db = TestDb::new_with_auth(
		"DEFINE TABLE person SCHEMALESS; DEFINE TABLE knows TYPE RELATION;
		 FOR $i IN 1..=2000 { CREATE type::record('person', $i) SET n = $i, friend = type::record('person', ($i % 2000) + 1), tags = [1, 2, 3]; };
		 FOR $i IN 1..=2000 { LET $a = type::record('person', $i); LET $b = type::record('person', ($i % 2000) + 1); RELATE $a->knows->$b; };",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for sql in [
		"SELECT VALUE id.{1..4}.friend FROM person",
		"SELECT VALUE id.{1..4+collect}.friend FROM person",
		"SELECT VALUE id.{1..3+path}->knows->person FROM person",
		"SELECT VALUE id.{ n, f: friend.n } FROM person",
		"SELECT VALUE friend.?.n FROM person",
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
