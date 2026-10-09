//! `IF` and `THROW` as control flow of a block, and schema statements
//! compiled into the block's program.

use super::golden::{Golden, render_outcome};
use super::program::{BlockInstr, Instr};
use super::tests::{bind, defined, parse, planner, render_defined, render_written, written_as};
use crate::exec::operators::test_util::TestDb;

/// `IF`, `THROW` and schema statements in a block's statements' places, each
/// run as [`defined`] runs a statement: the answer and the keyspace left.
const CTL_STATEMENTS: &[&str] = &[
	// `IF` in a statement's place: its value, its frame, its signals.
	"IF true { CREATE counter:1; RETURN 5 }",
	"IF false { 1 } ELSE { SELECT * FROM person }",
	"IF false { 1 } ELSE IF true { CREATE counter:2 SET n = 2 } ELSE { 3 }",
	"IF false { 1 }",
	"IF true THEN (CREATE counter:3) END",
	"IF true { 1; 2 }",
	"{ IF true { 1; 2 } }",
	"{ IF false { 1 } }",
	"{ IF true { CREATE counter:1 } }",
	"{ IF true { RETURN 1 }; 2 }",
	"{ IF false { RETURN 1 }; 2 }",
	"{ LET $x = 1; IF true { LET $x = 2; CREATE counter:1 SET x = $x }; RETURN [$x, (SELECT VALUE x FROM counter)] }",
	"{ LET $x = 1; IF true { { LET $x = 3 }; LET $y = $x }; RETURN $x }",
	"{ IF true { IF true { RETURN 'deep' } }; 'shallow' }",
	"{ FOR $i IN 0..5 { IF $i = 1 { CONTINUE } ELSE IF $i = 3 { BREAK }; CREATE type::record('counter', $i) }; RETURN SELECT VALUE id FROM counter ORDER BY id }",
	"{ FOR $i IN 0..5 { IF $i % 2 = 0 { LET $j = $i * 10; { IF $j = 20 { CONTINUE } }; CREATE type::record('counter', $j) } }; RETURN SELECT VALUE id FROM counter ORDER BY id }",
	"{ FOR $i IN 0..4 { IF $i = 2 { RETURN $i } ELSE { CREATE type::record('counter', $i) } }; RETURN 'late' }",
	"{ FOR $i IN 0..3 { FOR $j IN 0..3 { IF $j = 1 { BREAK } }; CREATE type::record('counter', $i) }; RETURN SELECT VALUE id FROM counter ORDER BY id }",
	"{ FOR $i IN 0..3 { IF $i = 1 THEN BREAK END; CREATE type::record('counter', $i) }; RETURN SELECT VALUE id FROM counter }",
	"{ LET $v = IF true { 'let' } ELSE { 'else' }; RETURN $v }",
	"{ IF (SELECT VALUE existing FROM ONLY person:1) = 'a' { RETURN 'read' }; 'unread' }",
	// `THROW` in a statement's place.
	"THROW 'plain'",
	"{ CREATE counter:1; THROW 'after' }",
	"{ FOR $i IN 0..3 { IF $i = 1 { THROW 'at ' + <string> $i }; CREATE type::record('counter', $i) } }",
	"{ THROW { code: 7 } }",
	// Schema statements compiled into the program.
	"{ DEFINE TABLE fresh; CREATE fresh:1 SET n = 1; RETURN SELECT * FROM fresh }",
	"{ DEFINE FIELD m ON person VALUE 7; UPDATE person:1 SET existing = 'b'; RETURN SELECT * FROM person }",
	"{ DEFINE TABLE fresh; DEFINE TABLE fresh }",
	"{ DEFINE TABLE IF NOT EXISTS fresh; DEFINE TABLE IF NOT EXISTS fresh; INFO FOR TABLE fresh }",
	"{ REMOVE TABLE person; RETURN SELECT * FROM person }",
	"{ DEFINE INDEX by_n ON counter FIELDS n; FOR $i IN 0..4 { CREATE type::record('counter', $i) SET n = $i % 2 }; RETURN SELECT VALUE id FROM counter WHERE n = 1 ORDER BY id }",
	"{ FOR $i IN 0..3 { DEFINE FIELD OVERWRITE m ON counter VALUE $i; CREATE type::record('counter', $i) }; RETURN SELECT * FROM counter ORDER BY id }",
	"{ FOR $i IN 0..3 { IF $i = 1 { DEFINE FIELD m ON counter VALUE 'one' }; CREATE type::record('counter', $i) }; RETURN SELECT * FROM counter ORDER BY id }",
	"{ FOR $i IN 0..3 { IF $i = 1 { DEFINE TABLE later; CREATE later:1 } ELSE { CREATE type::record('counter', $i) } }; RETURN [(SELECT * FROM later), (SELECT * FROM counter ORDER BY id)] }",
	"IF true { DEFINE TABLE fresh; CREATE fresh:1; RETURN SELECT * FROM fresh }",
	"{ DEFINE PARAM $p VALUE 3; RETURN $p }",
	"{ DEFINE FUNCTION fn::twice($x: int) { $x * 2 }; RETURN fn::twice(4) }",
	"{ INFO FOR DB; DEFINE TABLE fresh; RETURN (INFO FOR DB).tables }",
	// The block's own expressions holding statements, after a schema
	// statement: each reads the catalog as the block has left it.
	"{ DEFINE FIELD c ON person COMPUTED 'computed'; RETURN (SELECT VALUE c FROM person) }",
	"{ DEFINE FIELD c ON person COMPUTED 'computed'; RETURN SELECT VALUE c FROM person }",
	"{ DEFINE FIELD c ON person COMPUTED 3; FOR $x IN (SELECT VALUE c FROM person) { CREATE counter SET n = $x }; RETURN SELECT VALUE n FROM counter }",
	"{ DEFINE FIELD c ON person COMPUTED 3; IF (SELECT VALUE c FROM ONLY person:1) = 3 { RETURN 'seen' }; 'unseen' }",
	"{ DEFINE FIELD c ON person COMPUTED 3; LET $v = (SELECT VALUE c FROM ONLY person:1); RETURN $v }",
	"{ DEFINE FIELD c ON person COMPUTED 3; THROW (SELECT VALUE c FROM ONLY person:1) }",
	"{ DEFINE FIELD c ON person COMPUTED 3; (SELECT VALUE c FROM ONLY person:1) + 1 }",
	"{ FOR $i IN 0..3 { IF (SELECT VALUE count() FROM ONLY counter GROUP ALL LIMIT 1) = 2 { BREAK }; CREATE counter SET n = $i }; RETURN count(SELECT * FROM counter) }",
	"{ FOR $i IN (SELECT VALUE existing FROM person) { CREATE counter SET e = $i }; RETURN SELECT VALUE e FROM counter }",
	// A statement reading a table an earlier write defines.
	"{ CREATE fresh:1 SET n = 1; RETURN SELECT * FROM fresh }",
	"{ FOR $i IN 0..3 { CREATE type::record('fresh', $i) SET n = $i; LET $c = SELECT VALUE n FROM fresh ORDER BY n; CREATE type::record('seen', $i) SET c = $c }; RETURN SELECT * FROM seen ORDER BY id }",
	"{ IF false { CREATE fresh:1 }; RETURN SELECT * FROM fresh }",
	// Constants a `LET` binds, read as written where nothing binds them again.
	"{ LET $x = 2; LET $y = $x; FOR $i IN 0..$x { CREATE type::record('counter', $i) SET y = $y }; RETURN SELECT * FROM counter ORDER BY id }",
	"{ LET $debug = false; IF $debug { CREATE counter:1 } ELSE { CREATE counter:2 }; RETURN SELECT VALUE id FROM counter }",
	"{ LET $on = true; IF $on { CREATE counter:1 } ELSE IF $on { CREATE counter:2 } ELSE { CREATE counter:3 }; RETURN SELECT VALUE id FROM counter }",
	"{ LET $n = 1; { LET $n = 5; CREATE counter SET n = $n }; CREATE counter SET m = $n; RETURN SELECT n, m FROM counter ORDER BY n, m }",
	"{ LET $x = 1; LET $f = |$y: int| $x + $y; { LET $x = 2; RETURN $f(1) } }",
	"{ LET $x = 1; FOR $x IN [5] { CREATE counter SET x = $x }; RETURN [$x, (SELECT VALUE x FROM counter)] }",
	"{ LET $v = 'a'; RETURN SELECT * FROM person WHERE existing = $v }",
	"{ LET $x = 3; LET $x = $x + 1; RETURN $x }",
	"{ LET $x: string = 1; RETURN $x }",
	"{ LET $t = 'person'; RETURN SELECT VALUE existing FROM type::table($t) }",
	"{ LET $x = 1; RETURN (SELECT VALUE $x FROM person) }",
	"{ LET $c = true; IF $c { LET $c = false; IF $c { RETURN 'inner' } }; RETURN $c }",
	"{ LET $x = 1; RETURN (SELECT VALUE { LET $x = 2; $x } FROM person) }",
	"{ LET $x = [1, 2]; FOR $i IN $x { CREATE type::record('counter', $i) }; RETURN SELECT VALUE id FROM counter ORDER BY id }",
	"{ LET $x = NONE; IF $x { 1 } ELSE { 2 } }",
	// A statement nested in a loop's statements, kept across iterations only
	// where no iteration can change what it reads.
	"{ FOR $i IN 0..3 { LET $r = SELECT VALUE existing FROM person WHERE existing IN (SELECT VALUE existing FROM person); IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT a, (SELECT VALUE a FROM indexed) AS all FROM indexed; IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT a, (SELECT VALUE a FROM indexed) AS all FROM indexed; CREATE type::record('indexed', $i + 10) SET a = $i; IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT a, (SELECT VALUE a FROM indexed WHERE a <= $i) AS all FROM indexed; IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..2 { FOR $j IN 0..2 { LET $r = SELECT a, (SELECT VALUE a FROM indexed WHERE a <= $i) AS all FROM indexed; IF $i = 1 { RETURN [$j, $r] } } } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT VALUE a FROM indexed WHERE a IN (SELECT VALUE a + $i FROM indexed); IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT VALUE a FROM indexed WHERE a IN (SELECT VALUE a FROM indexed); CREATE type::record('indexed', $i + 10) SET a = $i + 10; IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { LET $j = $i; LET $r = SELECT VALUE a FROM indexed WHERE a IN (SELECT VALUE a + $j FROM indexed); IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { FOR $j IN 0..2 { LET $r = SELECT VALUE a FROM indexed WHERE a IN (SELECT VALUE a FROM indexed WHERE a < $i + 1); IF $i = 2 { RETURN [$j, $r] } } } }",
	"{ FOR $i IN 0..3 { LET $r = SELECT VALUE a FROM indexed WHERE a IN (SELECT VALUE a FROM indexed); fn::one(); IF $i = 2 { RETURN $r } } }",
	"{ FOR $i IN 0..3 { IF $i = 1 { UPDATE indexed:1 SET a = 7 }; LET $r = SELECT VALUE a FROM indexed WHERE a IN (SELECT VALUE a FROM indexed); IF $i = 2 { RETURN $r } } }",
	// A membership the logical rules lift into a join, beside terms the
	// rules hold above it: judged in the order the predicate was written.
	"{ LET $x = 1; RETURN SELECT VALUE id FROM indexed WHERE a IN (SELECT VALUE a FROM indexed WHERE <int> b > 0) AND false }",
	"{ LET $x = 1; RETURN SELECT VALUE id FROM indexed WHERE a IN (SELECT VALUE a FROM indexed WHERE <int> b > 0) }",
	"{ LET $x = 1; RETURN SELECT VALUE id FROM indexed WHERE a IN (SELECT VALUE a FROM indexed) AND (IF a = 1 THEN THROW 'between' ELSE true END) AND a NOT IN (SELECT VALUE a + 5 FROM indexed) }",
	"{ LET $x = 1; RETURN SELECT VALUE id FROM indexed WHERE a NOT IN (SELECT VALUE a + 5 FROM indexed) AND a IN (SELECT VALUE a FROM indexed) }",
	"{ LET $x = 1; RETURN SELECT VALUE id FROM indexed WHERE (SELECT VALUE a FROM indexed) CONTAINS a }",
	// Kept across iterations whose writes touch other tables, and not across
	// those that write what it reads.
	"{ FOR $i IN 0..3 { LET $r = SELECT a, (SELECT VALUE a FROM indexed) AS all FROM indexed; CREATE type::record('counter', $i) SET r = $r }; RETURN SELECT r FROM counter ORDER BY id }",
	"{ FOR $i IN 0..3 { LET $r = SELECT a, (SELECT VALUE a FROM indexed) AS all FROM indexed; UPDATE indexed:1 SET a = $i + 5; CREATE type::record('counter', $i) SET r = $r }; RETURN SELECT r FROM counter ORDER BY id }",
	"{ FOR $i IN 0..3 { LET $r = SELECT a, (SELECT VALUE existing FROM person) AS all FROM indexed; UPDATE person:1 SET existing = <string> $i; CREATE type::record('counter', $i) SET r = $r }; RETURN SELECT r FROM counter ORDER BY id }",
	// Reads of tables only the data names, of tables the block defines by
	// writing to them, and a value written in `FROM` after a schema statement
	// that changes what a read of its table finds.
	"{ CREATE fresh:1 SET n = 1; LET $t = (SELECT VALUE 'fresh' FROM ONLY person:1); RETURN SELECT * FROM type::table($t) }",
	"{ CREATE fresh:1; FOR $i IN ['person', 'fresh'] { CREATE log SET r = (SELECT * FROM type::table($i)) }; RETURN SELECT r FROM log ORDER BY r }",
	"{ LET $a = SELECT * FROM [person:1]; DEFINE FIELD c ON person COMPUTED 7; RETURN [$a, (SELECT * FROM [person:1])] }",
	// A `RETURN` caught by the `LET` or `RETURN` whose value holds it.
	"{ LET $x = { IF true { CREATE counter:1; RETURN 1 }; 2 }; RETURN $x }",
	"{ LET $x = { FOR $i IN 0..5 { IF $i = 3 { RETURN $i } }; 'none' }; RETURN $x }",
	"{ LET $x = { LET $y = 7; { RETURN $y } }; RETURN [$x, $y] }",
	"{ RETURN { IF true { RETURN 'inner' }; 'outer' } }",
	"{ FOR $i IN 0..4 { LET $x = { IF $i = 2 { BREAK }; $i }; CREATE type::record('counter', $x) }; RETURN SELECT VALUE id FROM counter ORDER BY id }",
	"{ LET $x: int = { RETURN 'five' }; RETURN $x }",
];

#[tokio::test]
async fn control_statements_answer_and_change_the_keyspace() {
	let mut golden = Golden::new("control_statements_answer_and_change_the_keyspace");
	for sql in CTL_STATEMENTS {
		let parsed = parse(sql);
		golden.record(sql, render_defined(&defined((sql, &parsed)).await));
	}
	golden.check();
}

/// A block holding `IF` and schema statements lowers to one program: one
/// control pipeline, every statement a group of it, and nothing planned
/// apart from it. One whose schema statement an iteration decides to run,
/// before writes that read what it defines, is declined whole.
#[tokio::test]
async fn control_statements_compile_into_one_program() {
	let db = TestDb::new("DEFINE TABLE t; DEFINE FIELD n ON t TYPE int;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let declined =
		"FOR $i IN 0..3 { IF $i = 1 { DEFINE FIELD m ON t VALUE 1 }; CREATE t SET n = $i }";
	let bound = bind(&ctx, declined).await;
	// The mapping declines it, before anything is lowered.
	let mapped = super::map::map(&planner(&ctx), &bound).await;
	assert_eq!(
		mapped.err().map(|why| why.reason),
		Some(super::speculate::DATA_DEPENDENT_DDL.to_owned()),
		"`{declined}`"
	);
	for (sql, groups) in [
		(
			"{ DEFINE TABLE u; CREATE u:1; IF rand::bool() { SELECT * FROM u } ELSE { THROW 'no' } }",
			3,
		),
		("IF rand::bool() { CREATE t:1 } ELSE IF rand::bool() { DELETE t } ELSE { 3 }", 2),
		("FOR $i IN 0..3 { IF $i = 1 { BREAK }; THROW 'x' }", 0),
		("{ CREATE u:1; SELECT * FROM u }", 2),
		// A branch a constant `LET` rules out is compiled all the same, so a
		// refusal its statements raise is raised.
		("{ LET $debug = false; IF $debug { CREATE t:1 } ELSE { SELECT * FROM t } }", 2),
		(
			"{ LET $on = true; IF rand::bool() { CREATE t:1 } ELSE IF $on { SELECT * FROM t } ELSE { DELETE t } }",
			3,
		),
	] {
		let bound = bind(&ctx, sql).await;
		let physical = super::map::map(&planner(&ctx), &bound).await.expect("maps");
		assert!(
			matches!(physical.root, super::plan::Plan::Control { .. }),
			"`{sql}` is no control flow"
		);
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		let control: Vec<_> = program.pipelines.iter().filter(|p| p.group.is_none()).collect();
		assert_eq!(control.len(), 1, "`{sql}`");
		assert_eq!(program.groups.len(), groups, "`{sql}`");
		// Every statement the block runs is a group of the program, run by
		// its own instruction.
		let runs = program
			.pipelines
			.iter()
			.flat_map(|p| &p.code)
			.filter(|i| matches!(i, Instr::Block(BlockInstr::RunEmbedded { .. })))
			.count();
		assert_eq!(runs, groups, "`{sql}` runs a statement that is not a group of the program");
		assert!(program.subqueries.is_empty(), "`{sql}` runs a statement as a program of its own");
	}
}

/// A record user's `IF` branches read and write through the table's
/// permissions and field guards, and a schema statement it may not run fails.
#[tokio::test]
async fn control_statements_keep_a_record_users_permissions() {
	let mut golden = Golden::new("control_statements_keep_a_record_users_permissions");
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in [
			"IF true { SELECT * FROM doc ORDER BY id }",
			"{ FOR $i IN 1..3 { IF $i = 2 { UPDATE doc SET n += 1 RETURN AFTER } } }",
			"{ IF true { LET $d = SELECT secret, echoed FROM doc ORDER BY id; RETURN $d } }",
			"{ IF true { DEFINE FIELD extra ON doc VALUE 1 }; RETURN SELECT * FROM doc ORDER BY id }",
			"{ IF (SELECT VALUE secret FROM ONLY doc:1) = 's1' { RETURN 'seen' }; 'hidden' }",
		] {
			let (rows, state) = written_as(sql, &session).await;
			golden.record(format!("{who}: {sql}"), render_written(&rows, &state));
		}
	}
	golden.check();
}

/// A loop whose iterations decide what its body defines, before writes that
/// read it, runs on the VM planning what follows each definition when it
/// reaches it; one whose definitions the block models is planned ahead.
#[tokio::test]
async fn a_loop_defining_what_it_writes_runs_where_it_is_modelled() {
	let statements = [
		"FOR $i IN 0..3 { IF $i = 1 { DEFINE TABLE lt; DEFINE FIELD k ON lt VALUE 7 }; \
		 CREATE type::record('lt', $i) }",
		"FOR $i IN 0..2 { DEFINE FIELD OVERWRITE m ON lt VALUE $i; UPDATE lt SET n = m }",
		"IF true { DEFINE FIELD OVERWRITE o ON lt VALUE 'o'; UPDATE lt:1 }",
		"SELECT * FROM lt ORDER BY id",
	];
	let db = TestDb::new("").await;
	let mut golden = Golden::new("a_loop_defining_what_it_writes_runs_where_it_is_modelled");
	for sql in statements {
		let responses = db.datastore().execute(sql, &TestDb::owner(), None).await.expect("runs");
		let answered: Vec<String> =
			responses.into_iter().map(|r| format!("{:?}", r.result)).collect();
		golden.record(sql, answered.join("\n"));
	}
	golden.check();
}

/// A table whose select permission reads a table the record user may not
/// read, which a permission reads with the caller's permissions bypassed.
const GRANTED_SCHEMA: &str = "
	DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE grant SCHEMALESS PERMISSIONS FOR select NONE;
	DEFINE TABLE item SCHEMALESS PERMISSIONS
		FOR select WHERE (SELECT * FROM grant WHERE holder = $auth.id LIMIT 1);
	CREATE person:bob;
	CREATE grant:g1 SET holder = person:bob;
	FOR $i IN 0..3 { CREATE type::record('item', $i) SET n = $i };
";

/// What `sql` answers for `session` over a fresh [`GRANTED_SCHEMA`].
async fn granted_as(
	sql: &str,
	session: &crate::dbs::Session,
) -> Result<Vec<crate::val::Value>, String> {
	use super::tests::outcome;
	let db = TestDb::new_with_auth(GRANTED_SCHEMA).await;
	let ctx = db.exec_ctx_as(session, crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let rows = outcome(&op, &ctx).await;
	ctx.txn().cancel().await.expect("cancel");
	rows
}

/// A block's statements read through a permission that reads another table
/// as a statement outside a block does: the permission's own read bypasses
/// the caller's permissions.
#[tokio::test]
async fn a_block_statement_reads_through_a_permission_reading_another_table() {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("person", "bob"));
	let bob = crate::dbs::Session::for_record("test", "test", "user", rid);
	let mut golden =
		Golden::new("a_block_statement_reads_through_a_permission_reading_another_table");
	for sql in [
		"SELECT id FROM item",
		"RETURN (SELECT id FROM item)",
		"RETURN SELECT id FROM item",
		"RETURN array::len((SELECT id FROM item))",
		"{ LET $n = SELECT id FROM item; RETURN $n }",
		"IF true { SELECT id FROM item }",
		"FOR $i IN [1] { RETURN SELECT id FROM item }",
	] {
		let rows = granted_as(sql, &bob).await;
		let shown = format!("{rows:?}");
		assert!(shown.contains("Number(2)") || shown.contains("Int(3)"), "`{sql}`: {shown}");
		golden.rows(sql, &rows);
	}
	golden.check();
}

/// A read-only, uncorrelated statement nested in a loop's statements is run
/// once for the loop where it reads no name the loop binds and nothing in the
/// body writes what it reads — with what the catalog says each write and read
/// triggers — and once per run of its statement otherwise.
#[tokio::test]
async fn a_loop_invariant_nested_statement_is_kept_across_iterations() {
	let db = TestDb::new(
		"DEFINE TABLE t; DEFINE TABLE u; CREATE u:1 SET n = 1; CREATE t:1 SET n = 1; \
		 DEFINE TABLE evented; DEFINE EVENT e ON evented THEN (UPDATE u:1 SET n += 1); \
		 DEFINE TABLE guarded PERMISSIONS FOR select WHERE (SELECT VALUE n FROM t:1)[0] > 0; \
		 CREATE guarded:1 SET n = 1;",
	)
	.await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	for (sql, kept) in [
		("FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t }", true),
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u WHERE n > $i) AS us FROM t }",
			false,
		),
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; CREATE u SET n = $i }",
			false,
		),
		(
			"FOR $i IN 0..3 { FOR $j IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u WHERE n > $i) AS us FROM t } }",
			true,
		),
		// A write to a table the kept statement does not read leaves it kept.
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; CREATE t SET n = $i }",
			true,
		),
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; UPDATE t:1 SET n = $i }",
			true,
		),
		// A write to what it reads, a delete (which may clear references in any
		// table), a schema statement over its table, a write through a table
		// running events, and a table defined by the write it reads.
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; UPDATE u:1 SET n = $i }",
			false,
		),
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; DELETE t:1 }",
			false,
		),
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; CREATE evented SET n = $i }",
			false,
		),
		(
			"FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM guarded) AS gs FROM t; CREATE t SET n = $i }",
			false,
		),
	] {
		let bound = bind(&ctx, sql).await;
		let physical = super::map::map(&planner(&ctx), &bound).await.expect("maps");
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		assert!(!program.nested.is_empty(), "`{sql}` nests no statement in a group");
		assert_eq!(program.nested.iter().any(|spec| spec.hoist.is_some()), kept, "`{sql}`");
	}
	// A schema statement each iteration runs over what the kept statement
	// reads, before the statement reads it again: the block is declined whole.
	let sql = "FOR $i IN 0..3 { LET $r = SELECT n, (SELECT VALUE n FROM u) AS us FROM t; DEFINE FIELD OVERWRITE m ON u VALUE $i }";
	let bound = bind(&ctx, sql).await;
	let mapped = super::map::map(&planner(&ctx), &bound).await;
	assert_eq!(
		mapped.err().map(|why| why.reason),
		Some(super::speculate::DATA_DEPENDENT_DDL.to_owned()),
		"`{sql}`"
	);
}

/// Rows for the statements that carry a tail: a table to scan and one a
/// clause may read.
const TAIL_SCHEMA: &str = "
	DEFINE TABLE t SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE e SCHEMALESS PERMISSIONS FULL;
	CREATE t:1 SET n = 1;
	CREATE t:2 SET n = 2;
	CREATE t:3 SET n = 3;
	CREATE e:1 SET d = 1s;
";

/// What `sql` answers over a fresh [`TAIL_SCHEMA`], and whether the plan is
/// the program's own operator with no operator of the tail around it.
async fn tailed(sql: &str) -> (Result<Vec<crate::val::Value>, String>, bool) {
	use super::tests::outcome;
	let db = TestDb::new_with_auth(TAIL_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let bound = bind(&ctx, sql).await;
	let op = super::plan(&planner(&ctx), &bound)
		.await
		.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
	let bare = super::runs_on_vm(op.as_ref());
	let rows = outcome(&op, &ctx).await;
	ctx.txn().cancel().await.expect("cancel");
	(rows, bare)
}

/// A statement's `TIMEOUT`, `VERSION` and `ONLY` are applied by its program: a
/// deadline that passes while the scan runs fails the statement, and no
/// operator of the tail stands around the program.
#[tokio::test]
async fn the_tail_is_applied_by_the_program() {
	let mut golden = Golden::new("the_tail_is_applied_by_the_program");
	for sql in [
		"SELECT * FROM t ORDER BY id TIMEOUT 1s",
		"SELECT * FROM t ORDER BY id TIMEOUT NONE",
		"SELECT * FROM t ORDER BY id TIMEOUT 'x'",
		// A deadline that passes while the scan runs.
		"SELECT VALUE sleep(30ms) FROM t TIMEOUT 1ms",
		"SELECT * FROM t WHERE sleep(30ms) = NONE TIMEOUT 1ms",
		// `ONLY` over no row, one and several.
		"SELECT * FROM ONLY t WHERE n = 99",
		"SELECT * FROM ONLY t WHERE n = 1",
		"SELECT * FROM ONLY t",
		"SELECT * FROM ONLY t WHERE n = 1 TIMEOUT 1s",
		"SELECT * FROM ONLY t TIMEOUT 1s",
		// A clause holding a statement.
		"SELECT * FROM t ORDER BY id TIMEOUT (RETURN 1s)",
		"SELECT * FROM t ORDER BY id TIMEOUT (SELECT VALUE d FROM ONLY e:1)",
		"SELECT * FROM t ORDER BY id TIMEOUT ({ LET $d = 1s; RETURN $d })",
		// `VERSION`, its clause a statement too.
		"SELECT * FROM [{ n: 1 }, { n: 2 }] VERSION d'2999-01-01T00:00:00Z'",
		"SELECT * FROM [{ n: 1 }] VERSION 'not a datetime'",
		"SELECT * FROM [{ n: 1 }, { n: 2 }] VERSION (RETURN d'2999-01-01T00:00:00Z')",
		"SELECT n, (SELECT VALUE n FROM [1, 2]) AS m FROM [{ n: 1 }] VERSION (RETURN d'2999-01-01T00:00:00Z')",
		"SELECT VALUE n FROM ONLY [{ n: 1 }] VERSION (SELECT VALUE <datetime> '2999-01-01T00:00:00Z' FROM ONLY [1])",
		// In a statement's value and a loop's body.
		"RETURN (SELECT * FROM ONLY t WHERE n = 2)",
		"RETURN (SELECT * FROM t ORDER BY id TIMEOUT 1s)",
		"FOR $i IN [1, 2] { SELECT * FROM ONLY t WHERE n = $i }",
		"{ FOR $i IN [1, 4] { SELECT * FROM ONLY t WHERE n = $i } }",
		"SELECT n, (SELECT * FROM ONLY t WHERE n = $parent.n) AS same FROM t ORDER BY id",
	] {
		let (rows, bare) = tailed(sql).await;
		assert!(bare, "`{sql}` is wrapped in an operator of its tail");
		golden.rows(sql, &rows);
	}
	golden.check();
}

/// A deadline that passes while a scan runs fails the statement with the
/// timeout error, and one that does not leaves its rows alone.
#[tokio::test]
async fn a_timeout_that_passes_mid_scan_fails_the_statement() {
	let (expired, _) = tailed("SELECT VALUE sleep(30ms) FROM t TIMEOUT 1ms").await;
	let message = expired.expect_err("the deadline passes during the scan");
	assert!(message.contains("exceeded the timeout: 1ms"), "{message}");
	let (kept, _) = tailed("SELECT VALUE n FROM t ORDER BY n TIMEOUT 5s").await;
	assert_eq!(kept.expect("rows").len(), 3);
}

/// `ONLY` over several rows fails, and over none answers `NONE`.
#[tokio::test]
async fn only_over_no_row_and_over_several() {
	let (none, _) = tailed("SELECT * FROM ONLY t WHERE n = 99").await;
	assert_eq!(none, Ok(vec![crate::val::Value::None]));
	let (many, _) = tailed("SELECT * FROM ONLY t").await;
	let mut golden = Golden::new("only_over_no_row_and_over_several");
	golden.record("SELECT * FROM ONLY t", render_outcome(&many));
	assert!(many.is_err());
	golden.check();
}

/// A program of a statement with a tail names the tail, and its clauses'
/// statements are units of the program, not operators.
#[tokio::test]
async fn a_program_holds_its_tail_and_the_statements_of_its_clauses() {
	let db = TestDb::new_with_auth(TAIL_SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Read).await;
	for (sql, only, timeout, units) in [
		("SELECT * FROM t TIMEOUT 1s", false, true, 0),
		("SELECT * FROM ONLY t WHERE n = 1", true, false, 0),
		("SELECT * FROM ONLY t WHERE n = 1 TIMEOUT 1s", true, true, 0),
		("SELECT * FROM t TIMEOUT (RETURN 1s)", false, true, 1),
		("SELECT * FROM t ORDER BY id", false, false, 0),
	] {
		let bound = bind(&ctx, sql).await;
		let physical =
			super::optimize::optimize(super::map::map(&planner(&ctx), &bound).await.expect("maps"));
		let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
		match &program.tail {
			Some(tail) => {
				assert_eq!(tail.only, only, "`{sql}`");
				assert_eq!(tail.timeout.is_some(), timeout, "`{sql}`");
				assert!(tail.version.is_none(), "`{sql}`");
			}
			None => assert!(!only && !timeout, "`{sql}` holds no tail"),
		}
		let held: usize = program
			.subqueries
			.iter()
			.filter(|spec| {
				program.units.get(spec.unit.0 as usize).is_some_and(|unit| unit.unit.is_some())
			})
			.count();
		assert_eq!(held, program.subqueries.len(), "`{sql}` holds a subquery that is no unit");
		assert_eq!(program.units.len(), units, "`{sql}`");
	}
}
