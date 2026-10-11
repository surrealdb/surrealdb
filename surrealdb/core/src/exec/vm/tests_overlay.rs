//! A block planned against what its own schema statements write
//! (`vm::speculate`), through the executor: what each statement answers, the
//! keyspace it leaves, and what the program compiled again.

use surrealdb_types::ToSql;

use super::golden::{Golden, render_changes};
use super::tests::{changes, keyspace};
use crate::dbs::Session;
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
	CREATE user:reader;
	DEFINE TABLE t PERMISSIONS FULL;
	CREATE t:0 SET n = 0;
	DEFINE TABLE log PERMISSIONS FULL;
	DEFINE FUNCTION fn::none() { NONE };
	DEFINE TABLE doc PERMISSIONS FOR select WHERE owner = $auth.id, FOR create, update FULL;
	DEFINE FIELD secret ON doc PERMISSIONS FOR select WHERE owner = $auth.id;
	CREATE doc:1 SET owner = user:reader, secret = 's1';
	CREATE doc:2 SET owner = user:other, secret = 's2';
";

/// Blocks whose statements read what an earlier schema statement of the block
/// defines, removes or alters, each followed by reads of what it left.
const OVERLAY_BLOCKS: &[&str] = &[
	// Fields: computed, defaulted, typed, asserted, read and written after.
	"{ DEFINE FIELD k ON t VALUE 5; CREATE t:1 SET n = 1; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE FIELD c ON t COMPUTED n * 10; RETURN SELECT VALUE c FROM t ORDER BY id }",
	"{ DEFINE FIELD d ON t DEFAULT 'x'; CREATE t:1; UPDATE t:0 SET n = 9; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE FIELD a ON t TYPE int; CREATE t:1 SET a = 'no' }",
	"{ DEFINE FIELD a ON t TYPE int ASSERT $value > 0; CREATE t:1 SET a = 3; CREATE t:2 SET a = -1 }",
	"{ DEFINE TABLE s SCHEMAFULL; DEFINE FIELD a ON s TYPE int; CREATE s:1 SET a = 1, b = 2; RETURN SELECT * FROM s }",
	// Tables: defined, redefined, removed, altered.
	"{ DEFINE TABLE fresh; CREATE fresh:1 SET n = 1; RETURN SELECT * FROM fresh }",
	"{ DEFINE TABLE fresh TYPE RELATION IN t OUT t; RELATE t:0->fresh:1->t:0; RETURN SELECT in, out FROM fresh }",
	"{ DEFINE TABLE fresh; DEFINE TABLE fresh; CREATE fresh:1 }",
	"{ DEFINE TABLE IF NOT EXISTS t; DEFINE TABLE IF NOT EXISTS t; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE TABLE OVERWRITE t SCHEMAFULL; CREATE t:1 SET n = 1; RETURN SELECT * FROM t ORDER BY id }",
	"{ REMOVE TABLE t; RETURN SELECT * FROM t }",
	"{ REMOVE TABLE IF EXISTS missing; REMOVE TABLE IF EXISTS missing; RETURN 1 }",
	"{ REMOVE TABLE missing; RETURN 1 }",
	"{ DEFINE FIELD a ON t TYPE int; ALTER FIELD a ON t TYPE string; CREATE t:1 SET a = 's'; RETURN SELECT * FROM t ORDER BY id }",
	"{ ALTER TABLE t SCHEMAFULL; CREATE t:1 SET n = 1, extra = 2; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE FIELD a ON t VALUE 1; REMOVE FIELD a ON t; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	// Indexes: the ids a definition leases, the entries later writes make,
	// and the reads that use them — in the block and in the query after.
	"{ DEFINE INDEX by_n ON t FIELDS n; FOR $i IN 1..6 { CREATE type::record('t', $i) SET n = $i % 3 }; RETURN SELECT VALUE id FROM t WHERE n = 1 ORDER BY id }",
	"{ DEFINE INDEX by_n ON t FIELDS n UNIQUE; CREATE t:1 SET n = 1; CREATE t:2 SET n = 1 }",
	"{ DEFINE INDEX by_n ON t FIELDS n UNIQUE; CREATE t:1 SET n = 1; UPDATE t:1 SET n = 2; DELETE t:0; RETURN SELECT VALUE id FROM t WHERE n = 2 }",
	"{ DEFINE TABLE fresh; DEFINE INDEX by_n ON fresh FIELDS n; CREATE fresh:1 SET n = 4; RETURN SELECT VALUE id FROM fresh WHERE n = 4 }",
	"{ DEFINE INDEX by_n ON t FIELDS n; REMOVE INDEX by_n ON t; CREATE t:1 SET n = 1; RETURN SELECT VALUE id FROM t WHERE n = 1 }",
	"{ DEFINE INDEX by_n ON t FIELDS n; DEFINE INDEX OVERWRITE by_n ON t FIELDS n UNIQUE; CREATE t:1 SET n = 0 }",
	// Events, views and references the block defines, maintained by its writes.
	"{ DEFINE EVENT noted ON t WHEN $event = 'CREATE' THEN (UPSERT type::record('log', record::id($after.id)) SET of = $after.id); CREATE t:1; RETURN SELECT VALUE of FROM log }",
	"{ DEFINE TABLE counts AS SELECT count() AS c FROM t GROUP ALL; CREATE t:1; CREATE t:2; RETURN SELECT c FROM counts }",
	"{ DEFINE TABLE mirror AS SELECT n FROM t; CREATE t:1 SET n = 7; RETURN SELECT n FROM mirror ORDER BY n }",
	"{ DEFINE FIELD up ON t TYPE option<record<fresh>> REFERENCE ON DELETE UNSET; CREATE fresh:1; UPDATE t:0 SET up = fresh:1; DELETE fresh:1; RETURN SELECT * FROM t ORDER BY id }",
	// Functions and parameters the block defines, called and read after.
	"{ DEFINE FUNCTION fn::twice($x: int) { $x * 2 }; RETURN fn::twice(4) }",
	"{ DEFINE FUNCTION fn::f() { 1 }; DEFINE FUNCTION OVERWRITE fn::f() { 2 }; RETURN fn::f() }",
	"{ DEFINE FUNCTION fn::f() { 1 }; REMOVE FUNCTION fn::f; RETURN fn::f() }",
	"{ DEFINE PARAM $p VALUE 3; RETURN $p }",
	"{ DEFINE FUNCTION fn::rows() { SELECT VALUE n FROM t ORDER BY n }; CREATE t:1 SET n = 1; RETURN fn::rows() }",
	// A function the block redefines or removes, whose arrival body a call
	// could otherwise be replaced by.
	"{ DEFINE FUNCTION OVERWRITE fn::none() { RETURN 7 }; RETURN fn::none() }",
	"{ REMOVE FUNCTION fn::none; RETURN fn::none() }",
	"{ FOR $i IN 0..2 { DEFINE FUNCTION OVERWRITE fn::none() { RETURN 7 } }; RETURN fn::none() }",
	"{ DEFINE PARAM $p VALUE 3; CREATE t:1 SET p = $p; RETURN SELECT VALUE p FROM t:1 }",
	// Several schema statements in turn, each read by what follows it.
	"{ DEFINE FIELD a ON t VALUE 1; CREATE t:1 SET n = 1; DEFINE FIELD b ON t VALUE 2; CREATE t:2 SET n = 2; DEFINE FIELD c ON t VALUE 3; CREATE t:3 SET n = 3; RETURN SELECT * FROM t ORDER BY id }",
	// Schema statements in branches and loops.
	"{ IF true { DEFINE FIELD k ON t VALUE 1 }; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	"{ IF false { DEFINE FIELD k ON t VALUE 1 }; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	"{ FOR $i IN 0..3 { DEFINE FIELD IF NOT EXISTS k ON t VALUE $i; CREATE type::record('t', $i + 1) }; RETURN SELECT * FROM t ORDER BY id }",
	"{ FOR $i IN 0..3 { DEFINE FIELD OVERWRITE k ON t VALUE $i; CREATE type::record('t', $i + 1) }; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE FIELD k ON t VALUE 1; FOR $i IN 0..3 { CREATE type::record('t', $i + 1) }; RETURN SELECT * FROM t ORDER BY id }",
	// A table a write defines, then a schema statement over it: one that
	// defines its table where there is none shares the definition's ids with
	// the write, whichever of the two defines it.
	"{ CREATE fresh:1; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2; RETURN SELECT * FROM fresh ORDER BY id }",
	"{ CREATE fresh:1 SET n = 1; DEFINE INDEX by_n ON fresh FIELDS n; CREATE fresh:2 SET n = 1; RETURN SELECT VALUE id FROM fresh WHERE n = 1 }",
	"{ CREATE fresh:1; DEFINE EVENT e ON fresh THEN (UPSERT type::record('log', record::id($after.id)) SET of = $after.id); CREATE fresh:2; RETURN SELECT VALUE of FROM log }",
	"{ IF (SELECT VALUE n FROM ONLY t:0) = 1 { CREATE fresh:1 }; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2; RETURN SELECT * FROM fresh ORDER BY id }",
	"{ CREATE fresh:1; CREATE fresh:2; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:3; DEFINE FIELD j ON fresh VALUE 2; CREATE fresh:4; RETURN SELECT * FROM fresh ORDER BY id }",
	// Any other schema statement over such a table, or one after a write to a
	// table the write evaluates, is not modelled.
	"{ CREATE fresh:1; DEFINE TABLE fresh; RETURN 1 }",
	"{ CREATE fresh:1; DEFINE TABLE fresh; CREATE fresh:2 }",
	"{ CREATE fresh:1; REMOVE TABLE fresh; RETURN SELECT * FROM fresh }",
	"{ LET $r = type::record('fresh', (SELECT VALUE n FROM ONLY t:0)); CREATE $r; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2; RETURN SELECT * FROM fresh ORDER BY id }",
	// A schema statement reading rows an earlier write of the block wrote: its
	// build reads them when it runs.
	"{ CREATE t:1 SET n = 1; DEFINE INDEX by_n ON t FIELDS n; CREATE t:2 SET n = 1; RETURN SELECT VALUE id FROM t WHERE n = 1 ORDER BY id }",
	"{ CREATE t:1 SET n = 1; CREATE t:2 SET n = 1; DEFINE INDEX u ON t FIELDS n UNIQUE; CREATE t:3 }",
	"{ CREATE t:1 SET n = 7; DEFINE TABLE mirror AS SELECT n FROM t; CREATE t:2 SET n = 8; RETURN SELECT n FROM mirror ORDER BY n }",
	"{ DEFINE EVENT e ON t THEN (DEFINE FIELD IF NOT EXISTS late ON t VALUE 'event'); CREATE t:1; DEFINE FIELD IF NOT EXISTS late ON t VALUE 'block'; CREATE t:2; RETURN SELECT * FROM t ORDER BY id }",
	// Reads of what a record user may and may not see, after a definition.
	"{ DEFINE FIELD shown ON doc VALUE 'v'; UPDATE doc SET n = 1; RETURN SELECT * FROM doc ORDER BY id }",
	"{ DEFINE FIELD guarded ON doc VALUE 'g' PERMISSIONS FOR select NONE; CREATE doc:3 SET owner = $auth.id; RETURN SELECT * FROM doc ORDER BY id }",
	// A definition that changes what a later statement computes or may see.
	"{ DEFINE FIELD loud ON doc COMPUTED string::uppercase(secret ?? ''); RETURN SELECT id, loud FROM doc ORDER BY id }",
	"{ DEFINE FIELD OVERWRITE secret ON doc PERMISSIONS FULL; RETURN SELECT id, secret FROM doc ORDER BY id }",
	"{ DEFINE TABLE OVERWRITE doc PERMISSIONS FULL; RETURN SELECT id, owner FROM doc ORDER BY id }",
	"{ DEFINE FIELD c ON t COMPUTED n + 1; DEFINE FIELD OVERWRITE c ON t COMPUTED n + 2; RETURN SELECT VALUE c FROM t }",
	// A table removed, then read and written.
	"{ REMOVE TABLE t; CREATE t:5 SET n = 5; RETURN SELECT * FROM t ORDER BY id }",
	"{ REMOVE TABLE log; RETURN [(SELECT * FROM log), (INFO FOR TABLE log)] }",
	// A name bound to a constant is the name it binds.
	"{ LET $f = 'n'; DEFINE INDEX by_n ON t FIELDS $f; CREATE t:1 SET n = 1; RETURN SELECT VALUE id FROM t WHERE n = 1 }",
	"{ LET $v = 4; DEFINE FIELD k ON t VALUE $v; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	// A table a write defines, read and written after.
	"{ CREATE fresh:1 SET n = 1; CREATE fresh:2 SET n = 2; RETURN SELECT * FROM fresh ORDER BY id }",
	"{ CREATE fresh:1 SET n = 1; UPDATE fresh SET n += 1; DELETE fresh:1; RETURN SELECT * FROM fresh }",
	// Catalog changes a run of the block decides: declined whole.
	"{ FOR $f IN ['n', 'm'] { DEFINE INDEX OVERWRITE by_f ON t FIELDS $f; CREATE t SET n = 1, m = 1 }; RETURN SELECT VALUE count() FROM t WHERE n = 1 GROUP ALL }",
	"{ FOR $i IN 0..3 { CREATE type::record('t', $i + 1) SET n = $i; IF $i = 1 { REMOVE TABLE t } }; RETURN SELECT * FROM t ORDER BY id }",
	"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE FIELD k ON t VALUE 1 }; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE FUNCTION fn::mk() { DEFINE FIELD IF NOT EXISTS made ON t VALUE 'm'; RETURN 1 }; LET $x = fn::mk(); CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE EVENT e ON t THEN (DEFINE FIELD IF NOT EXISTS late ON t VALUE 'event'); FOR $i IN 1..4 { CREATE type::record('t', $i) }; RETURN SELECT * FROM t ORDER BY id }",
	"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE FUNCTION OVERWRITE fn::none() { RETURN 7 } }; RETURN fn::none() }",
	"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { REMOVE FUNCTION fn::none }; RETURN fn::none() }",
	"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE PARAM $q VALUE 4 }; RETURN $q }",
	// A failure partway: the block's writes, its catalog's among them, are
	// rolled back.
	"{ DEFINE FIELD k ON t VALUE 1; CREATE t:1 SET n = 1; THROW 'stop' }",
	"BEGIN; { DEFINE TABLE fresh; DEFINE INDEX by_n ON fresh FIELDS n UNIQUE; CREATE fresh:1 SET n = 1; CREATE fresh:2 SET n = 1 }; COMMIT;",
	"BEGIN; { DEFINE FIELD k ON t VALUE 1; CREATE t:1 SET n = 1 }; CANCEL;",
];

/// A statement's result as one line: its value, or its error.
fn answer(result: Result<surrealdb_types::Value, surrealdb_types::Error>) -> String {
	match result {
		Ok(value) => value.to_sql().replace('\n', "\\n"),
		Err(error) => format!("error: {}", error.to_string().replace('\n', "\\n")),
	}
}

/// The answers and changed entries of a run, as a golden body.
fn render_run(answers: &[String], keys: &[(Vec<u8>, Option<Vec<u8>>)]) -> String {
	format!("{}\nentries:\n{}", answers.join("\n"), render_changes(keys))
}

/// What `statements` answer run one after another by `session` on a fresh
/// database, and the entries they changed.
async fn ran(
	statements: &[&str],
	session: &Session,
) -> (Vec<String>, Vec<(Vec<u8>, Option<Vec<u8>>)>) {
	let db = TestDb::quiescent(SETUP, true).await;
	let before = keyspace(&db).await;
	let mut answers = Vec::new();
	for sql in statements {
		let responses = db.datastore().execute(sql, session, None).await.expect("runs");
		answers.extend(responses.into_iter().map(|r| answer(r.result)));
	}
	let after = keyspace(&db).await;
	let mut changed: Vec<_> = changes(&before, &after).into_iter().map(without_doc_uuids).collect();
	changed.sort();
	(answers, changed)
}

/// What a run on the VM did beyond answering: how many blocks it declined
/// whole for a catalog change their statements read, and how many times a
/// running program bound, mapped, lowered or compiled anything
/// ([`crate::exec::compiles`]).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct Counted {
	declined: usize,
	compiled: usize,
}

impl Counted {
	fn now() -> Self {
		Self {
			declined: super::speculate::DECLINED.with(std::cell::Cell::get),
			compiled: crate::exec::compiles::count(),
		}
	}

	fn since(before: Self) -> Self {
		let now = Self::now();
		Self {
			declined: now.declined - before.declined,
			compiled: now.compiled - before.compiled,
		}
	}
}

/// [`ran`], with what it did beyond answering.
async fn ran_counted(
	statements: &[&str],
	session: &Session,
) -> (Vec<String>, Vec<(Vec<u8>, Option<Vec<u8>>)>, Counted) {
	let before = Counted::now();
	let (answers, keys) = ran(statements, session).await;
	(answers, keys, Counted::since(before))
}

/// A unique index's document-id entries without the uuid each draws per
/// record, which differs between any two runs: the uuid after `!ds` and `!is`
/// in a key, and the uuid ending a `!dh` or `!ih` entry's value.
fn without_doc_uuids((key, value): (Vec<u8>, Option<Vec<u8>>)) -> (Vec<u8>, Option<Vec<u8>>) {
	let mut key = key;
	for marker in [&b"!ds"[..], &b"!is"[..]] {
		if let Some(at) = key.windows(3).position(|w| w == marker) {
			let from = at + 3;
			let to = (from + 16).min(key.len());
			key[from..to].fill(0);
		}
	}
	let hashed = key.windows(3).any(|w| w == b"!dh" || w == b"!ih");
	let value = value.map(|mut value| {
		if hashed && value.len() >= 16 {
			let from = value.len() - 16;
			value[from..].fill(0);
		}
		value
	});
	(key, value)
}

fn reader() -> Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	Session::for_record("test", "test", "user", rid)
}

/// Every block answers and leaves the recorded entries, for the owner and for a
/// checked record user, which may not change the catalog at all.
#[tokio::test]
async fn overlay_a_block_answers_and_leaves_its_entries() {
	let mut golden = Golden::new("overlay_a_block_answers_and_leaves_its_entries");
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		for body in OVERLAY_BLOCKS {
			let statements = [
				*body,
				"SELECT * FROM t ORDER BY id",
				"SELECT VALUE id FROM t WHERE n = 1",
				"SELECT * FROM fresh ORDER BY id",
				"SELECT * FROM doc ORDER BY id",
			];
			let (answers, keys, counted) = ran_counted(&statements, &session).await;
			golden.record(format!("{who}: {body}"), render_run(&answers, &keys));
			assert_eq!(
				counted.compiled, 0,
				"`{body}` for the {who} compiled or planned a statement once its block ran"
			);
		}
	}
	golden.check();
}

/// A block whose schema statements run once each, in order, after nothing
/// that changes the catalog unseen, is planned knowing what each defines and
/// compiles nothing once it runs. One whose catalog changes depend on what it
/// meets — a schema statement after a change it was not planned knowing, one
/// under a condition read from data — and whose later statements read what
/// they change, is declined whole; each block answers and leaves the recorded
/// entries.
#[tokio::test]
async fn overlay_a_block_planned_knowing_its_schema_compiles_nothing_again() {
	let mut golden =
		Golden::new("overlay_a_block_planned_knowing_its_schema_compiles_nothing_again");
	for (body, declined, speculated) in [
		("{ DEFINE FIELD k ON t VALUE 5; CREATE t:1 SET n = 1; RETURN SELECT * FROM t }", 0, 1),
		(
			"{ DEFINE TABLE fresh; DEFINE INDEX by_n ON fresh FIELDS n UNIQUE; \
			   CREATE fresh:1 SET n = 4; RETURN SELECT VALUE id FROM fresh WHERE n = 4 }",
			0,
			2,
		),
		(
			"{ DEFINE FIELD a ON t VALUE 1; CREATE t:1; DEFINE FIELD b ON t VALUE 2; \
			   CREATE t:2; DEFINE FIELD c ON t VALUE 3; CREATE t:3 }",
			0,
			3,
		),
		("{ DEFINE FUNCTION fn::twice($x: int) { $x * 2 }; RETURN fn::twice(4) }", 0, 1),
		(
			"{ DEFINE EVENT noted ON t THEN (UPSERT type::record('log', record::id($after.id)) SET of = $after.id); CREATE t:1; \
			   RETURN SELECT VALUE of FROM log }",
			0,
			1,
		),
		// The write defines its table, and the schema statement after it, which
		// defines its table where there is none, is modelled: the write's
		// definition takes the ids its speculative run drew, and so does its
		// own where the write defines none.
		("{ CREATE fresh:1; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }", 0, 1),
		("{ CREATE fresh:1 SET n = 1; DEFINE INDEX by_n ON fresh FIELDS n; CREATE fresh:2 }", 0, 1),
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 1 { CREATE fresh:1 }; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }",
			0,
			1,
		),
		// A schema statement that does not define its table where there is none
		// finds the table the write defined, which its speculative run could
		// not: it is not modelled, and the write after it reads what it changes.
		("{ CREATE fresh:1; DEFINE TABLE fresh; CREATE fresh:2 }", 1, 0),
		// A write to a table it evaluates may define any table: a schema
		// statement over a table the catalog does not hold is not modelled after
		// it, and one over a table the catalog holds is.
		(
			"{ LET $r = type::record('fresh', (SELECT VALUE n FROM ONLY t:0)); CREATE $r; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }",
			1,
			0,
		),
		(
			"{ LET $r = type::record('fresh', (SELECT VALUE n FROM ONLY t:0)); CREATE $r; DEFINE FIELD k ON t VALUE 1; CREATE t:1 }",
			0,
			1,
		),
		// What a write before it wrote to the rows a schema statement reads is
		// read by the statement's real run: the statement is modelled.
		("{ CREATE t:1 SET n = 1; DEFINE INDEX by_n ON t FIELDS n; CREATE t:2 SET n = 1 }", 0, 1),
		// A table the writes define is read as the table no definition holds,
		// and a schema statement over another table is modelled after them.
		("{ CREATE fresh:1; CREATE fresh:2; RETURN SELECT * FROM fresh }", 0, 0),
		(
			"{ CREATE fresh:1; DEFINE FIELD k ON t VALUE 1; CREATE t:1; RETURN SELECT * FROM fresh }",
			0,
			1,
		),
		// What a write's event and a function body run is read from the
		// catalog: neither changes it, so the schema statements after them are
		// modelled.
		(
			"{ DEFINE EVENT noted ON t THEN (UPSERT type::record('log', record::id($after.id)) SET of = $after.id); \
			   CREATE t:1; DEFINE FIELD k ON t VALUE 1; CREATE t:2 }",
			0,
			2,
		),
		("{ LET $x = fn::none(); DEFINE FIELD k ON t VALUE 1; CREATE t:1 }", 0, 1),
		// An event that defines changes the catalog where the block cannot see.
		(
			"{ DEFINE EVENT noted ON t THEN (DEFINE FIELD IF NOT EXISTS late ON t VALUE 1); \
			   CREATE t:1; CREATE t:2 }",
			1,
			1,
		),
		// A schema statement in a branch changes the catalog only when the
		// branch runs.
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE FIELD k ON t VALUE 1 }; CREATE t:1 }",
			1,
			0,
		),
		// Nothing after it reads what it changes.
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE TABLE other }; CREATE t:1; RETURN 1 }",
			0,
			0,
		),
		// A condition the binding settles leaves the branch it takes, which is
		// then the block's own statement.
		("{ IF true { DEFINE FIELD k ON t VALUE 1 }; CREATE t:1 }", 0, 1),
	] {
		let speculated_before = super::speculate::SPECULATED.with(std::cell::Cell::get);
		let (answers, keys, counted) =
			ran_counted(&[body, "SELECT * FROM t ORDER BY id"], &TestDb::owner()).await;
		let speculated_after = super::speculate::SPECULATED.with(std::cell::Cell::get);
		golden.record(body, render_run(&answers, &keys));
		assert_eq!(
			counted,
			Counted {
				declined,
				..Counted::default()
			},
			"`{body}` declined, compiled again"
		);
		assert_eq!(speculated_after - speculated_before, speculated, "`{body}` speculated");
	}
	golden.check();
}

/// A schema statement whose outcome depends on a table's rows — an index's
/// build, a view's backfill — conflicts with a write to the table, and not
/// with one to another; a write reads its table's definition.
#[test]
fn overlay_a_schema_statement_reading_rows_conflicts_with_a_write_to_its_table() {
	use crate::exec::logical::footprint::of;
	let registry = crate::exec::function::FunctionRegistry::with_builtins();
	let op = super::tests::parse;
	let index = of(&op("DEFINE INDEX u ON t FIELDS n UNIQUE"), &registry);
	let view = of(&op("DEFINE TABLE v AS SELECT n FROM t"), &registry);
	let field = of(&op("DEFINE FIELD n ON t TYPE int"), &registry);
	let create_t = of(&op("CREATE t:1 SET n = 1"), &registry);
	let create_u = of(&op("CREATE u:1 SET n = 1"), &registry);
	let read_link = of(&op("SELECT author.name FROM post"), &registry);
	assert!(create_t.conflicts(&index));
	assert!(create_t.conflicts(&view));
	assert!(!create_u.conflicts(&index));
	assert!(!create_u.conflicts(&view));
	assert!(field.conflicts(&create_t));
	assert!(!field.conflicts(&create_u));
	// A path that may follow a record link reads any table.
	assert!(create_u.conflicts(&read_link));
}

/// A schema statement known to do nothing — `IF NOT EXISTS` over what
/// exists, `IF EXISTS` over what does not — is not compiled, and the
/// statements after one known to raise a declared refusal are not either; the
/// block answers and leaves the recorded entries. An `OVERWRITE` writes
/// whatever it finds, and an `IF NOT EXISTS` over what does not exist defines
/// it, so neither is folded.
#[tokio::test]
async fn overlay_known_schema_outcomes_are_folded() {
	let mut golden = Golden::new("overlay_known_schema_outcomes_are_folded");
	for (who, session, body, folded) in [
		(
			"owner",
			TestDb::owner(),
			"{ DEFINE TABLE IF NOT EXISTS t; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
			1,
		),
		(
			"owner",
			TestDb::owner(),
			"{ REMOVE TABLE IF EXISTS missing; REMOVE FIELD IF EXISTS nope ON t; RETURN 1 }",
			2,
		),
		(
			"owner",
			TestDb::owner(),
			"{ DEFINE FIELD IF NOT EXISTS k ON t VALUE 1; DEFINE FIELD IF NOT EXISTS k ON t VALUE 2; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
			1,
		),
		("owner", TestDb::owner(), "{ DEFINE TABLE t; CREATE t:1; RETURN 'unreached' }", 1),
		("owner", TestDb::owner(), "{ REMOVE TABLE missing; CREATE t:1 }", 1),
		("owner", TestDb::owner(), "{ { DEFINE TABLE t }; CREATE t:1 }", 1),
		("owner", TestDb::owner(), "{ DEFINE TABLE OVERWRITE t PERMISSIONS FULL; CREATE t:1 }", 0),
		(
			"owner",
			TestDb::owner(),
			"{ CREATE t:1 SET n = 1; CREATE t:2 SET n = 1; DEFINE INDEX IF NOT EXISTS u ON t FIELDS n UNIQUE; CREATE t:3 }",
			0,
		),
		("owner", TestDb::owner(), "{ IF true { DEFINE TABLE IF NOT EXISTS t }; CREATE t:1 }", 1),
		(
			"record user",
			reader(),
			"{ DEFINE FIELD k ON t VALUE 1; CREATE t:1 SET n = 1; RETURN 'unreached' }",
			1,
		),
		("record user", reader(), "{ DEFINE TABLE IF NOT EXISTS t; CREATE t:1 }", 1),
	] {
		let before = super::speculate::FOLDED.with(std::cell::Cell::get);
		let statements = [body, "SELECT * FROM t ORDER BY id"];
		let (answers, keys) = ran(&statements, &session).await;
		let after = super::speculate::FOLDED.with(std::cell::Cell::get);
		golden.record(format!("{who}: {body}"), render_run(&answers, &keys));
		assert_eq!(after - before, folded, "`{body}` for the {who} folded");
	}
	golden.check();
}

/// A write to a table the catalog holds no events or indexes for leaves
/// those stages out, and answers and leaves the recorded entries —
/// including when the block defines an event or index partway: planned
/// against it when the block models it, and declined whole when a later
/// iteration's write reads what an iteration decides to define.
#[tokio::test]
async fn overlay_a_write_leaves_out_the_stages_its_table_has_nothing_for() {
	let mut golden = Golden::new("overlay_a_write_leaves_out_the_stages_its_table_has_nothing_for");
	for (body, folded, declined) in [
		("{ CREATE t:1 SET n = 1; UPDATE t:1 SET n = 2 }", 4, 0),
		("{ CREATE t:1 SET n = fn::none() }", 0, 0),
		(
			"FOR $i IN 0..4 { CREATE t:[$i] SET n = $i; IF $i = 1 { DEFINE EVENT e ON t THEN (UPSERT log:ev SET n += 1) } }",
			0,
			1,
		),
		(
			"FOR $i IN 0..4 { CREATE t:[$i] SET n = $i % 2; IF $i = 1 { DEFINE INDEX u ON t FIELDS n UNIQUE } }",
			0,
			1,
		),
		// A table found as records arrive lowers its stages per table.
		("FOR $i IN 0..4 { CREATE type::record('t', $i + 1) SET n = $i }", 0, 0),
		(
			"{ DEFINE INDEX by_n ON t FIELDS n; CREATE t:1 SET n = 1; RETURN SELECT VALUE id FROM t WHERE n = 1 }",
			1,
			0,
		),
	] {
		let statements = [
			body,
			"SELECT * FROM t ORDER BY id",
			"SELECT * FROM log",
			"SELECT VALUE id FROM t WHERE n = 1 ORDER BY id",
		];
		let before = super::speculate::STAGES_LEFT_OUT.with(std::cell::Cell::get);
		let (answers, keys, counted) = ran_counted(&statements, &TestDb::owner()).await;
		let after = super::speculate::STAGES_LEFT_OUT.with(std::cell::Cell::get);
		golden.record(body, render_run(&answers, &keys));
		assert!(after - before >= folded, "`{body}` folded {} stages", after - before);
		assert_eq!(
			counted,
			Counted {
				declined,
				..Counted::default()
			},
			"`{body}` declined, compiled again"
		);
	}
	golden.check();
}

/// A loop whose schema statements change the catalog on its first iteration
/// and do nothing after — `IF NOT EXISTS` — runs its first iteration planned
/// as the block's own statements are and the rest planned against what the
/// first defined, compiling nothing once it runs. One that may run no
/// iteration, whose later iterations change the catalog too, or that writes
/// what its schema statements read, is not peeled, and the block is declined
/// whole when what follows reads what the loop changes.
#[tokio::test]
async fn overlay_a_loop_defining_once_is_peeled() {
	let mut golden = Golden::new("overlay_a_loop_defining_once_is_peeled");
	for (who, session, body, declined) in [
		(
			"owner",
			TestDb::owner(),
			"FOR $i IN 0..4 { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; CREATE t:[$i] SET n = $i }",
			0,
		),
		(
			"owner",
			TestDb::owner(),
			"{ FOR $i IN 0..4 { DEFINE TABLE IF NOT EXISTS fresh; DEFINE FIELD IF NOT EXISTS k ON fresh VALUE 'k'; CREATE fresh:[$i]; IF $i = 2 { BREAK } }; RETURN SELECT * FROM fresh ORDER BY id }",
			0,
		),
		(
			"owner",
			TestDb::owner(),
			"{ FOR $i IN 0..4 { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; IF $i = 1 { CONTINUE }; CREATE t:[$i] SET n = $i }; CREATE t:[9]; RETURN SELECT * FROM t ORDER BY id }",
			0,
		),
		(
			"owner",
			TestDb::owner(),
			"{ FOR $i IN [] { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; CREATE t:[$i] }; CREATE t:[9]; RETURN SELECT * FROM t ORDER BY id }",
			1,
		),
		(
			"owner",
			TestDb::owner(),
			"{ LET $to = (SELECT VALUE n FROM ONLY t:0); FOR $i IN 0..$to { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; CREATE t:[$i] }; CREATE t:[9]; RETURN SELECT * FROM t ORDER BY id }",
			1,
		),
		(
			"owner",
			TestDb::owner(),
			"{ FOR $i IN 0..3 { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; CREATE t:[$i] }; DEFINE FIELD IF NOT EXISTS m ON t VALUE 8; CREATE t:[9]; RETURN SELECT * FROM t ORDER BY id }",
			0,
		),
		(
			"owner",
			TestDb::owner(),
			"FOR $i IN 0..3 { DEFINE FIELD OVERWRITE k ON t VALUE $i; CREATE t:[$i] SET n = $i }",
			1,
		),
		(
			"owner",
			TestDb::owner(),
			"FOR $i IN 0..3 { DEFINE INDEX IF NOT EXISTS u ON t FIELDS n UNIQUE; CREATE t:[$i] SET n = $i % 2 }",
			1,
		),
		(
			"owner",
			TestDb::owner(),
			"{ IF true { FOR $i IN 0..3 { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; CREATE t:[$i] } } }",
			0,
		),
		// A record user may not define: the schema statement's speculative run
		// raises, so the loop is not peeled, and the block answers with the
		// refusal.
		(
			"record user",
			reader(),
			"FOR $i IN 0..3 { DEFINE FIELD IF NOT EXISTS k ON t VALUE 7; CREATE t:[$i] SET n = $i }",
			1,
		),
	] {
		let statements = [body, "SELECT * FROM t ORDER BY id", "SELECT * FROM fresh ORDER BY id"];
		let (answers, keys, counted) = ran_counted(&statements, &session).await;
		golden.record(format!("{who}: {body}"), render_run(&answers, &keys));
		assert_eq!(
			counted,
			Counted {
				declined,
				..Counted::default()
			},
			"`{body}` for the {who} declined, compiled again"
		);
	}
	golden.check();
}

/// Blocks whose schema statements are run speculatively while what reads the
/// transaction may read it from elsewhere: a function body that defines, run
/// by the statements after; statements compiled when the block reaches them;
/// nested statements planned against the block's own definitions.
const SPEC_BLOCKS: &[&str] = &[
	"{ DEFINE FUNCTION fn::mk() { DEFINE FIELD IF NOT EXISTS made ON t VALUE 'm'; RETURN 1 }; \
	   DEFINE FIELD k ON t VALUE 2; RETURN [fn::mk(), fn::mk()]; CREATE t:1; RETURN SELECT * FROM t ORDER BY id }",
	"{ DEFINE FIELD k ON t VALUE 2; RETURN SELECT n, fn::none() AS f, (SELECT VALUE k FROM ONLY $parent.id) AS again FROM t }",
	"{ DEFINE FIELD k ON t VALUE 2; LET $before = (SELECT VALUE k FROM t); CREATE t:9 SET n = 9; \
	   RETURN [$before, (SELECT VALUE k FROM t:9), (SELECT VALUE count() FROM t GROUP ALL)] }",
	"{ CREATE fresh:1; DEFINE FIELD k ON t VALUE 3; RETURN [(SELECT * FROM fresh), (SELECT VALUE k FROM t)] }",
];

/// A schema statement's speculative run is seen by nothing but its view: a
/// task reading the transaction for as long as the run lasts never finds the
/// table it defines, the transaction holds nothing of it once the run ends,
/// a view from the run's overlay finds it, and a view held past its
/// speculation's end takes no write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn spec_a_speculation_is_seen_only_through_its_view() {
	use std::sync::Arc;
	use std::sync::atomic::{AtomicBool, Ordering};

	use crate::catalog::providers::TableProvider;
	use crate::kvs::TransactionType;

	let db = TestDb::new("DEFINE TABLE t;").await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
	let mut planner = super::tests::planner(&ctx);
	planner.speculate_with(Some(Arc::new(ctx.root().options.clone().expect("options"))));
	let bound = super::tests::bind(&ctx, "DEFINE TABLE fresh").await;
	let physical = super::map::map(&planner, &bound).await.expect("maps");
	let txn = ctx.try_txn().expect("a transaction");
	let (ns, dbid) = planner.ns_db_ids().await.expect("the database");
	let table = surrealdb_strand::TableName::from("fresh");

	let stop = Arc::new(AtomicBool::new(false));
	let reader = {
		let (txn, stop, table) = (Arc::clone(&txn), Arc::clone(&stop), table.clone());
		tokio::spawn(async move {
			let mut reads = 0usize;
			while !stop.load(Ordering::Acquire) {
				if txn.get_tb(ns, dbid, &table, None).await.expect("reads").is_some() {
					return Err(reads);
				}
				reads += 1;
				tokio::task::yield_now().await;
			}
			Ok(reads)
		})
	};
	let mut runs = Vec::new();
	for _ in 0..20 {
		runs.push(
			super::speculate::run(
				&planner,
				&physical,
				&crate::kvs::speculation::Overlay::default(),
			)
			.await,
		);
	}
	stop.store(true, Ordering::Release);
	let reads = reader.await.expect("the reader").expect("the reader never saw the table");
	assert!(reads > 0, "the reader read while the speculations ran");

	let Some(super::speculate::Outcome::Ran(run)) = runs.pop().flatten() else {
		panic!("the statement ran speculatively");
	};
	assert!(run.changed);
	assert!(txn.get_tb(ns, dbid, &table, None).await.expect("reads").is_none());
	let (speculation, over) = super::speculate::view(&planner, &run.overlay).expect("a view");
	let view = Arc::clone(over.txn.as_ref().expect("a transaction"));
	assert!(view.get_tb(ns, dbid, &table, None).await.expect("reads").is_some());
	drop(over);
	drop(speculation);
	assert!(view.set(surrealdb_kvs::Key::from(b"/late".to_vec()), b"late".to_vec()).await.is_err());
	assert!(
		txn.get(surrealdb_kvs::Key::from(b"/late".to_vec()), None).await.expect("reads").is_none()
	);
	txn.cancel().await.expect("cancels");
}

/// A schema statement run after its speculation stamps what the speculation
/// drew: the catalog version, and the uuids its definition records, that the
/// statements after it were planned over are the ones the catalog holds.
#[tokio::test]
async fn spec_a_schema_statement_stamps_what_its_speculation_drew() {
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use crate::kvs::TransactionType;

	let db = TestDb::new("DEFINE TABLE t;").await;
	super::speculate::DRAWN.with(|drawn| drawn.borrow_mut().clear());
	let session = TestDb::owner();
	let responses = db
		.datastore()
		.execute(
			"{ DEFINE FIELD k ON t VALUE 1; CREATE t:1; DEFINE TABLE fresh; CREATE fresh:1 }",
			&session,
			None,
		)
		.await
		.expect("runs");
	assert!(responses.iter().all(|r| r.result.is_ok()), "{responses:?}");
	let drawn = super::speculate::DRAWN.with(|drawn| drawn.borrow().clone());
	let txn = db.datastore().transaction(TransactionType::Read).await.expect("a transaction");
	let ns = txn.expect_ns_by_name("test").await.expect("namespace").namespace_id;
	let dbid = txn.expect_db_by_name("test", "test").await.expect("database").database_id;
	for table in ["t", "fresh"] {
		let table = surrealdb_strand::TableName::from(table);
		let version = txn.catalog_version(ns, dbid, &table).await.expect("reads").expect("stamped");
		assert!(
			drawn.contains(&version.as_u128()),
			"`{table}`'s version was drawn by its speculation"
		);
	}
	let fresh = txn
		.get_tb(ns, dbid, &surrealdb_strand::TableName::from("fresh"), None)
		.await
		.expect("reads")
		.expect("defined");
	assert!(
		drawn.contains(&fresh.cache_fields_ts.as_u128()),
		"the table's stamps were drawn by its speculation"
	);
	txn.cancel().await.expect("cancels");
}

/// A table a write of the block defines before a schema statement that
/// defines it where there is none takes the id, stamps and catalog version
/// the statement's speculation drew, as the statement's own definition of it
/// does where no write before it defines it: the catalog holds what the
/// statements after them were planned against. A write that records a value
/// shape an index had not stored stamps a version of its own, so the writes
/// after the definitions store no indexed value.
#[tokio::test]
async fn spec_a_table_a_write_defines_takes_what_the_schema_statement_after_it_drew() {
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use crate::kvs::TransactionType;

	for (body, by_write) in [
		("{ CREATE fresh:1; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }", true),
		("{ UPSERT fresh:1; DEFINE INDEX by_n ON fresh FIELDS n; CREATE fresh:2 }", true),
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 1 { CREATE fresh:1 }; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }",
			false,
		),
	] {
		let db = TestDb::new("DEFINE TABLE t; CREATE t:0 SET n = 0;").await;
		super::speculate::DRAWN.with(|drawn| drawn.borrow_mut().clear());
		let session = TestDb::owner();
		let before = Counted::now();
		let responses = db.datastore().execute(body, &session, None).await.expect("runs");
		assert!(responses.iter().all(|r| r.result.is_ok()), "`{body}`: {responses:?}");
		assert_eq!(Counted::since(before), Counted::default(), "`{body}` ran on the VM");
		let drawn = super::speculate::DRAWN.with(|drawn| drawn.borrow().clone());
		let txn = db.datastore().transaction(TransactionType::Read).await.expect("a transaction");
		let ns = txn.expect_ns_by_name("test").await.expect("namespace").namespace_id;
		let dbid = txn.expect_db_by_name("test", "test").await.expect("database").database_id;
		let table = surrealdb_strand::TableName::from("fresh");
		let fresh = txn.get_tb(ns, dbid, &table, None).await.expect("reads").expect("defined");
		assert!(
			drawn.contains(&(fresh.table_id.0 as i64 as u128)),
			"`{body}`: the table's id was drawn by the speculation (written first: {by_write})"
		);
		assert!(
			drawn.contains(&fresh.cache_events_ts.as_u128()),
			"`{body}`: the table's stamps were drawn by the speculation"
		);
		let version = txn.catalog_version(ns, dbid, &table).await.expect("reads").expect("stamped");
		assert!(drawn.contains(&version.as_u128()), "`{body}`: the version was drawn");
		txn.cancel().await.expect("cancels");
	}
}

/// The namespace, database, table and index ids the catalog holds, by name.
async fn catalog_ids(db: &TestDb) -> Vec<String> {
	use crate::catalog::providers::{DatabaseProvider, NamespaceProvider, TableProvider};
	use crate::kvs::TransactionType;

	let txn = db.datastore().transaction(TransactionType::Read).await.expect("a transaction");
	let mut ids = Vec::new();
	for ns in txn.all_ns(None).await.expect("reads").iter() {
		ids.push(format!("{} {}", ns.name, ns.namespace_id.0));
		for dbdef in txn.all_db(ns.namespace_id, None).await.expect("reads").iter() {
			let (nsid, dbid) = (ns.namespace_id, dbdef.database_id);
			ids.push(format!("{}/{} {}", ns.name, dbdef.name, dbid.0));
			for tb in txn.all_tb(nsid, dbid, None).await.expect("reads").iter() {
				ids.push(format!("{}/{}/{} {}", ns.name, dbdef.name, tb.name, tb.table_id.0));
				for ix in
					txn.all_tb_indexes(nsid, dbid, &tb.name, None).await.expect("reads").iter()
				{
					ids.push(format!(
						"{}/{}/{}/{} {}",
						ns.name, dbdef.name, tb.name, ix.name, ix.index_id.0
					));
				}
			}
		}
	}
	txn.cancel().await.expect("cancels");
	ids.sort();
	ids
}

/// The catalog ids a block's definitions take, and those the definitions
/// after it take, are the recorded ones whatever the block does: it runs
/// them, fails before they run, fails after they run, runs a loop's first
/// iteration as its speculation modelled and then fails, runs a definition
/// whose speculative run raised after leasing, defines a table by writing
/// to it with the id a later definition's speculation leased, or is declined
/// whole after its speculative runs leased ids. An id a speculative run leased
/// that no definition took is handed out again.
#[tokio::test]
async fn spec_catalog_ids_are_stable_whether_the_block_runs_or_fails() {
	// Each step, with whether the VM runs a schema statement of it
	// speculatively.
	const STEPS: &[(&str, bool)] = &[
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { THROW 'stop' }; DEFINE TABLE a; \
			   DEFINE INDEX ia ON a FIELDS n; DEFINE INDEX it ON t FIELDS n; \
			   RETURN SELECT * FROM a WHERE n = 1 }",
			true,
		),
		("DEFINE TABLE a; DEFINE INDEX ia ON a FIELDS n; DEFINE INDEX it ON t FIELDS n;", false),
		(
			"{ DEFINE TABLE b; DEFINE INDEX ib ON b FIELDS n; CREATE b:1 SET n = 1; \
			   RETURN SELECT * FROM b WHERE n = 1 }",
			true,
		),
		(
			"{ DEFINE TABLE c; DEFINE INDEX ic ON c FIELDS n; CREATE c:1 SET n = 1; \
			   RETURN SELECT * FROM c WHERE n = 1; THROW 'after' }",
			true,
		),
		("DEFINE TABLE c; DEFINE INDEX ic ON c FIELDS n;", false),
		(
			"{ DEFINE TABLE d; FOR $i IN 0..3 { DEFINE FIELD OVERWRITE m ON counter VALUE $i; \
			   CREATE type::record('counter', $i) }; RETURN SELECT * FROM counter ORDER BY id }",
			true,
		),
		("DEFINE TABLE e;", false),
		("{ FOR $i IN [1] { DEFINE TABLE l }; RETURN 1 }", true),
		("DEFINE TABLE l2;", false),
		(
			"{ FOR $i IN [1, 2] { DEFINE TABLE IF NOT EXISTS l3; IF $i = 2 { THROW 'x' } }; \
			   RETURN 1 }",
			true,
		),
		("DEFINE TABLE l4;", false),
		("{ DEFINE TABLE r COMMENT <string> rand(); RETURN 1 }", true),
		(
			"{ DEFINE TABLE r2; DEFINE INDEX r2i ON r2 FIELDS n COMMENT <string> rand(); \
			   RETURN 1 }",
			true,
		),
		("DEFINE TABLE r3;", false),
		("{ CREATE g:1; DEFINE FIELD k ON g VALUE 1; CREATE g:2 }", true),
		("DEFINE TABLE h;", false),
	];
	let mut golden = Golden::new("spec_catalog_ids_are_stable_whether_the_block_runs_or_fails");
	let db = TestDb::new("DEFINE TABLE t; CREATE t:0 SET n = 0;").await;
	let session = TestDb::owner();
	for (body, speculates) in STEPS {
		let before = super::speculate::SPECULATED.with(std::cell::Cell::get);
		let responses = db.datastore().execute(body, &session, None).await.expect("runs");
		let speculated = super::speculate::SPECULATED.with(std::cell::Cell::get) > before;
		assert_eq!(speculated, *speculates, "`{body}` speculated: {responses:?}");
		let answers: Vec<bool> = responses.iter().map(|r| r.result.is_ok()).collect();
		golden.record(*body, format!("{answers:?}\n{}", catalog_ids(&db).await.join("\n")));
	}
	golden.check();
}

/// A block planned and dropped without running — cancelled before it starts,
/// or while a speculative run of one of its schema statements holds the first
/// or the second id the block's speculation leases — leaves those ids to the
/// next definitions: they take the ids they take where nothing was planned.
#[tokio::test]
async fn spec_a_block_planned_and_not_run_releases_what_its_speculation_leased() {
	use std::future::Future;
	use std::task::Poll;

	use crate::kvs::TransactionType;

	/// How the block is planned before the definitions run.
	#[derive(Debug, Clone, Copy)]
	enum Planning {
		/// Not at all.
		None,
		/// To its end, and the plan is dropped unrun.
		Whole,
		/// Until a speculative run holds this many ids, and the planning is
		/// dropped there.
		CancelledAtHold(usize),
	}

	const SETUP: &str = "DEFINE TABLE t;";
	const BLOCK: &str = "{ DEFINE TABLE fresh; DEFINE INDEX ix ON fresh FIELDS n; \
	                     RETURN SELECT * FROM fresh WHERE n = 1 }";
	const DEFINE: &str = "DEFINE TABLE fresh; DEFINE INDEX ix ON fresh FIELDS n;";
	let session = TestDb::owner();
	let mut catalogs = Vec::new();
	for planning in [
		Planning::None,
		Planning::Whole,
		Planning::CancelledAtHold(1),
		Planning::CancelledAtHold(2),
	] {
		let db = TestDb::new(SETUP).await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let mut planner = super::tests::planner(&ctx);
		planner.speculate_with(ctx.root().options.clone().map(std::sync::Arc::new));
		let bound = super::tests::bind(&ctx, BLOCK).await;
		match planning {
			Planning::None => {}
			Planning::Whole => {
				let before = super::speculate::SPECULATED.with(std::cell::Cell::get);
				let plan = super::plan(&planner, &bound).await.expect("the VM plans the block");
				assert!(super::runs_on_vm(plan.as_ref()));
				assert!(
					super::speculate::SPECULATED.with(std::cell::Cell::get) > before,
					"the block's schema statements ran speculatively"
				);
			}
			Planning::CancelledAtHold(at) => {
				super::speculate::PAUSED.with(|paused| paused.set(false));
				super::speculate::PAUSE_AFTER_HOLD.with(|left| left.set(at));
				let mut planned = std::pin::pin!(super::plan(&planner, &bound));
				let paused = std::future::poll_fn(|cx| match planned.as_mut().poll(cx) {
					Poll::Ready(_) => Poll::Ready(false),
					Poll::Pending if super::speculate::PAUSED.with(|p| p.get()) => {
						Poll::Ready(true)
					}
					Poll::Pending => Poll::Pending,
				})
				.await;
				super::speculate::PAUSE_AFTER_HOLD.with(|left| left.set(0));
				assert!(paused, "{planning:?}: planning reached a speculative run's hold");
			}
		}
		drop(planner);
		ctx.try_txn().expect("a transaction").cancel().await.expect("cancels");
		let responses = db.datastore().execute(DEFINE, &session, None).await.expect("runs");
		assert!(responses.iter().all(|r| r.result.is_ok()), "{planning:?}: {responses:?}");
		catalogs.push((planning, catalog_ids(&db).await));
	}
	let (_, unplanned) = catalogs.remove(0);
	for (planning, catalog) in catalogs {
		assert_eq!(catalog, unplanned, "the catalog after a block planned {planning:?}");
	}
}

/// A loop whose first iteration's speculative mapping is undone holds none of
/// the ids that mapping's speculative runs leased once the block is mapped,
/// since no program reserves them; one whose first iteration is kept holds
/// them for its program.
#[tokio::test]
async fn spec_a_loop_mapping_undone_releases_what_its_speculation_leased() {
	use crate::kvs::TransactionType;

	for (body, kept) in [
		("{ FOR $i IN [1] { DEFINE TABLE l }; RETURN 1 }", false),
		("{ FOR $i IN [1, 2] { DEFINE TABLE IF NOT EXISTS l }; RETURN 1 }", true),
	] {
		let db = TestDb::new("DEFINE TABLE t;").await;
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let mut planner = super::tests::planner(&ctx);
		planner.speculate_with(ctx.root().options.clone().map(std::sync::Arc::new));
		let bound = super::tests::bind(&ctx, body).await;
		let before = super::speculate::SPECULATED.with(std::cell::Cell::get);
		// The mapping may decline the block once its speculation has run.
		let _ = super::map::map(&planner, &bound).await;
		assert!(
			super::speculate::SPECULATED.with(std::cell::Cell::get) > before,
			"`{body}`: the loop's first iteration ran speculatively"
		);
		let held = &planner.speculation.as_ref().expect("speculates").held;
		assert_eq!(!held.is_empty(), kept, "`{body}`: {held:?}");
		ctx.try_txn().expect("a transaction").cancel().await.expect("cancels");
	}
}

/// A statement after a `DEFINE INDEX` in the block reads the same rows whether
/// the index is built in place or concurrently, as does the same read with no
/// index defined.
#[tokio::test]
async fn spec_a_read_after_a_concurrent_index_build_answers() {
	let mut golden = Golden::new("spec_a_read_after_a_concurrent_index_build_answers");
	let setup =
		"DEFINE TABLE t; FOR $i IN 0..2000 { CREATE type::record('t', $i) SET n = $i % 10 };";
	for body in [
		"RETURN count(SELECT * FROM t WHERE n = 5)",
		"{ RETURN count(SELECT * FROM t WHERE n = 5) }",
		"{ DEFINE INDEX by_n ON t FIELDS n CONCURRENTLY; RETURN count(SELECT * FROM t WHERE n = 5) }",
		"{ DEFINE INDEX by_n ON t FIELDS n; RETURN count(SELECT * FROM t WHERE n = 5) }",
		"{ DEFINE INDEX by_n ON t FIELDS n UNIQUE CONCURRENTLY; RETURN count(SELECT * FROM t WHERE n = 5) }",
	] {
		let db = TestDb::new(setup).await;
		let session = TestDb::owner();
		let responses = db.datastore().execute(body, &session, None).await.expect("runs");
		let answers: Vec<String> = responses.into_iter().map(|r| answer(r.result)).collect();
		golden.record(body, answers.join("\n"));
	}
	golden.check();
}

/// A parameter defined with a value that differs between evaluations reads
/// the value its definition stored, in the block and after it.
#[tokio::test]
async fn spec_a_parameter_defined_in_a_block_reads_the_value_it_stored() {
	for body in [
		"{ DEFINE PARAM $p VALUE rand::uuid(); RETURN $p }",
		"{ DEFINE PARAM $p VALUE rand::uuid(); CREATE t:1 SET p = $p; RETURN (SELECT VALUE p FROM ONLY t:1) }",
		"{ DEFINE PARAM $p VALUE time::now(); RETURN $p }",
	] {
		let db = TestDb::new("DEFINE TABLE t;").await;
		let session = TestDb::owner();
		let sql = format!("{body}; RETURN $p;");
		let responses = db.datastore().execute(&sql, &session, None).await.expect("runs");
		let values: Vec<_> = responses.into_iter().map(|r| answer(r.result)).collect();
		assert_eq!(values[0], values[1], "`{body}`: the block read what the definition stored");
	}
	// A literal value is known to the statements after it; one a second
	// evaluation may answer differently is evaluated by the real run alone,
	// so a block reading the parameter after it is declined whole.
	for (value, declined) in [("3", 0), ("rand::uuid()", 1), ("(SELECT VALUE n FROM ONLY t:0)", 1)]
	{
		let body = format!(
			"{{ DEFINE PARAM $p VALUE {value}; CREATE t:1 SET p = $p; RETURN SELECT VALUE p FROM t:1 }}"
		);
		let db = TestDb::new("DEFINE TABLE t; CREATE t:0 SET n = 1;").await;
		let session = TestDb::owner();
		let before = Counted::now();
		let responses = db.datastore().execute(&body, &session, None).await.expect("runs");
		let counted = Counted::since(before);
		assert!(responses.iter().all(|r| r.result.is_ok()), "`{body}`: {responses:?}");
		assert_eq!(
			counted,
			Counted {
				declined,
				..Counted::default()
			},
			"`{body}` declined, compiled or planned a statement once its block ran"
		);
	}
}

/// Blocks whose schema statements run speculatively beside what else reads
/// the transaction answer and leave the recorded entries, for the owner and for
/// a checked record user, and with every read a region.
#[tokio::test]
async fn spec_blocks_beside_other_readers_answer_and_leave_their_entries() {
	let mut golden = Golden::new("spec_blocks_beside_other_readers_answer_and_leave_their_entries");
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader())] {
		for body in SPEC_BLOCKS {
			let statements =
				[*body, "SELECT * FROM t ORDER BY id", "SELECT * FROM fresh ORDER BY id"];
			let (answers, keys, counted) = ran_counted(&statements, &session).await;
			golden.record(format!("{who}: {body}"), render_run(&answers, &keys));
			assert_eq!(
				counted.compiled, 0,
				"`{body}` for the {who} compiled or planned a statement once its block ran"
			);
		}
	}
	let setup = "DEFINE TABLE t; FOR $i IN 0..300 { CREATE type::record('t', $i) SET n = $i };";
	let config = || surrealdb_cnf::ConfigMap::empty().with_key_value("region_row_threshold", "1");
	let handed = super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed);
	let body = "{ DEFINE FIELD k ON t COMPUTED n * 2; \
	   RETURN SELECT VALUE $this.k + n FROM t WHERE n % 50 = 0 ORDER BY n; \
	   RETURN SELECT math::sum($this.k) AS s, count() AS c FROM t GROUP ALL }";
	let db = TestDb::new_with_config(setup, config()).await;
	let session = TestDb::owner();
	let responses = db.datastore().execute(body, &session, None).await.expect("runs");
	let answers: Vec<String> = responses.into_iter().map(|r| answer(r.result)).collect();
	golden.record("a region reading a speculated field", answers.join("\n"));
	assert!(
		super::region::HANDED_TO_WORKERS.load(std::sync::atomic::Ordering::Relaxed) > handed,
		"the reads ran as regions"
	);
	golden.check();
}

/// Throughput of DEFINE-heavy blocks, planned against the overlay and with
/// the schema statements unmodelled (which declines a block whose later
/// statements read what they define); with the blocks each declined.
#[test]
#[ignore = "measurement; run with --release --ignored --nocapture"]
fn overlay_define_heavy_throughput() {
	let interleaved: String = (0..20)
		.map(|i| format!("DEFINE FIELD f{i} ON t VALUE {i}; CREATE t:[{i}] SET n = {i};"))
		.collect();
	let shapes = [
		("interleaved 20", format!("{{ {interleaved} RETURN count(SELECT * FROM t) }}")),
		(
			"loop IF NOT EXISTS 200",
			"FOR $i IN 0..200 { DEFINE FIELD IF NOT EXISTS k ON t VALUE 1; CREATE t:[$i] SET n = $i }"
				.to_owned(),
		),
		(
			"table index write 1",
			"{ DEFINE TABLE fresh; DEFINE FIELD n ON fresh TYPE int; DEFINE INDEX by_n ON fresh FIELDS n; \
			  FOR $i IN 0..200 { CREATE fresh:[$i] SET n = $i % 7 }; RETURN count(SELECT * FROM fresh WHERE n = 3) }"
				.to_owned(),
		),
	];
	for (name, sql) in &shapes {
		for (engine, speculate) in [("overlay", true), ("unmodelled", false)] {
			let mut best = std::time::Duration::MAX;
			let mut declined = 0;
			for _ in 0..5 {
				let runtime = tokio::runtime::Builder::new_current_thread()
					.enable_all()
					.build()
					.expect("a runtime");
				runtime.block_on(async {
					let db = TestDb::new("DEFINE TABLE t;").await;
					super::speculate::WITHOUT_SPECULATION.with(|off| off.set(!speculate));
					let before = Counted::now();
					let session = TestDb::owner();
					let start = web_time::Instant::now();
					let responses =
						db.datastore().execute(sql, &session, None).await.expect("runs");
					let took = start.elapsed();
					assert!(responses.iter().all(|r| r.result.is_ok()), "`{name}` on {engine}");
					best = best.min(took);
					let counted = Counted::since(before);
					assert_eq!(counted.compiled, 0, "`{name}` on {engine} compiled while it ran");
					declined = counted.declined;
					super::speculate::WITHOUT_SPECULATION.with(|off| off.set(false));
				});
			}
			println!(
				"{name:<24} {engine:<14} {:>9.3} ms  declined {declined}",
				best.as_secs_f64() * 1e3
			);
		}
	}
}

/// The interleaved DEFINE-heavy block run `SPEC_PROFILE_RUNS` times in one
/// mode (`SPEC_PROFILE_ENGINE`: `overlay` or `unmodelled`), for a sampling
/// profiler to attach to; with `ab`, the modes take turns run by run and the
/// median of each is reported, which a loaded machine skews less than runs
/// taken one mode after another.
#[test]
#[ignore = "profiling; run with --ignored"]
fn spec_interleaved_profile() {
	let runs: usize =
		std::env::var("SPEC_PROFILE_RUNS").ok().and_then(|v| v.parse().ok()).unwrap_or(2000);
	let engine = std::env::var("SPEC_PROFILE_ENGINE").unwrap_or_else(|_| "overlay".to_owned());
	let interleaved: String = (0..20)
		.map(|i| format!("DEFINE FIELD f{i} ON t VALUE {i}; CREATE t:[{i}] SET n = {i};"))
		.collect();
	let sql = format!("{{ {interleaved} RETURN count(SELECT * FROM t) }}");
	let engines: Vec<&str> = match engine.as_str() {
		"ab" => vec!["overlay", "unmodelled"],
		one => vec![one],
	};
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let session = TestDb::owner();
		let mut took: Vec<Vec<f64>> = vec![Vec::new(); engines.len()];
		for _ in 0..runs {
			for (at, engine) in engines.iter().enumerate() {
				let db = TestDb::new("DEFINE TABLE t;").await;
				super::speculate::WITHOUT_SPECULATION.with(|off| off.set(*engine == "unmodelled"));
				let start = web_time::Instant::now();
				let responses = db.datastore().execute(&sql, &session, None).await.expect("runs");
				took[at].push(start.elapsed().as_secs_f64() * 1e3);
				super::speculate::WITHOUT_SPECULATION.with(|off| off.set(false));
				assert!(responses.iter().all(|r| r.result.is_ok()));
			}
		}
		for (at, engine) in engines.iter().enumerate() {
			let times = &mut took[at];
			times.sort_by(f64::total_cmp);
			println!("{engine}: median {:.3} ms", times[times.len() / 2]);
		}
	});
}

/// A block planned for the executor's statement, as the executor plans it:
/// speculating, so its schema statements are modelled.
async fn lowered_block(
	ctx: &crate::exec::ExecutionContext,
	sql: &str,
) -> Result<super::program::Program, super::NotCovered> {
	let mut planner = super::tests::planner(ctx);
	planner.speculate_with(Some(std::sync::Arc::new(ctx.root().options.clone().expect("options"))));
	let bound = super::tests::bind(ctx, sql).await;
	let physical = super::optimize::optimize(super::map::map(&planner, &bound).await?);
	super::lower::lower(&planner, &physical).await
}

/// A read after a `DEFINE INDEX` in the block is lowered against the index
/// the block defines, and reads through it: after writes to the indexed
/// table, and over a table a write before it defines.
#[tokio::test]
async fn overlay_a_read_after_define_index_reads_through_it() {
	use crate::kvs::TransactionType;
	let db = TestDb::new("DEFINE TABLE t; CREATE t:0 SET n = 0;").await;
	for (sql, indexed) in [
		("{ DEFINE INDEX by_n ON t FIELDS n; RETURN SELECT VALUE id FROM t WHERE n = 1 }", true),
		("{ DEFINE FIELD k ON t VALUE 1; RETURN SELECT VALUE id FROM t WHERE n = 1 }", false),
		(
			"{ CREATE t:1 SET n = 1; DEFINE INDEX by_n ON t FIELDS n; RETURN SELECT VALUE id FROM t WHERE n = 1 }",
			true,
		),
		(
			"{ CREATE fresh:1 SET n = 1; DEFINE INDEX by_n ON fresh FIELDS n; RETURN SELECT VALUE id FROM fresh WHERE n = 1 }",
			true,
		),
	] {
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		let program = lowered_block(&ctx, sql).await.expect("lowers");
		let reads_index = program
			.pipelines
			.iter()
			.any(|pipeline| matches!(pipeline.source, super::program::Source::Index { .. }));
		assert_eq!(reads_index, indexed, "`{sql}`");
		ctx.txn().cancel().await.expect("cancel");
	}
}

/// A block whose catalog changes depend on what it meets, and whose later
/// statements read what they may change, is declined whole, and the decline
/// names its reason; one whose later statements read none of it is lowered.
#[tokio::test]
async fn overlay_data_dependent_ddl_is_declined_whole_by_name() {
	use crate::kvs::TransactionType;
	let db = TestDb::new(
		"DEFINE TABLE t; CREATE t:0 SET n = 0; DEFINE TABLE log; \
		 DEFINE TABLE e; DEFINE EVENT noted ON e THEN (CREATE log SET of = $after.id); \
		 DEFINE FUNCTION fn::mk() { DEFINE FIELD IF NOT EXISTS made ON t VALUE 1; RETURN 1 }; \
		 DEFINE FUNCTION fn::plain() { RETURN 1 }; DEFINE FUNCTION fn::none() { NONE };",
	)
	.await;
	for (sql, declined) in [
		// A function an iteration or a condition read from data redefines,
		// called after: the call reads the definition the run leaves.
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE FUNCTION OVERWRITE fn::none() { RETURN 7 } }; RETURN fn::none() }",
			true,
		),
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { REMOVE FUNCTION fn::none }; RETURN fn::none() }",
			true,
		),
		("{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE PARAM $q VALUE 4 }; RETURN $q }", true),
		// Modelled ahead in the block, the call is lowered against the
		// definition.
		("{ DEFINE FUNCTION OVERWRITE fn::none() { RETURN 7 }; RETURN fn::none() }", false),
		// An `OVERWRITE` in a loop may write a new definition on each
		// iteration, so the call after the loop reads what the run leaves.
		(
			"{ FOR $i IN 0..2 { DEFINE FUNCTION OVERWRITE fn::none() { RETURN 7 } }; RETURN fn::none() }",
			true,
		),
		("{ FOR $i IN [3, 4] { DEFINE PARAM OVERWRITE $q VALUE $i }; RETURN $q }", true),
		("FOR $i IN 0..3 { DEFINE FIELD OVERWRITE k ON t VALUE $i; CREATE t SET n = $i }", true),
		(
			"FOR $i IN 0..3 { IF $i = 1 { DEFINE INDEX u ON t FIELDS n }; CREATE t SET n = $i }",
			true,
		),
		(
			"{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { REMOVE TABLE t }; RETURN SELECT * FROM t }",
			true,
		),
		("{ LET $x = fn::mk(); RETURN SELECT * FROM t }", true),
		("{ LET $x = fn::plain(); RETURN SELECT * FROM t }", false),
		("FOR $i IN 0..3 { CREATE e SET n = $i; CREATE t SET n = $i }", false),
		("{ IF (SELECT VALUE n FROM ONLY t:0) = 0 { DEFINE TABLE other }; RETURN 1 }", false),
		("{ DEFINE FIELD k ON t VALUE 1; CREATE t:1; RETURN SELECT * FROM t }", false),
		// A table a write defines: a schema statement defining it where there
		// is none is modelled, any other is not; after a write to a table it
		// evaluates, nothing over a table the catalog does not hold is.
		("{ CREATE fresh:1; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }", false),
		("{ CREATE fresh:1; DEFINE TABLE fresh; CREATE fresh:2 }", true),
		("{ CREATE fresh:1; REMOVE TABLE fresh; RETURN SELECT * FROM fresh }", true),
		(
			"{ LET $r = type::record('fresh', (SELECT VALUE n FROM ONLY t:0)); CREATE $r; DEFINE FIELD k ON fresh VALUE 1; CREATE fresh:2 }",
			true,
		),
		// Rows written before a schema statement that reads them.
		(
			"{ CREATE t:1 SET n = 1; DEFINE INDEX u ON t FIELDS n UNIQUE; CREATE t:2 SET n = 2 }",
			false,
		),
	] {
		let ctx = db.exec_ctx_as(&TestDb::owner(), TransactionType::Write).await;
		match lowered_block(&ctx, sql).await {
			Ok(_) => assert!(!declined, "`{sql}` lowered"),
			Err(why) => {
				assert!(declined, "`{sql}` declined: {why:?}");
				assert_eq!(why.reason, super::speculate::DATA_DEPENDENT_DDL, "`{sql}`");
			}
		}
		ctx.txn().cancel().await.expect("cancel");
	}
}
