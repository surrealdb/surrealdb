//! A unit the compile cache keeps is served to every principal of the class
//! it was compiled for, so the session's own values — `$auth`, `$access`,
//! `$token`, `$session` — are read when the unit runs, never folded into its
//! code. Each test calls the same code as two record users of one access
//! method, in one namespace and database, and checks that each sees only
//! what its own `$auth` selects, whichever of them compiled the unit.

use crate::exec::operators::test_util::TestDb;
use crate::val::Value;

fn record_user(user: &str) -> crate::dbs::Session {
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
	crate::dbs::Session::for_record("test", "test", "user", rid)
}

const SETUP: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
	DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
	DEFINE INDEX doc_txt ON doc FIELDS txt FULLTEXT ANALYZER simple BM25;
	CREATE user:alice, user:bob;
	CREATE doc:1 SET owner = user:alice, txt = 'one';
	CREATE doc:2 SET owner = user:bob, txt = 'one';
	DEFINE FUNCTION fn::mine() { RETURN SELECT VALUE id FROM doc WHERE owner = $auth; };
	DEFINE FUNCTION fn::mine_expr() { RETURN $auth; };
	DEFINE FUNCTION fn::by_session() { RETURN SELECT VALUE id FROM doc WHERE owner = $session.rd; };
	DEFINE FUNCTION fn::found($q: any) {
		RETURN SELECT VALUE id FROM doc WHERE owner = $auth AND txt @@ $q;
	};
";

/// The last statement's value of `sql` run as `session`.
async fn last(db: &TestDb, session: &crate::dbs::Session, sql: &str) -> Value {
	let mut results = db.datastore().execute(sql, session, None).await.expect("runs");
	let result = results.pop().expect("a result").result;
	result.map(Value::from).unwrap_or_else(|e| Value::from(e.to_string()))
}

/// `sql` run as alice and then as bob answers each their own rows.
async fn each_sees_their_own(sql: &str, alice_sees: &str, bob_sees: &str) {
	let db = TestDb::new_with_auth(SETUP).await;
	let alice = last(&db, &record_user("alice"), sql).await;
	let bob = last(&db, &record_user("bob"), sql).await;
	let alice_again = last(&db, &record_user("alice"), sql).await;
	let parse = |text: &str| Value::from(crate::syn::value(text).expect("parses"));
	assert_eq!(alice, parse(alice_sees), "alice ran `{sql}` first");
	assert_eq!(bob, parse(bob_sees), "bob ran `{sql}` after alice");
	assert_eq!(alice_again, parse(alice_sees), "alice ran `{sql}` after bob");
}

#[tokio::test]
async fn a_function_body_reads_the_callers_auth() {
	each_sees_their_own("RETURN fn::mine()", "[doc:1]", "[doc:2]").await;
}

#[tokio::test]
async fn a_late_bound_call_reads_the_callers_auth() {
	each_sees_their_own(
		"{ IF (SELECT VALUE id FROM doc WHERE txt = 'never') { DEFINE TABLE other }; RETURN fn::mine(); }",
		"[doc:1]",
		"[doc:2]",
	)
	.await;
}

#[tokio::test]
async fn a_specialised_body_reads_the_callers_auth() {
	each_sees_their_own("RETURN fn::found('one')", "[doc:1]", "[doc:2]").await;
}

#[tokio::test]
async fn a_late_bound_specialised_body_reads_the_callers_auth() {
	each_sees_their_own(
		"{ IF (SELECT VALUE id FROM doc WHERE txt = 'never') { DEFINE TABLE other }; RETURN fn::found('one'); }",
		"[doc:1]",
		"[doc:2]",
	)
	.await;
}

#[tokio::test]
async fn a_closure_reads_the_callers_auth() {
	each_sees_their_own("RETURN [1].map(|$x| fn::mine())", "[[doc:1]]", "[[doc:2]]").await;
}

#[tokio::test]
async fn a_function_expression_reads_the_callers_auth() {
	each_sees_their_own("RETURN fn::mine_expr()", "user:alice", "user:bob").await;
}

#[tokio::test]
async fn a_late_bound_closure_reads_the_callers_auth() {
	each_sees_their_own(
		"{ IF (SELECT VALUE id FROM doc WHERE txt = 'never') { DEFINE TABLE other }; RETURN [1].map(|$x| SELECT VALUE id FROM doc WHERE owner = $auth); }",
		"[[doc:1]]",
		"[[doc:2]]",
	)
	.await;
}

#[tokio::test]
async fn a_function_body_reads_the_callers_session() {
	each_sees_their_own("RETURN fn::by_session()", "[doc:1]", "[doc:2]").await;
}

/// A record user whose session carries `token` as `$token`.
fn record_user_with_token(user: &str, token: &str) -> crate::dbs::Session {
	let mut session = record_user(user);
	session.tk = Some(crate::syn::value(token).expect("parses"));
	session
}

/// The last statement's outcome of `sql` run as `session`: its value, or
/// whether it failed.
async fn outcome(db: &TestDb, session: &crate::dbs::Session, sql: &str) -> Result<Value, ()> {
	let mut results = db.datastore().execute(sql, session, None).await.expect("runs");
	results.pop().expect("a result").result.map(Value::from).map_err(|_| ())
}

/// `sql` run on one warm datastore set up with `setup` as `denied`, then as
/// `admitted`, then as `denied` again: the first and last fail, the second
/// answers `admitted_sees`. Whoever compiled a guard first, each caller is
/// judged on its own session.
async fn guard_judges_each_caller(
	setup: &str,
	sql: &str,
	denied: &crate::dbs::Session,
	admitted: &crate::dbs::Session,
	admitted_sees: &str,
) {
	let db = TestDb::new_with_auth(setup).await;
	let expected = Value::from(crate::syn::value(admitted_sees).expect("parses"));
	assert_eq!(outcome(&db, denied, sql).await, Err(()), "denied ran `{sql}` first");
	assert_eq!(outcome(&db, admitted, sql).await, Ok(expected.clone()), "admitted after denied");
	assert_eq!(outcome(&db, denied, sql).await, Err(()), "denied ran `{sql}` after admitted");
	assert_eq!(outcome(&db, admitted, sql).await, Ok(expected), "admitted again");
}

/// An analyzer whose `FUNCTION` is guarded by `guard`, over an index on
/// `doc.txt`, and a function `fn::gated` whose body reads parameter `$gate`,
/// guarded by `param_guard`. The analyzer's call is served from the compile
/// cache to every record user of the access method, and the function and
/// parameter guards are judged inside it.
fn guarded_setup(guard: &str, param_guard: &str) -> String {
	format!(
		"
		DEFINE ACCESS user ON DATABASE TYPE RECORD;
		DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
		DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
		CREATE user:alice SET admin = true;
		CREATE user:bob SET admin = false;
		DEFINE PARAM $gate VALUE 'x' PERMISSIONS {param_guard};
		DEFINE FUNCTION fn::low($s: string) -> string {{ RETURN string::lowercase($s) }}
			PERMISSIONS {guard};
		DEFINE FUNCTION fn::gated($s: string) -> string {{
			RETURN IF $gate = 'x' {{ string::lowercase($s) }} ELSE {{ 'never' }};
		}};
		DEFINE ANALYZER low FUNCTION fn::low TOKENIZERS blank;
		DEFINE ANALYZER gated FUNCTION fn::gated TOKENIZERS blank;
		DEFINE INDEX doc_low ON doc FIELDS txt FULLTEXT ANALYZER low BM25;
		DEFINE INDEX doc_gated ON doc FIELDS txt2 FULLTEXT ANALYZER gated BM25;
		CREATE doc:1 SET txt = 'one', txt2 = 'one';
		"
	)
}

const LOW_SEARCH: &str = "RETURN SELECT VALUE id FROM doc WHERE txt @@ 'ONE'";
const GATED_SEARCH: &str = "RETURN SELECT VALUE id FROM doc WHERE txt2 @@ 'ONE'";

#[tokio::test]
async fn a_function_guard_reads_the_callers_session() {
	guard_judges_each_caller(
		&guarded_setup("WHERE $session.rd = user:alice", "FULL"),
		LOW_SEARCH,
		&record_user("bob"),
		&record_user("alice"),
		"[doc:1]",
	)
	.await;
}

#[tokio::test]
async fn a_function_guard_reads_the_callers_auth() {
	guard_judges_each_caller(
		&guarded_setup("WHERE (SELECT VALUE admin FROM user WHERE id = $auth)[0] = true", "FULL"),
		LOW_SEARCH,
		&record_user("bob"),
		&record_user("alice"),
		"[doc:1]",
	)
	.await;
}

#[tokio::test]
async fn a_function_guard_reads_the_callers_token() {
	guard_judges_each_caller(
		&guarded_setup("WHERE count(SELECT * FROM user WHERE $token.sub = 'alice') > 0", "FULL"),
		LOW_SEARCH,
		&record_user_with_token("alice", "{ sub: 'bob' }"),
		&record_user_with_token("alice", "{ sub: 'alice' }"),
		"[doc:1]",
	)
	.await;
}

#[tokio::test]
async fn a_function_guard_reads_the_callers_access() {
	// `$access` names the access method, which the frame's class holds, so
	// two principals of one class read the same value; the guard admits one
	// and the class's other access method is refused on its own frame.
	let setup = format!(
		"{} DEFINE ACCESS other ON DATABASE TYPE RECORD;",
		guarded_setup("WHERE count(SELECT * FROM user WHERE $access = 'user') > 0", "FULL")
	);
	let other = {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "bob"));
		crate::dbs::Session::for_record("test", "test", "other", rid)
	};
	guard_judges_each_caller(&setup, LOW_SEARCH, &other, &record_user("alice"), "[doc:1]").await;
}

#[tokio::test]
async fn a_param_guard_reads_the_callers_session() {
	guard_judges_each_caller(
		&guarded_setup("FULL", "WHERE $session.rd = user:alice"),
		GATED_SEARCH,
		&record_user("bob"),
		&record_user("alice"),
		"[doc:1]",
	)
	.await;
}

#[tokio::test]
async fn a_param_guard_reads_the_callers_auth() {
	guard_judges_each_caller(
		&guarded_setup("FULL", "WHERE (SELECT VALUE admin FROM user WHERE id = $auth)[0] = true"),
		GATED_SEARCH,
		&record_user("bob"),
		&record_user("alice"),
		"[doc:1]",
	)
	.await;
}

/// `sql` run on one warm datastore set up with `setup` as each of `runs` in
/// turn, each answering its expected outcome: a value, or `None` for a
/// refusal.
async fn each_answers(setup: &str, sql: &str, runs: &[(&crate::dbs::Session, Option<&str>)]) {
	let db = TestDb::new_with_auth(setup).await;
	for (i, (session, expected)) in runs.iter().enumerate() {
		let expected =
			expected.map(|text| Value::from(crate::syn::value(text).expect("parses"))).ok_or(());
		assert_eq!(outcome(&db, session, sql).await, expected, "run {i} of `{sql}`");
	}
}

/// Alice, then bob, then alice again, each answering their own outcome.
async fn alice_bob_alice(setup: &str, sql: &str, alice: Option<&str>, bob: Option<&str>) {
	let (a, b) = (record_user("alice"), record_user("bob"));
	each_answers(setup, sql, &[(&a, alice), (&b, bob), (&a, alice)]).await;
}

const SECURED: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE ACCESS other ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE secret SCHEMALESS PERMISSIONS FOR select WHERE owner = $auth;
	DEFINE TABLE noted SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD note ON noted PERMISSIONS FOR select WHERE owner = $auth;
	DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE made SCHEMALESS PERMISSIONS FULL;
	DEFINE FIELD creator ON made VALUE $auth;
	DEFINE TABLE watched SCHEMALESS PERMISSIONS FULL;
	DEFINE EVENT seen ON watched WHEN $event = 'CREATE' THEN {
		CREATE log SET who = $auth, doc = $after.id
	};
	DEFINE TABLE thing SCHEMALESS PERMISSIONS FULL;
	CREATE user:alice, user:bob;
	CREATE doc:1 SET owner = user:alice, sub = 'alice';
	CREATE doc:2 SET owner = user:bob, sub = 'bob';
	CREATE secret:1 SET owner = user:alice;
	CREATE secret:2 SET owner = user:bob;
	CREATE noted:1 SET owner = user:alice, note = 'a';
	CREATE noted:2 SET owner = user:bob, note = 'b';
	CREATE thing:1 SET n = 1;
	DEFINE FIELD mine ON thing COMPUTED (|$x| (SELECT VALUE id FROM doc WHERE owner = $auth))(n);
	DEFINE PARAM $hush VALUE 'hushed' PERMISSIONS WHERE $auth = user:alice;
	DEFINE FUNCTION fn::by_token() { RETURN SELECT VALUE id FROM doc WHERE sub = $token.sub; };
	DEFINE FUNCTION fn::access() { RETURN $access; };
	DEFINE FUNCTION fn::hush() { RETURN $hush; };
	DEFINE FUNCTION fn::apply($f: any) { RETURN $f(1); };
";

#[tokio::test]
async fn a_function_body_reads_the_callers_token() {
	let alice = record_user_with_token("alice", "{ sub: 'alice' }");
	let bob = record_user_with_token("bob", "{ sub: 'bob' }");
	each_answers(
		SECURED,
		"RETURN fn::by_token()",
		&[(&alice, Some("[doc:1]")), (&bob, Some("[doc:2]")), (&alice, Some("[doc:1]"))],
	)
	.await;
}

#[tokio::test]
async fn a_function_body_reads_the_callers_access() {
	let user = record_user("alice");
	let other = {
		let rid =
			crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "bob"));
		crate::dbs::Session::for_record("test", "test", "other", rid)
	};
	each_answers(
		SECURED,
		"RETURN fn::access()",
		&[(&user, Some("'user'")), (&other, Some("'other'")), (&user, Some("'user'"))],
	)
	.await;
}

#[tokio::test]
async fn a_table_permission_reads_the_callers_auth() {
	alice_bob_alice(
		SECURED,
		"RETURN SELECT VALUE id FROM secret",
		Some("[secret:1]"),
		Some("[secret:2]"),
	)
	.await;
}

#[tokio::test]
async fn a_dispatched_table_permission_reads_the_callers_auth() {
	alice_bob_alice(
		SECURED,
		"RETURN SELECT VALUE id FROM type::table(rand::enum('secret'))",
		Some("[secret:1]"),
		Some("[secret:2]"),
	)
	.await;
}

#[tokio::test]
async fn a_field_permission_reads_the_callers_auth() {
	alice_bob_alice(
		SECURED,
		"RETURN SELECT VALUE note FROM noted ORDER BY id",
		Some("['a', NONE]"),
		Some("[NONE, 'b']"),
	)
	.await;
	alice_bob_alice(
		SECURED,
		"RETURN SELECT VALUE note FROM type::table(rand::enum('noted')) ORDER BY id",
		Some("['a', NONE]"),
		Some("[NONE, 'b']"),
	)
	.await;
}

#[tokio::test]
async fn a_param_permission_reads_the_callers_auth() {
	alice_bob_alice(SECURED, "RETURN $hush", Some("'hushed'"), None).await;
	alice_bob_alice(SECURED, "RETURN fn::hush()", Some("'hushed'"), None).await;
}

#[tokio::test]
async fn a_field_value_on_write_reads_the_callers_auth() {
	alice_bob_alice(
		SECURED,
		"RETURN (CREATE ONLY made RETURN VALUE creator)",
		Some("user:alice"),
		Some("user:bob"),
	)
	.await;
}

#[tokio::test]
async fn an_event_reads_the_callers_auth() {
	alice_bob_alice(
		SECURED,
		"LET $d = (CREATE ONLY watched RETURN VALUE id); RETURN SELECT VALUE who FROM log WHERE doc = $d",
		Some("[user:alice]"),
		Some("[user:bob]"),
	)
	.await;
}

/// A closure invoked mid-computation of a computed field, or handed to a
/// function's body, runs a unit the compile cache keeps for the frame's class,
/// which reads the caller's `$auth` when it runs.
#[tokio::test]
async fn a_closure_unit_reads_the_callers_auth() {
	alice_bob_alice(
		SECURED,
		"RETURN SELECT VALUE mine FROM thing",
		Some("[[doc:1]]"),
		Some("[[doc:2]]"),
	)
	.await;
	alice_bob_alice(
		SECURED,
		"RETURN fn::apply(|$x| SELECT VALUE id FROM doc WHERE owner = $auth)",
		Some("[doc:1]"),
		Some("[doc:2]"),
	)
	.await;
}

/// An edge table whose `SELECT` permission hides one of the two edges `person:1` has. The edges
/// have keys of their own, which order what a hop reaches.
const GRAPH: &str = "
	DEFINE ACCESS user ON DATABASE TYPE RECORD;
	DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE person SCHEMALESS PERMISSIONS FULL;
	DEFINE TABLE knows TYPE RELATION SCHEMALESS PERMISSIONS FOR select WHERE shown = true;
	CREATE user:alice;
	CREATE person:1, person:2, person:3;
	RELATE person:1->knows:1->person:2 SET shown = true;
	RELATE person:1->knows:2->person:3 SET shown = false;
	DEFINE FUNCTION fn::friends() { RETURN person:1->knows->person; };
	DEFINE FUNCTION fn::degree() { RETURN count(person:1->knows); };
";

/// Whether `program`, or a program among its units, holds an instruction `found` admits.
fn holds(
	program: &super::program::Program,
	found: &dyn Fn(&super::program::Instr) -> bool,
) -> bool {
	program.pipelines.iter().any(|pipeline| pipeline.code.iter().any(found))
		|| program.host_code.iter().any(|pipeline| pipeline.code.iter().any(found))
		|| program.units.iter().any(|unit| holds(unit, found))
}

/// Whether a lookup of `program` reaches vertices through edges in one hop.
fn reads_through_edges(program: &super::program::Program) -> bool {
	program.lookups.iter().flat_map(|spec| &spec.hops).any(|hop| {
		matches!(
			&*hop.read,
			super::program::HopRead::Graph {
				targets: Some(_),
				..
			}
		)
	}) || program.units.iter().any(|unit| reads_through_edges(unit))
}

/// The edge to the vertex beyond it, and the count of a record's edges, are read off the
/// adjacency keys only for a caller whose reads go unchecked: an owner's code takes both, a
/// record user's code reads each edge and is judged by the edge table's permission.
#[tokio::test]
async fn only_an_unchecked_callers_code_reads_the_graph_off_its_adjacency_keys() {
	use crate::kvs::TransactionType::Read;
	let db = TestDb::new_with_auth(GRAPH).await;
	let degree =
		|instr: &super::program::Instr| matches!(instr, super::program::Instr::Degree { .. });
	let owner = db.exec_ctx_as(&TestDb::owner(), Read).await;
	let user = db.exec_ctx_as(&record_user("alice"), Read).await;
	let counted = "SELECT count(->knows) AS degree FROM person";
	let walked = "SELECT VALUE ->knows->person FROM person";
	let program = super::tests_placement::program_of(&owner, counted).await;
	assert!(holds(&program, &degree), "an owner counts off the adjacency");
	let program = super::tests_placement::program_of(&owner, walked).await;
	assert!(reads_through_edges(&program), "an owner reads through the edges");
	let program = super::tests_placement::program_of(&user, counted).await;
	assert!(!holds(&program, &degree), "a record user's count reads each edge");
	let program = super::tests_placement::program_of(&user, walked).await;
	assert!(!reads_through_edges(&program), "a record user reads each edge");
}

/// The same statements and the same function bodies answer an owner every edge and a record
/// user the edges the edge table's permission shows, whichever of them ran the code first.
#[tokio::test]
async fn graph_reads_off_the_adjacency_keys_are_never_served_to_a_checked_caller() {
	let db = TestDb::new_with_auth(GRAPH).await;
	let owner = TestDb::owner();
	let alice = record_user("alice");
	let parse = |text: &str| Value::from(crate::syn::value(text).expect("parses"));
	let cases = [
		("RETURN person:1->knows->person", "[person:2, person:3]", "[person:2]"),
		("RETURN count(person:1->knows)", "2", "1"),
		("RETURN fn::friends()", "[person:2, person:3]", "[person:2]"),
		("RETURN fn::degree()", "2", "1"),
		("SELECT VALUE count(->knows) FROM person:1..=3", "[2, 0, 0]", "[1, 0, 0]"),
		(
			"SELECT VALUE ->knows->person FROM person:1..=3",
			"[[person:2, person:3], [], []]",
			"[[person:2], [], []]",
		),
	];
	for (sql, owner_sees, alice_sees) in cases {
		for (first, second) in [(&owner, &alice), (&alice, &owner)] {
			let one = last(&db, first, sql).await;
			let two = last(&db, second, sql).await;
			let (owner_got, alice_got) = if std::ptr::eq(first, &owner) {
				(one, two)
			} else {
				(two, one)
			};
			assert_eq!(owner_got, parse(owner_sees), "the owner ran `{sql}`");
			assert_eq!(alice_got, parse(alice_sees), "alice ran `{sql}`");
		}
	}
}
