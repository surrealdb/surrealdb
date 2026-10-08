//! Graph and reference lookups lowered to [`Instr::Lookup`], and closures
//! lowered to the values they make and the loops the builtins taking them
//! run, answer their values, for the owner and for a record user whose reads
//! are checked against the tables' permissions and field views.

use surrealdb_types::ToSql;

use super::golden::Golden;
use super::program::Instr;
use super::tests::{bind, outcome, planner};
use crate::exec::operators::test_util::TestDb;

const SETUP: &str = "
	DEFINE TABLE person PERMISSIONS FOR select WHERE hidden != true;
	DEFINE TABLE knows TYPE RELATION PERMISSIONS FOR select WHERE since > 2000;
	DEFINE FIELD secret ON knows PERMISSIONS FOR select WHERE $auth.id = user:x;
	DEFINE FIELD twice ON knows COMPUTED since * 2;
	DEFINE TABLE likes TYPE RELATION PERMISSIONS FULL;
	DEFINE TABLE pal TYPE RELATION IN person OUT person LIGHTWEIGHT;
	DEFINE TABLE cached INLINE EDGES 4 PERMISSIONS FULL;
	DEFINE TABLE post PERMISSIONS FOR select WHERE draft != true;
	DEFINE FIELD author ON post TYPE option<record<person>> REFERENCE;
	DEFINE FIELD editor ON post TYPE option<record<person>> REFERENCE;
	CREATE person:a SET name = 'a', n = 1;
	CREATE person:b SET name = 'b', n = 2;
	CREATE person:c SET name = 'c', n = 3;
	CREATE person:h SET name = 'h', hidden = true;
	CREATE cached:1, cached:2;
	RELATE person:a->knows:k1->person:b SET since = 2020, secret = 's1';
	RELATE person:a->knows:k2->person:h SET since = 2021;
	RELATE person:a->knows:k3->person:c SET since = 1990;
	RELATE person:a->likes:l1->person:c;
	RELATE person:a->likes:l2->person:h;
	RELATE person:b->likes:l3->person:a;
	RELATE person:c->likes:l4->person:b;
	RELATE person:a->pal->person:b;
	RELATE person:c->pal->person:a;
	RELATE cached:1->likes:l5->cached:2;
	RELATE cached:1->likes:l6->person:a;
	CREATE post:1 SET author = person:a;
	CREATE post:2 SET author = person:a, editor = person:a, draft = true;
	CREATE post:3 SET editor = person:a;
";

/// Lookups, each a program of instructions alone for the owner and for a
/// record user. A record user's wildcard hop reads records of tables lowering
/// does not know as the scans resolve them.
const LOOKUPS: &[&str] = &[
	"SELECT VALUE ->knows FROM person:a",
	"SELECT VALUE ->knows->person FROM person:a",
	"SELECT VALUE ->likes->person FROM person:a",
	"SELECT VALUE <-likes<-person FROM person:a",
	"SELECT VALUE <->likes<->person FROM person:a",
	"SELECT VALUE ->(knows, likes)->person FROM person:a",
	"SELECT VALUE ->likes->person->likes->person FROM person:a",
	"SELECT VALUE ->likes->(person, cached) FROM cached:1",
	"SELECT VALUE ->likes->? FROM person:a",
	"SELECT VALUE ->? FROM person:a",
	"SELECT VALUE ->knows[WHERE since > 2015] FROM person:a",
	"SELECT VALUE ->(likes WHERE out = person:c)->person FROM person:a",
	"SELECT VALUE ->knows[WHERE twice > 4030]->person FROM person:a",
	"SELECT VALUE ->knows[WHERE secret = 's1'] FROM person:a",
	"SELECT VALUE ->knows[WHERE out = person:b] FROM person:a",
	"SELECT VALUE ->knows[WHERE id = knows:k2 OR in = person:x] FROM person:a",
	"SELECT VALUE ->knows[WHERE out.n > $parent.n] FROM person",
	"SELECT VALUE ->likes->person[WHERE name != 'c'] FROM person:a",
	"SELECT VALUE ->pal->person FROM person:a",
	"SELECT VALUE <->pal<->person FROM person:a",
	"SELECT VALUE ->pal FROM person",
	"SELECT VALUE pal:[person:a, person:b]->person FROM person:a",
	"SELECT VALUE ->likes->person FROM cached:1",
	"SELECT VALUE ->likes FROM cached:1",
	"SELECT VALUE <~post FROM person:a",
	"SELECT VALUE <~(post FIELD editor) FROM person:a",
	"SELECT VALUE <~(post FIELD author, post FIELD editor) FROM person:a",
	"SELECT VALUE <~post[WHERE editor = NONE] FROM person:a",
	"SELECT VALUE <~? FROM person:a",
	"SELECT VALUE ->likes->person<~post FROM person:b",
	"SELECT VALUE [person:a, 'x', person:b, [person:c]]->likes->person FROM person:a",
	"SELECT VALUE { id: person:a }->likes FROM person:b",
	"SELECT VALUE (5)->likes FROM person:b",
	"SELECT id, ->likes->person AS out, <-likes<-person AS in FROM person",
	"SELECT VALUE ->likes->person.name FROM person:a",
	"SELECT VALUE ->knows[WHERE since > 2015].out.name FROM person:a",
	"SELECT VALUE ->likes->person[WHERE n > $parent.n] FROM person",
	"SELECT VALUE ->likes->person->likes->person.name FROM person",
	"SELECT VALUE <-likes<-person.n FROM [person:a, person:b]",
	"SELECT VALUE ->knows.* FROM person:a",
	"SELECT VALUE ->knows.*.secret FROM person:a",
	"SELECT VALUE ->knows.twice FROM person:a",
	"SELECT VALUE (->likes->person)[0] FROM person:a",
	"SELECT VALUE ->likes->person.{ name, n } FROM person:a",
	"SELECT VALUE [1, 2].map(|$t| $t + n) FROM person",
	"SELECT VALUE [1, 2].filter(|$t| $t > n).len() FROM person",
	"SELECT VALUE { f: |$x| $x > n } FROM person",
	"SELECT VALUE (|$x| $x + n)(1) FROM person",
];

#[tokio::test]
async fn graph_lookups_answer_their_values() {
	let mut golden = Golden::new("graph_lookups_answer_their_values");
	let db = TestDb::new_with_auth(SETUP).await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in LOOKUPS {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
			let physical = super::optimize::optimize(
				super::map::map(&planner(&ctx), &bound).await.expect("maps"),
			);
			let program = super::lower::lower(&planner(&ctx), &physical).await.expect("lowers");
			let code = || program.pipelines.iter().chain(program.host_code.iter());
			let looked_up =
				code().any(|p| p.code.iter().any(|i| matches!(i, Instr::Lookup { .. })));
			let lookup = ["->", "<-", "<~"].iter().any(|arrow| sql.contains(arrow));
			assert!(looked_up || !lookup, "`{sql}` for the {who} takes no lookup instruction");
		}
	}
	golden.check();
}

/// A table whose permission reads `$parent`, bound to the row the lookup is
/// taken over, and a `WHERE` that raises.
#[tokio::test]
async fn graph_lookup_edge_cases_answer_their_values() {
	let mut golden = Golden::new("graph_lookup_edge_cases_answer_their_values");
	let db = TestDb::new_with_auth(
		"
		DEFINE TABLE v PERMISSIONS FOR select WHERE $parent.open = true;
		DEFINE TABLE e TYPE RELATION PERMISSIONS FULL;
		CREATE v:1 SET open = true; CREATE v:2;
		RELATE v:1->e:1->v:2;
		RELATE v:2->e:2->v:1;
	",
	)
	.await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in [
			"SELECT VALUE ->e->v FROM [{ id: v:1, open: true }, { id: v:2 }]",
			"SELECT VALUE ->e->v FROM v",
			"RETURN v:1->e->v",
			"SELECT VALUE [->e->v, ->e[WHERE $parent.open]->v] FROM [{ id: v:1, open: true }]",
			"SELECT VALUE ->e[WHERE THROW 'no'] FROM [v:1]",
			"SELECT VALUE ->e[WHERE <int> 'x' > 1] FROM [v:1]",
			"SELECT VALUE ->e[WHERE (SELECT VALUE open FROM ONLY $parent.id)] FROM [v:1, v:2]",
		] {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}`"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}

/// Run each of `statements` on a fresh database from `setup` as `session`,
/// and answer every result.
fn run_as(
	setup: &str,
	statements: &[&str],
	session: &crate::dbs::Session,
) -> Vec<Result<crate::val::Value, String>> {
	let runtime =
		tokio::runtime::Builder::new_current_thread().enable_all().build().expect("a runtime");
	runtime.block_on(async {
		let db = TestDb::new_with_auth(setup).await;
		let session = session.clone();
		let mut out = Vec::new();
		for sql in statements {
			let results = match db.datastore().execute(sql, &session, None).await {
				Ok(results) => results,
				Err(e) => {
					out.push(Err(e.to_string()));
					continue;
				}
			};
			for result in results {
				out.push(result.result.map(crate::val::Value::from).map_err(|e| e.to_string()));
			}
		}
		out
	})
}

/// The results of a run, one per line.
fn render_results(results: &[Result<crate::val::Value, String>]) -> String {
	results
		.iter()
		.map(|result| match result {
			Ok(value) => value.to_sql().replace('\n', "\\n"),
			Err(message) => format!("error: {}", message.replace('\n', "\\n")),
		})
		.collect::<Vec<_>>()
		.join("\n")
}

/// Closure literals, the values they make and their invocation answer their
/// values: captures of block names, parameters and the row, arguments coerced
/// and missing, `RETURN` in a body, recursion to the depth limit, a closure
/// escaping to a later statement, the builtins and methods that take one, and
/// the errors each raises.
#[test]
fn graph_closures_answer_their_values() {
	let mut golden = Golden::new("graph_closures_answer_their_values");
	let setup = "
		DEFINE TABLE person PERMISSIONS FOR select WHERE hidden != true;
		CREATE person:a SET name = 'a', n = 1, tags = [1, 2, 3];
		CREATE person:b SET name = 'b', n = 5, tags = [4];
		CREATE person:h SET name = 'h', hidden = true, tags = [];
		DEFINE PARAM $limit VALUE 2 PERMISSIONS FULL;
	";
	let statements = [
		"LET $f = |$x: int| $x * 2; RETURN $f(21);",
		"LET $k = 10; LET $f = |$x| $x + $k; LET $k = 99; RETURN [$f(1), $k];",
		"LET $f = |$x: int| $x; RETURN $f('nope');",
		"LET $f = |$a, $b: int| $b; RETURN $f(1);",
		"LET $f = |$a, $b: option<int>| $b; RETURN $f(1);",
		"LET $f = |$x| { IF $x > 1 { RETURN 'big' }; 'small' }; RETURN [$f(1), $f(5)];",
		"LET $f = |$x| -> int { $x }; RETURN $f('a');",
		"LET $s = 3; RETURN 3($s);",
		"RETURN (|$x| $x + 1)(1);",
		"LET $fact = |$f, $n| IF $n <= 1 { 1 } ELSE { $n * $f($f, $n - 1) }; RETURN $fact($fact, 10);",
		"LET $s = |$s| $s($s); RETURN $s($s);",
		"LET $g = || $limit; RETURN $g();",
		"LET $f = |$x| $x > 1; RETURN [[1, 2, 3].filter($f), array::filter([1, 2, 3], $f)];",
		"LET $f = |$x| $x; RETURN $f;",
		"LET $f = |$x| $x; LET $g = $f; RETURN $f == $g;",
		"SELECT VALUE tags.map(|$t| $t + n) FROM person",
		"SELECT VALUE tags.filter(|$t| $t > $limit) FROM person",
		"SELECT VALUE tags.all(|$t| $t > 0) FROM person",
		"SELECT VALUE tags.any(|$t| $t = 4) FROM person",
		"SELECT VALUE tags.find(|$t| $t >= 2) FROM person",
		"SELECT VALUE tags.find_index(|$t| $t >= 2) FROM person",
		"SELECT VALUE [tags.map(|$t, $i| $i), name.len()] FROM person",
		"SELECT VALUE (<set> tags).filter(|$t| $t > 1) FROM person",
		"SELECT VALUE { check: |$x| $x > n } FROM person",
		"SELECT VALUE tags.fold(0, |$a, $t| $a + $t) FROM person",
		"SELECT VALUE n.chain(|$v| $v * 3) FROM person",
		"SELECT VALUE (|| name)() FROM person",
		"LET $f = |$x| $x.name; SELECT VALUE $f(id) FROM person",
		"LET $f = |$x| $x.name; RETURN $f(person:h);",
		"RETURN [1, 2].map(|$x| $x.nope.deeper);",
		"RETURN [1, 2].map(|$x| <int> 'x' + $x);",
		"LET $obj = { f: |$x| $x * 3 }; RETURN $obj.f(2);",
	];
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		for sql in statements {
			let results = run_as(setup, &[sql], &session);
			golden.record(format!("{who}: {sql}"), render_results(&results));
		}
	}
	golden.check();
}

/// A table permission whose predicate walks edge tables its caller may not
/// select: the predicate's reads are not checked, so the walk reaches what
/// the permission's walk reaches, for a read and for a write's gates.
#[tokio::test]
async fn a_permission_predicate_walks_edges_its_caller_may_not_select() {
	let mut golden = Golden::new("a_permission_predicate_walks_edges_its_caller_may_not_select");
	const WALKED: &str = "
		DEFINE TABLE user PERMISSIONS FOR select FULL;
		DEFINE TABLE workspace PERMISSIONS FOR select FULL;
		DEFINE TABLE journey PERMISSIONS FOR select FULL;
		DEFINE TABLE task SCHEMAFULL PERMISSIONS
			FOR select WHERE $auth.id IN journey<-member_of_journey<-workspace<-member_of<-user
			FOR create, update WHERE $auth.id IN journey<-member_of_journey<-workspace<-member_of<-user;
		DEFINE FIELD journey ON task TYPE record<journey>;
		DEFINE FIELD n ON task TYPE option<int>;
		DEFINE TABLE member_of_journey TYPE RELATION;
		DEFINE TABLE member_of TYPE RELATION;
		CREATE user:martin, user:other;
		CREATE workspace:w1;
		CREATE journey:j1, journey:j2;
		CREATE task:t1 SET journey = journey:j1;
		CREATE task:t2 SET journey = journey:j2;
		RELATE user:martin->member_of->workspace:w1;
		RELATE workspace:w1->member_of_journey->journey:j1;
	";
	for user in ["martin", "other"] {
		let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", user));
		let session = crate::dbs::Session::for_record("test", "test", "user", rid);
		for sql in [
			"SELECT * FROM task",
			"SELECT VALUE id FROM task WHERE n = NONE",
			"UPDATE task SET n = 1",
			"UPDATE task:t1 SET n = 2 RETURN AFTER",
		] {
			let db = TestDb::new_with_auth(WALKED).await;
			let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Write).await;
			let bound = bind(&ctx, sql).await;
			let op = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for {user}"));
			golden.rows(format!("{user}: {sql}"), &outcome(&op, &ctx).await);
		}
	}
	golden.check();
}

/// A hop's `WHERE` over the fields its edges carry inline is judged from the
/// adjacency entries, comparisons of a field with a constant without running
/// the pipeline, and an entry whose inline section predates the field's
/// `INLINE` over its record; each answers its values.
#[tokio::test]
async fn conditions_over_inline_fields_answer_their_values() {
	let mut golden = Golden::new("conditions_over_inline_fields_answer_their_values");
	let db = TestDb::new_with_auth(
		"
		DEFINE TABLE person PERMISSIONS FULL;
		DEFINE TABLE knows TYPE RELATION PERMISSIONS FULL;
		CREATE |person:1..=12|;
		RELATE person:1->knows:1->person:2 SET score = 1, tag = 'a';
		RELATE person:1->knows:2->person:3 SET score = 'x', tag = 'b';
		DEFINE FIELD score ON knows TYPE any INLINE;
		FOR $n IN 3..=10 {
			RELATE person:1->(type::record('knows', $n))->(type::record('person', $n + 1))
				SET score = $n, tag = IF $n % 2 = 0 { 'a' } ELSE { 'b' };
		};
	",
	)
	.await;
	let rid = crate::types::PublicValue::RecordId(surrealdb_types::RecordId::new("user", "reader"));
	let reader = crate::dbs::Session::for_record("test", "test", "user", rid);
	for (who, session) in [("owner", TestDb::owner()), ("record user", reader)] {
		let ctx = db.exec_ctx_as(&session, crate::kvs::TransactionType::Read).await;
		for sql in [
			"SELECT VALUE ->(knows WHERE score > 5) FROM person:1",
			"SELECT VALUE ->(knows WHERE 5 < score) FROM person:1",
			"SELECT VALUE ->(knows WHERE score > 3 AND score <= 8) FROM person:1",
			"SELECT VALUE ->(knows WHERE score = 'x') FROM person:1",
			"SELECT VALUE ->(knows WHERE score > 3 AND id != knows:6) FROM person:1",
			"SELECT VALUE ->(knows WHERE score > 8 OR score < 4) FROM person:1",
			"SELECT VALUE ->(knows WHERE out = person:5 AND score >= 4) FROM person:1",
			"SELECT VALUE ->(knows WHERE score CONTAINS 1) FROM person:1",
			"SELECT VALUE ->(knows WHERE score > 3)->person FROM person:1",
		] {
			let bound = bind(&ctx, sql).await;
			let vm = super::plan(&planner(&ctx), &bound)
				.await
				.unwrap_or_else(|| panic!("the VM declined `{sql}` for the {who}"));
			golden.rows(format!("{who}: {sql}"), &outcome(&vm, &ctx).await);
		}
	}
	golden.check();
}
