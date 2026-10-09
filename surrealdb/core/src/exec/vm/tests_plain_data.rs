//! Every program is instructions over plain data: a statement holding an
//! expression the instructions do not answer is declined, never lowered with
//! that expression left as a tree of nodes.

use super::tests::{SCHEMA, decline_of};
use crate::exec::operators::test_util::TestDb;

/// Statements the VM lowers: operators, literals, parameters, indexes,
/// calls, methods and closures; sources, counts and subqueries; the stages
/// and targets of writes; schema statements.
const SELF_CONTAINED: &[&str] = &[
	"SELECT age + 1 AS next, age ?? 0 AS aged, [age, name] AS row, { a: <string> age } AS obj \
	 FROM person WHERE (age > $limit OR !name) AND age..45 != NONE",
	"SELECT * FROM person LIMIT $limit - 28 START [1, 0][$]",
	"SELECT * FROM person LIMIT [2, 3][$this ?? 0]",
	"SELECT tags[0] AS a, tags[$] AS b, tags[age % 2] AS c FROM person",
	"[1, 2][$limit - 29]",
	"[1, 2][age]",
	"SELECT person:[name, $limit] AS a FROM person WHERE id != person:[age]..",
	"person:[(RETURN 1)]",
	"fn::missing(1)",
	"SELECT IF age > 40 THEN 'old' ELSE name END AS a FROM person",
	"'abc'.slice(age)",
	"1 + 2 * $limit",
	"SELECT string::len(name) AS l FROM person WHERE math::abs(age) > 3",
	"SELECT name.len() AS l, obj.keys() AS k FROM person",
	"SELECT tags.slice(0, age) AS s FROM person",
	"SELECT name.uppercase() AS u FROM person",
	"SELECT name.to_string() AS s FROM person",
	"SELECT fn::inc(age) AS a FROM person",
	"fn::fact(6)",
	"array::map([1, 2, 3], |$v, $i| $v * $i + 1)",
	"array::map([1, 2], |$v| -> int { RETURN $v + 1 })",
	"SELECT array::filter(tags, |$t| $t != 'a') AS f FROM person",
	"SELECT array::map(tags, |$t| name) AS n FROM person",
	"SELECT tags.map(|$t| $t + age) AS n FROM person",
	"{ f: |$v| $v + 1 }",
	"array::map([1], |$v| (SELECT * FROM person))",
	"DEFINE NAMESPACE other",
	"SELECT * FROM person WHERE age > 18",
	"SELECT * FROM computed",
	"SELECT * FROM person:1 LIMIT 1",
	"SELECT count() FROM person GROUP ALL",
	"SELECT count() FROM person:1..=3 GROUP ALL",
	"SELECT (SELECT VALUE age FROM person WHERE age > $parent.age) AS a FROM person",
	"(SELECT * FROM computed).len()",
	"SELECT to.name AS n FROM link WHERE obj.k.v = 1",
	"CREATE person:9 SET name = 'x'",
	"CREATE type::record('person', $limit.nosuch()) SET name = 'x'",
	"CREATE type::record('person', $limit + 1) SET name = 'x'",
	"RELATE (type::record('person', $limit))->knows->([person:2, person:3][0])",
	"INSERT INTO person { id: person:9, name: 'x' }",
	"INSERT INTO person (id, name, a[1 + 1]) VALUES (person:9, 'x', $limit + 1)",
	"UPDATE person SET a = [1, 2], a[$limit + 1] = 3, a[WHERE $this > 1] += 1",
];

#[tokio::test]
async fn a_program_is_instructions_over_plain_data() {
	let db = TestDb::new(SCHEMA).await;
	let ctx = db.exec_ctx_as(&TestDb::owner(), crate::kvs::TransactionType::Write).await;
	let mut declined = Vec::new();
	for sql in SELF_CONTAINED {
		if let Some(reason) = decline_of(&ctx, sql).await {
			declined.push(format!("`{sql}`: {reason}"));
		}
	}
	assert!(declined.is_empty(), "{declined:#?}");
}
