/**
[env]
namespace = true
database = true

[test]
requires = ["scripting"]
reason = """
A script's `surrealdb.query` calls run one at a time, each after the one the
script started before it, however many are queued and whichever finishes
first: a query started once the first has finished still waits for those
started before it.
"""

[[test.results]]
value = "['a', 'b', 'c', 'd']"
*/

function() {
	const order = [];
	const a = surrealdb.query("SLEEP 30ms").then(() => order.push('a'));
	const b = surrealdb.query("SLEEP 30ms").then(() => order.push('b'));
	const c = surrealdb.query("SLEEP 30ms").then(() => order.push('c'));
	await a;
	const d = surrealdb.query("RETURN 'd'").then(() => order.push('d'));
	await Promise.all([b, c, d]);
	return order;
};
