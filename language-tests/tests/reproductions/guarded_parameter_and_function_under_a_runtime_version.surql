/**
[env]
namespace = true
database = true
auth = { level = "owner" }
backend = ["surrealkv"]
versioned = true

[test]
reason = "A parameter whose definition holds a permission predicate, and a function, read under a VERSION known only when the statement runs, answer at that version"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[6]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with VERSION"

[[test.results]]
value = "[[1]]"

[[test.results]]
error = "The table 't' does not exist"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[11, 12]"

*/
DEFINE TABLE t SCHEMALESS;
DEFINE INDEX ia ON t FIELDS a;
DEFINE FUNCTION fn::f($x: any) { RETURN $x + 1 };
DEFINE PARAM $pp VALUE 5 PERMISSIONS WHERE true;
CREATE t:1 SET a = 1;
SLEEP 50ms;
LET $ts = time::now();
SLEEP 50ms;
CREATE t:2 SET a = 2;
SELECT VALUE id FROM t VERSION $ts;
SELECT VALUE fn::f(a) FROM t VERSION $ts;
SELECT VALUE a + $pp FROM t VERSION $ts;
SELECT VALUE id FROM t WITH INDEX ia WHERE a = 1 VERSION $ts;
SELECT VALUE id FROM t WHERE a = 1 VERSION $ts;
SELECT VALUE id FROM t:1 VERSION $ts FOR UPDATE;
SELECT VALUE (SELECT VALUE a FROM t:1) FROM t VERSION $ts;
SELECT VALUE fn::f(a) FROM t VERSION d'2020-01-01T00:00:00Z';
SELECT VALUE fn::f(a) FROM t ORDER BY id VERSION time::now();
DEFINE FUNCTION OVERWRITE fn::f($x: any) { RETURN $x + 10 };
SELECT VALUE fn::f(a) FROM t VERSION $ts;
SELECT VALUE fn::f(a) FROM t ORDER BY id VERSION time::now();
