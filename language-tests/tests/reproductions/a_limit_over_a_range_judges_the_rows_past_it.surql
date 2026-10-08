/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A LIMIT over a record range or a union stops reading at its limit, as a
	table scan does, whatever judges its rows - a computed field, a closure, a
	WHERE: a row past the limit is neither read nor judged, so one that would
	fail does not fail the statement, and LIMIT 0 reads nothing. Main reads a
	whole record range, and the first source of a union, regardless.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, c: 3, id: t:2 }]"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: [1], c: [2], id: m:1 }]"

[[test.results]]
value = "[{ a: [1], c: [2], id: m:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 1, id: u:1 }]"















































*/
DEFINE TABLE t;
DEFINE FIELD c ON t COMPUTED a + 1;
CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
SELECT * FROM t:1.. LIMIT 1;
SELECT * FROM t:1..3 LIMIT 0;
SELECT VALUE c FROM t:1..=2 LIMIT 1 START 1;
DEFINE TABLE m;
DEFINE FIELD c ON m COMPUTED a.map(|$x| $x + 1);
CREATE m:1 SET a = [1];
SELECT * FROM m:1.. LIMIT 1;
DEFINE TABLE b;
DEFINE FIELD bad ON b COMPUTED <int> 'x';
CREATE b:1 SET a = 1 RETURN NONE;
CREATE b:2 SET a = 1 RETURN NONE;
DEFINE TABLE u;
CREATE u:1 SET a = 1 RETURN NONE;
SELECT * FROM b:1..3 LIMIT 0;
SELECT * FROM b:1..3 LIMIT 1;
SELECT * FROM u:1, u:2 WHERE <int> 'x' LIMIT 0;
SELECT * FROM b, u LIMIT 1;
SELECT * FROM u:1..3 LIMIT 0;
SELECT * FROM u:1, t:1 LIMIT 1;
