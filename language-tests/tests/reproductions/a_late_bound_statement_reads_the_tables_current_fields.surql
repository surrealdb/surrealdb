/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A statement planned after a call whose body changes the catalog is
	late-bound: it loads its table's fields when it opens its source, so it
	derives a computed field the call defined, applies a field's new VALUE,
	ASSERT and DEFAULT, and stops deriving a field the call removed.
"""

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, c: 2, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, b: 7, c: 3, id: t:1 }]"

[[test.results]]
error = "Found 3 for field `b`, with record `t:1`, but field must conform to: $value > 100"

[[test.results]]
value = "[{ a: 2, b: 7, id: t:1 }]"

[[test.results]]
value = "[{ b: 7, d: 'x', id: t:2 }]"

*/

DEFINE TABLE t;
CREATE t:1 SET a = 1;
DEFINE FUNCTION fn::c() { DEFINE FIELD c ON t COMPUTED a + 1; RETURN 0; };
DEFINE FUNCTION fn::b_value() { DEFINE FIELD b ON t VALUE 7; RETURN 0; };
DEFINE FUNCTION fn::b_assert() { DEFINE FIELD OVERWRITE b ON t ASSERT $value > 100; RETURN 0; };
DEFINE FUNCTION fn::no_c() { REMOVE FIELD c ON t; RETURN 0; };
DEFINE FUNCTION fn::d() { DEFINE FIELD d ON t DEFAULT 'x'; RETURN 0; };
{ LET $x = fn::c(); RETURN SELECT * FROM t; };
{ LET $x = fn::b_value(); RETURN UPDATE t:1 SET a = 2; };
{ LET $x = fn::b_assert(); RETURN UPDATE t:1 SET b = 3; };
{ LET $x = fn::no_c(); RETURN SELECT * FROM t; };
{ LET $x = fn::d(); RETURN CREATE t:2 SET b = 200; };
