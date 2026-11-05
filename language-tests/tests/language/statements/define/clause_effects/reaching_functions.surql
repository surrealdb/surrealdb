/**
[env]
namespace = true
database = true

[test]
reason = """
	What a clause reaches through stored functions is read from their bodies
	transitively: a recursive function that only reads is allowed, and a write
	two calls deep is refused naming both functions. A function the catalog
	does not define, called directly or reached through another, is not judged:
	the clause is accepted where it is defined, and reading the field fails as
	calling an undefined function does until the function is defined.
"""

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The COMPUTED clause must not have effects: it calls `fn::outer`, which reaches `fn::deep`, in whose body `CREATE` writes data"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Function 'fn::later' not found: The function 'fn::later' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 24, id: t:1, later: 1, via: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::fact($n: int) { RETURN IF $n <= 1 { 1 } ELSE { $n * fn::fact($n - 1) }; };
DEFINE FUNCTION fn::deep() { CREATE log; RETURN 1; };
DEFINE FUNCTION fn::middle() { RETURN fn::deep(); };
DEFINE FUNCTION fn::outer() { RETURN fn::middle(); };
DEFINE FUNCTION fn::calls_later() { RETURN fn::later(); };
DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select WHERE fn::fact(3) = 6;
DEFINE FIELD c ON t COMPUTED fn::fact(4);
DEFINE FIELD w ON t COMPUTED fn::outer();
DEFINE FIELD later ON t COMPUTED fn::later();
DEFINE FIELD via ON t COMPUTED fn::calls_later();
CREATE t:1 RETURN NONE;
SELECT * FROM t;
DEFINE FUNCTION fn::later() { RETURN 1; };
SELECT * FROM t;
SELECT count() FROM log GROUP ALL;
