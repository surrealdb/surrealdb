/**
[env]
namespace = true
database = true

[test]
reason = """
A top-level `LET` evaluates its right-hand side on the session's engine, the
same as a bare statement would, so a closure over a parameter source counts its
array and a subquery projection reads the inner table.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ \"SELECT * FROM b\": [{ id: b:1 }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ \"SELECT * FROM b\": [{ id: b:1 }] }]"
*/

LET $f = |$xs: array| (SELECT count() FROM $xs GROUP ALL);
RETURN $f([1, 2, 3]);
CREATE a:1;
CREATE b:1;
SELECT (SELECT * FROM b) FROM a;
LET $x = SELECT (SELECT * FROM b) FROM a;
RETURN $x;
