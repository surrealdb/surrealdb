/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only"]

[test]
reason = """
A top-level `LET` runs its right-hand side on the engine the session is
pinned to. Under `compute-only` the closure body, and the nested SELECT of a
statement-shaped RHS, evaluate on the legacy engine exactly as a bare
statement would, so a closure over a parameter source counts its array and a
subquery projection reads the inner table.
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
