/**
[env]
namespace = true
database = true

[test]
reason = """
A closure created by the streaming engine is a value like any other: it is
equal to itself and to a closure written the same way with the same captures,
so `==`, `array::distinct` and `CONTAINS` treat it as one value; and a closure
invoked from a statement the legacy engine runs (here a `CREATE`) sees the
session of the statement that calls it, so `session::db()` inside the body
names the current database.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "1"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 'test', id: t:1 }]"

[[test.results]]
value = "['test']"

[[test.results]]
value = "'test'"
*/

LET $f = |$x| $x;
RETURN $f == $f;
RETURN array::distinct([$f, $f]).len();
RETURN [$f] CONTAINS $f;
RETURN (|$x| $x) == (|$x| $x);
RETURN (|$x| $x) == (|$y| $y);
LET $g = || session::db();
CREATE t:1 SET d = $g();
SELECT VALUE d FROM t:1;
RETURN $g();
