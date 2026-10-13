/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	Common-subexpression elimination shares a value only between expressions
	that compute it. `1`, `1f` and `1dec` compare equal but are three values,
	so the sums below keep their number kinds, wherever an identical sum of the
	same kind is repeated beside them.
"""

[[test.results]]
value = "[2, 2f, 2dec, 2, 2f, 2dec]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[2, 2f, 2dec, 2f, 2]"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[[1, 1f, 1dec, 1f, 1]]"

[[test.results]]
value = "[1]"
*/

RETURN [1 + 1, 1f + 1, 1dec + 1, 1 + 1, 1f + 1, 1dec + 1];

LET $x = 1;
RETURN [$x + 1, $x + 1f, $x + 1dec, $x + 1f, $x + 1];

CREATE t:1 SET n = 1;
SELECT VALUE [n * 1, n * 1f, n * 1dec, n * 1f, n * 1] FROM t;
SELECT VALUE n FROM t WHERE n * 1f = 1 AND n * 1dec = 1 AND n * 1 = 1;
