/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 1729
reason = """
	The threshold a top-k sort publishes to the scan beneath it is built for one
	shape: a single sort key over a stored field. With two sort keys a row that
	ties the heap's worst on the first key can still win on the second, so the
	scan never skips a tie; and an ORDER BY over a `COMPUTED` field reads a value
	the stored bytes do not hold, so the pushdown is not planned at all. A
	subquery that runs once per outer row answers every row on both shapes, the
	same rows that the single-key form must return.
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
value = "[0, 5, 10, 15, 20, 25, 30, 35]"

[[test.results]]
value = "[4, 9, 14, 19, 24, 29, 34, 39]"

[[test.results]]
value = "[0, 5, 10, 15, 20, 25, 30, 35]"

[[test.results]]
value = "[4, 9, 14, 19, 24, 29, 34, 39]"
*/

DEFINE TABLE pl PERMISSIONS FULL;
FOR $n IN 0..40 { CREATE type::record('pl', $n) SET n = $n, k = $n % 5; };
DEFINE TABLE pc PERMISSIONS FULL;
DEFINE FIELD m ON pc COMPUTED n;
FOR $n IN 0..40 { CREATE type::record('pc', $n) SET n = $n, k = $n % 5; };

-- Two sort keys.
SELECT VALUE n FROM pl WHERE k = (SELECT VALUE k FROM pl ORDER BY n, k LIMIT 1)[0];
SELECT VALUE n FROM pl WHERE k = (SELECT VALUE k FROM pl ORDER BY n DESC, k LIMIT 1)[0];

-- A computed sort key.
SELECT VALUE n FROM pc WHERE k = (SELECT VALUE k FROM pc ORDER BY m LIMIT 1)[0];
SELECT VALUE n FROM pc WHERE k = (SELECT VALUE k FROM pc ORDER BY m DESC LIMIT 1)[0];
