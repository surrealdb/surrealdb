/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 1729
reason = """
	A subquery in a WHERE runs once per outer row, and each run of its
	`ORDER BY <field> LIMIT n` returns that run's own top n. With a single sort
	key the scan beneath the sort skips rows that tie the heap's worst entry, and
	that is sound only against the heap of the run doing the scanning. A run
	that started from an earlier run's threshold skipped the very rows it had to
	return, so the subquery read as empty and the outer filter admitted nothing
	more. The ascending and descending forms, the `IN` form, a limit above one
	(where the row tied with the worst is the one that is lost) and a subquery
	repeated outside a WHERE all answer every row.
"""

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
value = "[0, 1, 5, 6, 10, 11, 15, 16, 20, 21, 25, 26, 30, 31, 35, 36]"

[[test.results]]
value = "[3, 4, 8, 9, 13, 14, 18, 19, 23, 24, 28, 29, 33, 34, 38, 39]"

[[test.results]]
value = "[35, 30, 25, 20, 15, 10, 5, 0]"

[[test.results]]
value = "[[39, 38, 37], [39, 38, 37], [39, 38, 37]]"
*/

DEFINE TABLE pl PERMISSIONS FULL;
FOR $n IN 0..40 { CREATE type::record('pl', $n) SET n = $n, k = $n % 5; };

-- The first row sorted by n decides which k the outer rows must match.
SELECT VALUE n FROM pl WHERE k = (SELECT VALUE k FROM pl ORDER BY n LIMIT 1)[0];
SELECT VALUE n FROM pl WHERE k = (SELECT VALUE k FROM pl ORDER BY n DESC LIMIT 1)[0];
SELECT VALUE n FROM pl WHERE k IN (SELECT VALUE k FROM pl ORDER BY n LIMIT 1);

-- Two rows: the second run would skip the row that ties the first run's worst.
SELECT VALUE n FROM pl WHERE k IN (SELECT VALUE k FROM pl ORDER BY n LIMIT 2);
SELECT VALUE n FROM pl WHERE k IN (SELECT VALUE k FROM pl ORDER BY n DESC LIMIT 2);

-- The outer rows arrive in the opposite order to the subquery's.
SELECT VALUE n FROM pl WHERE k = (SELECT VALUE k FROM pl ORDER BY n LIMIT 1)[0] ORDER BY n DESC;

-- The same subquery run once per element, outside a WHERE.
SELECT VALUE (SELECT VALUE n FROM pl ORDER BY n DESC LIMIT 3) FROM [1, 2, 3];
