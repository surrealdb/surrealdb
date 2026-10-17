/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
	A plan that runs more than once answers each run from that run's rows
	alone. A subquery with ORDER BY ... LIMIT runs once per outer row, a FOR
	body once per iteration and a function body once per call; each run returns
	its whole top k. The threshold a top-k sort publishes, which lets the scan
	beneath it skip rows that cannot enter the top k, belongs to one run: a run
	that started from another run's threshold would skip the rows that tie it
	or fall below it, which a correlated subquery needs and an overlapping run
	has not yet seen.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[10, 10, 10]"

[[test.results]]
value = "[[20, 19, 18], [20, 19, 18]]"

[[test.results]]
value = "[[1, 2], [1, 2], [1, 2]]"

[[test.results]]
value = "[{ below: [14, 13], i: 15 }, { below: [9, 8], i: 10 }, { below: [4, 3], i: 5 }]"

[[test.results]]
value = "[[20, 19], [4, 3], [12, 11], [8, 7], [16, 15], [2, 1]]"

[[test.results]]
value = "[10, 10, 10, 10, 10, 10, 10, 10]"

[[test.results]]
value = "[20, 19, 18, 17, 16]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[20, 19, 18], [20, 19, 18], [20, 19, 18]]"

*/

FOR $i IN 1..21 { CREATE type::record('t', $i) SET n = 3, i = $i; };

-- Three outer rows run the subquery one after another.
SELECT VALUE array::len((SELECT VALUE i FROM t WHERE n = 3 ORDER BY i DESC LIMIT 10)) FROM [1, 2, 3];
SELECT VALUE (SELECT VALUE i FROM t WHERE n = 3 ORDER BY i DESC LIMIT 3) FROM [1, 2];
SELECT VALUE (SELECT VALUE i FROM t ORDER BY i LIMIT 2) FROM [1, 2, 3];

-- Correlated: each run's WHERE reads its outer row, so each run's top k is
-- its own. The outer rows arrive highest first, so a later run's rows all fall
-- below an earlier run's threshold.
SELECT i, (SELECT VALUE i FROM t WHERE i < $parent.i ORDER BY i DESC LIMIT 2) AS below FROM t WHERE i IN [5, 10, 15] ORDER BY i DESC;

-- Enough outer rows for the runs to overlap.
SELECT VALUE (SELECT VALUE i FROM t WHERE i <= $parent ORDER BY i DESC LIMIT 2) FROM [20, 4, 12, 8, 16, 2];
SELECT VALUE array::len((SELECT VALUE i FROM t ORDER BY i DESC LIMIT 10)) FROM [1, 2, 3, 4, 5, 6, 7, 8];

-- A FOR body runs its statements once per iteration.
RETURN { FOR $k IN [1, 2, 3] { LET $top = (SELECT VALUE i FROM t ORDER BY i DESC LIMIT 5); IF $k = 3 { RETURN $top; }; }; };

-- A function body runs once per call.
DEFINE FUNCTION fn::top() { RETURN SELECT VALUE i FROM t ORDER BY i DESC LIMIT 3; };
RETURN [fn::top(), fn::top(), fn::top()];
