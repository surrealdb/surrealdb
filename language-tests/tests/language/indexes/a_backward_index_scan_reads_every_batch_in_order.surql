/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 30000

[test]
reason = """
A descending ORDER BY over an indexed column declared a scalar kind reads its
index backward, a batch of entries at a time. The scan honours each bound's
inclusiveness at both ends of the range, reaches every entry across batch
boundaries in order, and answers what a table scan and sort answer, for a
non-unique and a UNIQUE index. Each table holds more than one batch of
entries."""

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
value = "NONE"

[[test.results]]
value = "[1200, 1400, 201, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1200, 1399, 200]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1500, 1499, 0]"

[[test.results]]
value = "[399, 398, 397]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1200, 1400, 201, true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1200, 1399, 200]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1500, 1499, 0]"

[[test.results]]
value = "[399, 398, 397]"

*/

DEFINE TABLE s SCHEMALESS;
DEFINE FIELD n ON s TYPE int;
DEFINE INDEX s_n ON s FIELDS n;
DEFINE TABLE u SCHEMALESS;
DEFINE FIELD n ON u TYPE int;
DEFINE INDEX u_n ON u FIELDS n UNIQUE;
FOR $i IN 0..1500 { CREATE type::record('s', $i) SET n = $i; };
FOR $i IN 0..1500 { CREATE type::record('u', $i) SET n = $i; };

-- non-unique index `s_n`.
-- An exclusive lower and an inclusive upper bound: its count, its first and
-- last entries, and the order a table scan and sort give.
LET $r = SELECT VALUE n FROM s WHERE n > 200 AND n <= 1400 ORDER BY n DESC;
[array::len($r), array::first($r), array::last($r),
	$r == (SELECT VALUE n FROM s WITH NOINDEX WHERE n > 200 AND n <= 1400 ORDER BY n DESC)];
-- An inclusive lower and an exclusive upper bound.
LET $r = SELECT VALUE n FROM s WHERE n >= 200 AND n < 1400 ORDER BY n DESC;
[array::len($r), array::first($r), array::last($r)];
-- The whole index, from its last entry to its first.
LET $r = SELECT VALUE n FROM s ORDER BY n DESC;
[array::len($r), array::first($r), array::last($r)];
-- A page that starts past the first batch the scan reads.
SELECT VALUE n FROM s WHERE n > 100 ORDER BY n DESC LIMIT 3 START 1100;

-- UNIQUE index `u_n`.
-- An exclusive lower and an inclusive upper bound: its count, its first and
-- last entries, and the order a table scan and sort give.
LET $r = SELECT VALUE n FROM u WHERE n > 200 AND n <= 1400 ORDER BY n DESC;
[array::len($r), array::first($r), array::last($r),
	$r == (SELECT VALUE n FROM u WITH NOINDEX WHERE n > 200 AND n <= 1400 ORDER BY n DESC)];
-- An inclusive lower and an exclusive upper bound.
LET $r = SELECT VALUE n FROM u WHERE n >= 200 AND n < 1400 ORDER BY n DESC;
[array::len($r), array::first($r), array::last($r)];
-- The whole index, from its last entry to its first.
LET $r = SELECT VALUE n FROM u ORDER BY n DESC;
[array::len($r), array::first($r), array::last($r)];
-- A page that starts past the first batch the scan reads.
SELECT VALUE n FROM u WHERE n > 100 ORDER BY n DESC LIMIT 3 START 1100;
