/**
[env]
namespace = true
database = true

[test]
reason = "a read in a statement that calls a function which writes sees the writes made before it, row by row, and the statement makes each write once, whatever width the scan runs at"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ n: 0, noted: 0, seen: 0 }, { n: 1, noted: 1, seen: 1 }, { n: 2, noted: 2, seen: 2 }, { n: 3, noted: 3, seen: 3 }, { n: 4, noted: 4, seen: 4 }, { n: 5, noted: 5, seen: 5 }]"

[[test.results]]
value = "[6]"

[[test.results]]
value = "[{ n: 0, seen: 12 }, { n: 1, seen: 12 }, { n: 2, seen: 12 }, { n: 3, seen: 12 }, { n: 4, seen: 12 }, { n: 5, seen: 12 }]"

[[test.results]]
value = "[12]"

[[test.results]]
value = "[{ n: 0, noted: 20, seen: 12 }, { n: 1, noted: 21, seen: 13 }, { n: 2, noted: 22, seen: 14 }, { n: 3, noted: 23, seen: 15 }, { n: 4, noted: 24, seen: 16 }, { n: 5, noted: 25, seen: 17 }]"

[[test.results]]
value = "[18]"
*/

DEFINE TABLE log;
DEFINE TABLE t;
DEFINE FUNCTION fn::note($n: int) { CREATE type::record('log', $n) SET n = $n RETURN NONE; RETURN $n };
FOR $i IN 0..6 { CREATE type::record('t', $i) SET n = $i RETURN NONE };
SELECT n, (SELECT VALUE count() FROM log GROUP ALL)[0] ?? 0 AS seen, fn::note(n) AS noted FROM t ORDER BY n;
SELECT VALUE count() FROM log GROUP ALL;
SELECT n, (SELECT VALUE count() FROM log GROUP ALL)[0] ?? 0 AS seen FROM t WHERE fn::note(n + 10) > 0 ORDER BY n;
SELECT VALUE count() FROM log GROUP ALL;
UPDATE t SET seen = (SELECT VALUE count() FROM log GROUP ALL)[0] ?? 0, noted = fn::note(n + 20) RETURN n, seen, noted;
SELECT VALUE count() FROM log GROUP ALL;
