/**
[test]
reason = "An aggregated view defined over rows that already exist is filled by a select that returns the aggregate values in a field named `a`. A view grouped by a source column that is itself named `a` is still grouped by that column: it holds the group ['a', true] rather than a group made of the aggregate values, and a later DELETE of the source row finds the group the row leaves."

[[test.results]]
value = "[{ a: 'a', b: true, id: s:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 'a', b: true, c: 1, id: v:['a', true] }]"

[[test.results]]
value = "[{ a: 'a', b: true, id: s:1, n: 1 }]"

[[test.results]]
value = "[]"
*/
CREATE s:1 SET a = 'a', b = true, n = 1;
DEFINE TABLE v AS SELECT a, b, count() AS c FROM s GROUP BY a, b;
SELECT * FROM v;
DELETE s:1 RETURN BEFORE;
SELECT * FROM v;
