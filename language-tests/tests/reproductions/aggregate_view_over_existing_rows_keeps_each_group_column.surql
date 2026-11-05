/**
[test]
reason = "An aggregated view defined over rows that already exist is filled by a select that names each group key g0, g1, … A source field with one of those names stood for the wrong key, so a view grouped by two or more plain fields named g1 and g2 stored every group under its last key and then failed to find the group again when a source row was deleted. The view defined before the rows exist, which is maintained incrementally, was never affected."

[[test.results]]
value = "[{ id: src:1, g1: 'a', g2: true, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, g1: 'a', g2: true, id: v:['a', true] }]"

[[test.results]]
value = "[{ id: src:1, g1: 'a', g2: true, n: 1 }]"

[[test.results]]
value = "[]"

*/
CREATE src:1 SET g1 = 'a', g2 = true, n = 1;
DEFINE TABLE v AS SELECT g1, g2, count() AS c FROM src GROUP BY g1, g2;
SELECT * FROM v;
DELETE src:1 RETURN BEFORE;
SELECT * FROM v;
