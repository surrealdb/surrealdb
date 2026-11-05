/**
[test]
reason = "A view whose clauses read another view of the same table sees what the views maintained before it made of the record."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 'x', id: t:1 }]"

[[test.results]]
value = "[{ g: 'x', id: t:2 }]"

[[test.results]]
value = "[{ g: 'y', id: t:1 }]"

[[test.results]]
value = "[{ g: 'y', id: b_reads:1, seen: [1, 1] }, { g: 'x', id: b_reads:2, seen: [2] }]"

[[test.results]]
value = "[{ c: 1, g: 'x', id: a_counts:['x'] }, { c: 1, g: 'y', id: a_counts:['y'] }]"

*/

DEFINE TABLE t;
DEFINE TABLE a_counts AS SELECT g, count() AS c FROM t GROUP BY g;
DEFINE TABLE b_reads AS SELECT g, (SELECT VALUE c FROM a_counts) AS seen FROM t;
CREATE t:1 SET g = 'x';
CREATE t:2 SET g = 'x';
UPDATE t:1 SET g = 'y';
SELECT * FROM b_reads ORDER BY id;
SELECT * FROM a_counts ORDER BY id;
