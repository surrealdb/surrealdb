/**
[test]

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:2, links: [a:3, a:4] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = ''''Project [ctx: Db]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: links CONTAINSANY [a:2]]
''''

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = ''''Project [ctx: Db]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: links CONTAINS a:2]
''''

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = ''''Project [ctx: Db]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: a:2 INSIDE links]
''''

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = ''''Project [ctx: Db]
    TableScan [ctx: Db] [table: t, direction: Forward, predicate: [a:2] ANYINSIDE links]
''''

*/

CREATE t:1 SET links = [a:2, a:1];
CREATE t:2 SET links = [a:3, a:4];

SELECT * FROM t WHERE links CONTAINSANY [a:2];
SELECT * FROM t WHERE links CONTAINS a:2;
SELECT * FROM t WHERE a:2 IN links;
SELECT * FROM t WHERE [a:2] ANYINSIDE links;

DEFINE INDEX idx ON t FIELDS links;

SELECT * FROM t WHERE links CONTAINSANY [a:2];
EXPLAIN SELECT * FROM t WHERE links CONTAINSANY [a:2];
SELECT * FROM t WHERE links CONTAINS a:2;
EXPLAIN SELECT * FROM t WHERE links CONTAINS a:2;
SELECT * FROM t WHERE a:2 IN links;
EXPLAIN SELECT * FROM t WHERE a:2 IN links;
SELECT * FROM t WHERE [a:2] ANYINSIDE links;
EXPLAIN SELECT * FROM t WHERE [a:2] ANYINSIDE links;
