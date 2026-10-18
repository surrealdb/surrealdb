/**
[env]
timeout = 5000

[test]
reason = "Pins current behaviour, which the issue reports as a defect: a view row takes the id of its source row, so a view over two views derived from the same table holds one row per source id and the later view's row replaces the earlier one's. Not changed here."
issue = 2561

[[test.results]]
value = "[{ id: a:1, x: 1, y: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: b:1, z: 1 }]"

[[test.results]]
value = "[{ id: c:1, z: 2 }]"

[[test.results]]
value = "[{ id: d:1, z: 2 }]"

*/

CREATE a:1 SET x = 1, y = 2;
DEFINE TABLE b AS SELECT x AS z FROM a;
DEFINE TABLE c AS SELECT y AS z FROM a;
DEFINE TABLE d AS SELECT * FROM b, c;
SELECT * FROM b;
SELECT * FROM c;
SELECT * FROM d;
