/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A statement in value position kept for one run of a statement is read afresh by every later run: a table view's query, an event body and a field's value each run again for every write they follow"

[[test.results]]
value = "[{ id: other:1, n: 1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, n: 10 }]"

[[test.results]]
value = "[{ m: 1, n: 10 }]"

[[test.results]]
value = "[{ id: other:2, n: 5 }]"

[[test.results]]
value = "[{ id: src:2, n: 20 }]"

[[test.results]]
value = "[{ m: 1, n: 10 }, { m: 5, n: 20 }]"

[[test.results]]
value = "[{ id: src:1, n: 11 }]"

[[test.results]]
value = "[{ m: 5, n: 11 }, { m: 5, n: 20 }]"

[[test.results]]
value = "[{ id: other:3, n: 9 }]"

[[test.results]]
value = "[{ id: src:3, n: 30 }]"

[[test.results]]
value = "[{ m: 5, n: 11 }, { m: 5, n: 20 }, { m: 9, n: 30 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 3, m: 9 }]"

[[test.results]]
value = "[{ id: other:4, n: 12 }]"

[[test.results]]
value = "[{ id: src:4, n: 40 }]"

[[test.results]]
value = "[{ c: 4, m: 12 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:5, n: 50 }]"

[[test.results]]
value = "[{ id: other:5, n: 15 }]"

[[test.results]]
value = "[{ id: src:6, n: 60 }]"

[[test.results]]
value = "[{ m: 12, of: src:5 }, { m: 15, of: src:6 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:7, n: 70, top: 15 }]"

[[test.results]]
value = "[{ id: other:6, n: 18 }]"

[[test.results]]
value = "[{ id: src:8, n: 80, top: 18 }]"

[[test.results]]
value = "[{ id: src:7, top: 15 }, { id: src:8, top: 18 }]"
*/

CREATE other:1 SET n = 1;
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE view AS SELECT id, n, (SELECT VALUE math::max(n) FROM other GROUP ALL)[0] AS m FROM src;
CREATE src:1 SET n = 10;
SELECT n, m FROM view ORDER BY n;
CREATE other:2 SET n = 5;
CREATE src:2 SET n = 20;
SELECT n, m FROM view ORDER BY n;
UPDATE src:1 SET n = 11;
SELECT n, m FROM view ORDER BY n;
CREATE other:3 SET n = 9;
CREATE src:3 SET n = 30;
SELECT n, m FROM view ORDER BY n;
DEFINE TABLE grouped AS SELECT count() AS c, (SELECT VALUE math::max(n) FROM other GROUP ALL)[0] AS m FROM src GROUP ALL;
SELECT c, m FROM grouped;
CREATE other:4 SET n = 12;
CREATE src:4 SET n = 40;
SELECT c, m FROM grouped;
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT ev ON src WHEN $event = "CREATE" THEN (CREATE log SET of = $after.id, m = (SELECT VALUE math::max(n) FROM other GROUP ALL)[0]);
CREATE src:5 SET n = 50;
CREATE other:5 SET n = 15;
CREATE src:6 SET n = 60;
SELECT of, m FROM log ORDER BY of;
DEFINE FIELD top ON src VALUE (SELECT VALUE math::max(n) FROM other GROUP ALL)[0];
CREATE src:7 SET n = 70;
CREATE other:6 SET n = 18;
CREATE src:8 SET n = 80;
SELECT id, top FROM src WHERE n >= 70 ORDER BY id;
