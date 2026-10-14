/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A non-aggregated view (DEFINE TABLE ... AS SELECT ... FROM table) writes its records directly, so indexes defined on the view were only built from the rows that existed at DEFINE INDEX time. Rows created, updated or deleted on the source table afterwards were missing from, or stale in, index-backed queries on the view."
issue = 7377

[[test.results]]
value = "[{ id: parent:1, n: 1 }]"

[[test.results]]
value = "[{ id: parent:2, n: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: parent:3, n: 3 }]"

[[test.results]]
value = "[{ id: child:3, n: 3 }, { id: child:2, n: 2 }, { id: child:1, n: 1 }]"

[[test.results]]
value = "[{ id: child:3, n: 3 }]"

[[test.results]]
value = "[{ id: parent:3, n: 5 }]"

[[test.results]]
value = "[{ id: child:3, n: 5 }, { id: child:2, n: 2 }, { id: child:1, n: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: child:3, n: 5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: child:3, n: 5 }, { id: child:1, n: 1 }]"

[[test.results]]
value = "[]"

*/

CREATE parent:1 SET n = 1;
CREATE parent:2 SET n = 2;
DEFINE TABLE child TYPE NORMAL SCHEMALESS AS SELECT * FROM parent;
DEFINE INDEX child_n ON child FIELDS n;
CREATE parent:3 SET n = 3;
SELECT id, n FROM child ORDER BY n DESC;
SELECT id, n FROM child WHERE n = 3;
UPDATE parent:3 SET n = 5;
SELECT id, n FROM child ORDER BY n DESC;
SELECT id, n FROM child WHERE n = 3;
SELECT id, n FROM child WHERE n = 5;
DELETE parent:2;
SELECT id, n FROM child ORDER BY n DESC;
SELECT id, n FROM child WHERE n = 2;
