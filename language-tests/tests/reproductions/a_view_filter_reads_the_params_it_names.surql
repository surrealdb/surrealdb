/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write to a view's source table maintains the view when the view's WHERE clause reads a DEFINE PARAM, evaluating the param as the catalog defines it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, x: 0 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:2, x: 5 }]"

[[test.results]]
value = "[{ id: src:3, x: 1 }]"

[[test.results]]
value = "[{ id: src:1, x: 7 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: v:1, x: 7 }]"
*/

DEFINE PARAM $p VALUE 1;
DEFINE TABLE src SCHEMALESS;
CREATE src:1 SET x = 0;
DEFINE TABLE v AS SELECT * FROM src WHERE x > $p;
CREATE src:2 SET x = 5;
CREATE src:3 SET x = 1;
UPDATE src:1 SET x = 7;
DELETE src:2;
SELECT * FROM v ORDER BY id;
