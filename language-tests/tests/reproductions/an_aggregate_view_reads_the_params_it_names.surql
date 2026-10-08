/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write to an aggregate view's source table maintains the view when its WHERE clause reads a DEFINE PARAM, evaluating the param as the catalog defines it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, x: 0 }]"

[[test.results]]
value = "[{ id: src:2, x: 5 }]"

[[test.results]]
value = "[{ id: src:3, x: 9 }]"

[[test.results]]
value = "[{ count: 2, id: v:[] }]"
*/

DEFINE PARAM $p VALUE 1;
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE v AS SELECT count() FROM src WHERE x > $p GROUP ALL;
CREATE src:1 SET x = 0;
CREATE src:2 SET x = 5;
CREATE src:3 SET x = 9;
SELECT * FROM v;
