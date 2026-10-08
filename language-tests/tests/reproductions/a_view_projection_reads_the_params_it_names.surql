/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write to a view's source table maintains the view when the view's projection reads a DEFINE PARAM, evaluating the param as the catalog defines it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, x: 3 }]"

[[test.results]]
value = "[{ id: src:2, x: 4 }]"

[[test.results]]
value = "[{ id: v:1, pv: 1, x: 3 }, { id: v:2, pv: 1, x: 4 }]"
*/

DEFINE PARAM $p VALUE 1;
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE v AS SELECT id, x, $p AS pv FROM src;
CREATE src:1 SET x = 3;
CREATE src:2 SET x = 4;
SELECT * FROM v ORDER BY id;
