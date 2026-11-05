/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A write to a view's source table evaluates the view's clauses with the DEFINE PARAM value current at the write, so DEFINE PARAM OVERWRITE reaches every later write"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:1, x: 5 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: src:2, x: 5 }]"

[[test.results]]
value = "[{ id: src:3, x: 20 }]"

[[test.results]]
value = "[{ id: src:1, x: 30 }]"

[[test.results]]
value = "[{ id: v:1, pv: 10, x: 30 }, { id: v:3, pv: 10, x: 20 }]"
*/

DEFINE PARAM $p VALUE 1;
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE v AS SELECT id, x, $p AS pv FROM src WHERE x > $p;
CREATE src:1 SET x = 5;
DEFINE PARAM OVERWRITE $p VALUE 10;
CREATE src:2 SET x = 5;
CREATE src:3 SET x = 20;
UPDATE src:1 SET x = 30;
SELECT * FROM v ORDER BY id;
