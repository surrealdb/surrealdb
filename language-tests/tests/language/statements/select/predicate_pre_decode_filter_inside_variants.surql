/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter for ALLINSIDE, ANYINSIDE, NONEINSIDE, NOTINSIDE combinations on arrays. EXPLAIN asserts pre_decode_filter: yes."

[[test.results]]
value = "{ bag: [1, 2, 3], id: pdiv:1 }"

[[test.results]]
value = "{ bag: [10], id: pdiv:2 }"

[[test.results]]
value = "{ bag: [1, 5], id: pdiv:3 }"

[[test.results]]
value = "[{ bag: [1, 2, 3], id: pdiv:1 }]"

[[test.results]]
value = "[{ bag: [1, 2, 3], id: pdiv:1 }, { bag: [1, 5], id: pdiv:3 }]"

[[test.results]]
value = "[{ bag: [1, 2, 3], id: pdiv:1 }, { bag: [10], id: pdiv:2 }, { bag: [1, 5], id: pdiv:3 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pdiv, direction: Forward, predicate: [1, 2] ALLINSIDE bag, pre_decode_filter: yes]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pdiv, direction: Forward, predicate: [1, 99] ANYINSIDE bag, pre_decode_filter: yes]\n''''
*/

CREATE ONLY pdiv:1 SET bag = [1, 2, 3];
CREATE ONLY pdiv:2 SET bag = [10];
CREATE ONLY pdiv:3 SET bag = [1, 5];

SELECT * FROM pdiv WHERE [1, 2] ALLINSIDE bag;
SELECT * FROM pdiv WHERE [1, 99] ANYINSIDE bag;
SELECT * FROM pdiv WHERE [99, 100] NONEINSIDE bag;

EXPLAIN SELECT * FROM pdiv WITH NOINDEX WHERE [1, 2] ALLINSIDE bag;
EXPLAIN SELECT * FROM pdiv WITH NOINDEX WHERE [1, 99] ANYINSIDE bag;
