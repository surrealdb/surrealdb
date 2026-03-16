/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test exact equality operator (==) uses index same as (=)"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ b: true, i: 1, id: t:1 }]"

[[test.results]]
value = "[{ b: false, i: 2, id: t:2 }]"

[[test.results]]
value = "[{ b: true, i: 1, id: t:1 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: '=', value: true }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ b: false, i: 2, id: t:2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'uniq', operator: '=', value: 2 }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
*/

DEFINE INDEX idx ON TABLE t COLUMNS b;
DEFINE INDEX uniq ON TABLE t COLUMNS i;
CREATE t:1 set b = true, i = 1;
CREATE t:2 set b = false, i = 2;
SELECT * FROM t WHERE b == true;
SELECT * FROM t WHERE b == true EXPLAIN;
SELECT * FROM t WHERE i == 2;
SELECT * FROM t WHERE i == 2 EXPLAIN;
