/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test composite index with partial and full key matching"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, on: true, value: 1 }]"

[[test.results]]
value = "[{ id: t:2, on: false, value: 1 }]"

[[test.results]]
value = "[{ id: t:3, on: true, value: 2 }]"

[[test.results]]
value = "[{ id: t:4, on: false, value: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 't_idx', operator: '=', value: true }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:1, on: true, value: 1 }, { id: t:3, on: true, value: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 't_idx', operator: '=', value: [false, 2] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:4, on: false, value: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, on: true, value: 1 }]"

[[test.results]]
value = "[{ id: t:2, on: false, value: 1 }]"

[[test.results]]
value = "[{ id: t:3, on: true, value: 2 }]"

[[test.results]]
value = "[{ id: t:4, on: false, value: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 't_idx', operator: '=', value: true }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:1, on: true, value: 1 }, { id: t:3, on: true, value: 2 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 't_idx', operator: '=', value: [false, 2] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:4, on: false, value: 2 }]"
*/

-- Standard composite index
DEFINE INDEX t_idx ON TABLE t COLUMNS on, value;
CREATE t:1 SET on = true, value = 1;
CREATE t:2 SET on = false, value = 1;
CREATE t:3 SET on = true, value = 2;
CREATE t:4 SET on = false, value = 2;
SELECT * FROM t WHERE on = true EXPLAIN;
SELECT * FROM t WHERE on = true;
SELECT * FROM t WHERE on = false AND value = 2 EXPLAIN;
SELECT * FROM t WHERE on = false AND value = 2;

-- Unique composite index
REMOVE TABLE t;
DEFINE INDEX t_idx ON TABLE t COLUMNS on, value UNIQUE;
CREATE t:1 SET on = true, value = 1;
CREATE t:2 SET on = false, value = 1;
CREATE t:3 SET on = true, value = 2;
CREATE t:4 SET on = false, value = 2;
SELECT * FROM t WHERE on = true EXPLAIN;
SELECT * FROM t WHERE on = true;
SELECT * FROM t WHERE on = false AND value = 2 EXPLAIN;
SELECT * FROM t WHERE on = false AND value = 2;
