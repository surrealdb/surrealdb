/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Test NOINDEX hint and unsupported unary operator fallback in planner"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table' }, { detail: { reason: 'WITH NOINDEX' }, operation: 'Fallback' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'table' }, operation: 'Iterate Table' }, { detail: { reason: 'Unsupported expression: !param.subparam' }, operation: 'Fallback' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"
*/

DEFINE TABLE table;
SELECT * FROM table WITH NOINDEX WHERE !param.subparam EXPLAIN;
SELECT * FROM table WHERE !param.subparam EXPLAIN;
