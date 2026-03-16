/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Tests reverse iteration with a unique index using ORDER BY DESC"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { direction: 'backward', from: { inclusive: false, value: 5 }, index: 'idx', to: { inclusive: false, value: NONE } }, table: 'i' }, operation: 'Iterate Index' }, { detail: { limit: 3, type: 'MemoryOrderedLimit' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3 }, operation: 'StartLimitStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: 'ReverseOrder' }, table: 'i' }, operation: 'Iterate Index' }, { detail: { limit: 3, type: 'MemoryOrderedLimit' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3 }, operation: 'StartLimitStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: 'ReverseOrder' }, table: 'i' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 15 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ v: 15 }, { v: 14 }, { v: 13 }, { v: 12 }, { v: 11 }, { v: 10 }, { v: 9 }, { v: 8 }, { v: 7 }, { v: 6 }, { v: 5 }, { v: 4 }, { v: 3 }, { v: 2 }, { v: 1 }]"
*/

{
    DEFINE INDEX idx ON TABLE i COLUMNS v UNIQUE;
    FOR $i IN 1..=15 { CREATE i:[$i] SET v = $i; };
    RETURN "OK";
};

SELECT v FROM i WHERE v > 5 ORDER BY v DESC LIMIT 3 EXPLAIN FULL;
SELECT v FROM i WHERE v > 5 ORDER BY v DESC LIMIT 3;
SELECT v FROM i ORDER BY v DESC LIMIT 3 EXPLAIN FULL;
SELECT v FROM i ORDER BY v DESC LIMIT 3;
SELECT v FROM i ORDER BY v DESC EXPLAIN FULL;
SELECT v FROM i ORDER BY v DESC;
