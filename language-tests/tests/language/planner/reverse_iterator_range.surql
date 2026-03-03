/**
[env]
new-planner-strategy = "compute-only"

[test]
reason = "Tests reverse iteration with range queries using ORDER BY DESC"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:[10] }, { id: t:[9] }, { id: t:[8] }]"

[[test.results]]
value = "[{ id: t:[9] }, { id: t:[8] }, { id: t:[7] }]"

[[test.results]]
value = "[{ detail: { direction: 'backward', range: [5]..=[10], table: 't' }, operation: 'Iterate Range' }, { detail: { limit: 3, type: 'MemoryOrderedLimit' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"
*/

{
    FOR $i IN 1..15 { CREATE t:[$i]; };
    RETURN "OK";
};

SELECT * FROM t:[5]..=[10] ORDER BY id DESC LIMIT 3;
SELECT * FROM t:[5]..[10] ORDER BY id DESC LIMIT 3;
SELECT * FROM t:[5]..=[10] ORDER BY id DESC LIMIT 3 EXPLAIN FULL;
