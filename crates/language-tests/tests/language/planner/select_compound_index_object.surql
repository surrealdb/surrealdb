/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:{ id: 3, r: o:1 } }]"

[[test.results]]
value = "[{ id: t:{ id: 3, r: o:1 } }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', operator: '=', value: [o:1, 2025] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', prefix: [o:2], ranges: [{ operator: '>', value: 2022 }] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 4, r: o:2 } }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', prefix: [o:2], ranges: [{ operator: '>=', value: 2025 }, { operator: '>', value: 2020 }] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

[[test.results]]
value = "[{ id: t:{ id: 2, r: o:2 } }]"

[[test.results]]
value = "[{ id: t:{ id: 2, r: o:2 } }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'idx', prefix: [o:2], ranges: [{ operator: '>', value: 2020 }, { operator: '<=', value: 2024 }] }, table: 't' }, operation: 'Iterate Index' }, { detail: { type: 'Memory' }, operation: 'Collector' }]"

*/
BEGIN;
    DEFINE INDEX idx ON t FIELDS id.id().r,b;
    CREATE t:{ id: 1, r: o:1 } SET b=2021;
    CREATE t:{ id: 2, r: o:2 } SET b=2021;
    CREATE t:{ id: 3, r: o:1 } SET b=2025;
    CREATE t:{ id: 4, r: o:2 } SET b=2025;
    CREATE t:{ id: 5, r: o:3 } SET b=2025;
    RETURN "OK";
COMMIT;

SELECT id FROM t WHERE id.id().r=o:1 AND b=2025;
SELECT id FROM t WITH NO INDEX WHERE id.id().r=o:1 AND b=2025;
SELECT id FROM t WHERE id.id().r=o:1 AND b=2025 EXPLAIN;

SELECT id FROM t WHERE id.id().r=o:2 AND b>2022;
SELECT id FROM t WITH NO INDEX WHERE id.id().r=o:2 AND b>2022;
SELECT id FROM t WHERE id.id().r=o:2 AND b>2022 EXPLAIN;

SELECT id FROM t WHERE b>=2025 AND id.id().r=o:2 AND b>2020;
SELECT id FROM t WITH NO INDEX WHERE b>=2025 AND id.id().r=o:2 AND b>2020;
SELECT id FROM t WHERE b>=2025 AND id.id().r=o:2 AND b>2020 EXPLAIN;

SELECT id FROM t WHERE b>2020 AND id.id().r=o:2 AND b<=2024;
SELECT id FROM t WITH NO INDEX WHERE b>=2021 AND id.id().r=o:2 AND b<=2024;
SELECT id FROM t WHERE b>2020 AND id.id().r=o:2 AND b<=2024 EXPLAIN;