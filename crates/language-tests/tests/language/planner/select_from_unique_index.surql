/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: d'2000-01-01T00:00:00Z' }, index: 'time', to: { inclusive: false, value: NONE } }, table: 'session' }, operation: 'Iterate Index' }, { detail: { limit: 3, type: 'MemoryOrderedLimit' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3 }, operation: 'StartLimitStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }]"

[[test.results]]
value = "[{ detail: { plan: { direction: 'forward', from: { inclusive: false, value: d'2000-01-01T00:00:00Z' }, index: 'time', to: { inclusive: false, value: NONE } }, table: 'session' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 4 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:5, time: d'2024-07-01T02:00:00Z' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'time', operator: 'Order' }, table: 'session' }, operation: 'Iterate Index' }, { detail: { limit: 3, type: 'MemoryOrderedLimit' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { CancelOnLimit: 3 }, operation: 'StartLimitStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }]"

[[test.results]]
value = "[{ detail: { plan: { index: 'time', operator: 'Order' }, table: 'session' }, operation: 'Iterate Index' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 4 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:5, time: d'2024-07-01T02:00:00Z' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'session' }, operation: 'Iterate Table' }, { detail: { limit: 3, type: 'MemoryOrderedLimit' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 0 }, operation: 'Fetch' }]"

[[test.results]]
value = "[{ id: session:5, time: d'2024-07-01T02:00:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }]"

[[test.results]]
value = "[{ detail: { direction: 'forward', table: 'session' }, operation: 'Iterate Table' }, { detail: { type: 'MemoryOrdered' }, operation: 'Collector' }, { detail: { type: 'KeysAndValues' }, operation: 'RecordStrategy' }, { detail: { count: 6 }, operation: 'Fetch' }]"

[[test.results]]
value = " [{ id: session:5, time: d'2024-07-01T02:00:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:4, time: NULL }, { id: session:3, other: 'test' }]"

*/

BEGIN;
    DEFINE INDEX time ON TABLE session COLUMNS time UNIQUE;
    CREATE session:1 SET time = d'2024-07-01T01:00:00Z';
    CREATE session:2 SET time = d'2024-06-30T23:00:00Z';
    CREATE session:3 SET other = 'test';
    CREATE session:4 SET time = null;
    CREATE session:5 SET time = d'2024-07-01T02:00:00Z';
    CREATE session:6 SET time = d'2024-06-30T23:30:00Z';
    RETURN "OK";
COMMIT;

SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC LIMIT 3 EXPLAIN FULL;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC LIMIT 3;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC EXPLAIN FULL;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC;

SELECT * FROM session ORDER BY time ASC LIMIT 3 EXPLAIN FULL;
SELECT * FROM session ORDER BY time ASC LIMIT 3;
SELECT * FROM session ORDER BY time ASC EXPLAIN FULL;
SELECT * FROM session ORDER BY time ASC;

// With the memory storage engine this is actually uses the table iterator
SELECT * FROM session ORDER BY time DESC LIMIT 3 EXPLAIN FULL;
SELECT * FROM session ORDER BY time DESC LIMIT 3;
SELECT * FROM session ORDER BY time DESC EXPLAIN FULL;
SELECT * FROM session ORDER BY time DESC;
