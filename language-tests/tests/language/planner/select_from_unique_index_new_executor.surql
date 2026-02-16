/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 3}
    IndexScan [ctx: Db] [index: time, access: >d'2000-01-01T00:00:00Z', direction: Forward, limit: 3] {rows: 3}

Total rows: 3"'''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }]"

[[test.results]]
value = '''"Project [ctx: Db] {rows: 4}
    IndexScan [ctx: Db] [index: time, access: >d'2000-01-01T00:00:00Z', direction: Forward] {rows: 4}

Total rows: 4"'''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:5, time: d'2024-07-01T02:00:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3}
    IndexScan [ctx: Db] [index: time, access: , direction: Forward, limit: 3] {rows: 3}

Total rows: 3''''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 4}
    IndexScan [ctx: Db] [index: time, access: , direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:5, time: d'2024-07-01T02:00:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 3}
    Limit [ctx: Db] [limit: 3] {rows: 3}
        SortTopKByKey [ctx: Db] [sort_keys: time DESC, limit: 3] {rows: 3}
            IndexScan [ctx: Db] [index: time, access: , direction: Forward] {rows: 4}

Total rows: 3''''

[[test.results]]
value = "[{ id: session:5, time: d'2024-07-01T02:00:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 4}
    SortByKey [ctx: Db] [sort_keys: time DESC] {rows: 4}
        IndexScan [ctx: Db] [index: time, access: , direction: Forward] {rows: 4}

Total rows: 4''''

[[test.results]]
value = "[{ id: session:5, time: d'2024-07-01T02:00:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:2, time: d'2024-06-30T23:00:00Z' }]"

*/

{
    DEFINE INDEX time ON TABLE session COLUMNS time UNIQUE;
    CREATE session:1 SET time = d'2024-07-01T01:00:00Z';
    CREATE session:2 SET time = d'2024-06-30T23:00:00Z';
    CREATE session:3 SET other = 'test';
    CREATE session:4 SET time = null;
    CREATE session:5 SET time = d'2024-07-01T02:00:00Z';
    CREATE session:6 SET time = d'2024-06-30T23:30:00Z';
    RETURN "OK";
};

EXPLAIN ANALYZE SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC LIMIT 3;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC LIMIT 3;
EXPLAIN ANALYZE SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC;

EXPLAIN ANALYZE SELECT * FROM session ORDER BY time ASC LIMIT 3;
SELECT * FROM session ORDER BY time ASC LIMIT 3;
EXPLAIN ANALYZE SELECT * FROM session ORDER BY time ASC;
SELECT * FROM session ORDER BY time ASC;

EXPLAIN ANALYZE SELECT * FROM session ORDER BY time DESC LIMIT 3;
SELECT * FROM session ORDER BY time DESC LIMIT 3;
EXPLAIN ANALYZE SELECT * FROM session ORDER BY time DESC;
SELECT * FROM session ORDER BY time DESC;
