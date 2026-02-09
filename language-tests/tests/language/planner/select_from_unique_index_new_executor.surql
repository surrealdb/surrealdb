/**
[test]

[[test.results]]
value = "'OK'"

[[test.results]]
value = '''"Project [ctx: Db]\n└────> Limit [ctx: Db] [limit: 3]\n       └────> SortByKey [ctx: Db] [sort_keys: time ASC]\n              └────> Scan [ctx: Db] [source: session, predicate: time > d'2000-01-01T00:00:00Z']\n"'''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }]"

[[test.results]]
value = '''"Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: time ASC]\n       └────> Scan [ctx: Db] [source: session, predicate: time > d'2000-01-01T00:00:00Z']\n"'''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:5, time: d'2024-07-01T02:00:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Limit [ctx: Db] [limit: 3]\n       └────> SortByKey [ctx: Db] [sort_keys: time ASC]\n              └────> Scan [ctx: Db] [source: session]\n''''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: time ASC]\n       └────> Scan [ctx: Db] [source: session]\n''''

[[test.results]]
value = "[{ id: session:2, time: d'2024-06-30T23:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:5, time: d'2024-07-01T02:00:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Limit [ctx: Db] [limit: 3]\n       └────> SortByKey [ctx: Db] [sort_keys: time DESC]\n              └────> Scan [ctx: Db] [source: session]\n''''

[[test.results]]
value = "[{ id: session:5, time: d'2024-07-01T02:00:00Z' }, { id: session:1, time: d'2024-07-01T01:00:00Z' }, { id: session:6, time: d'2024-06-30T23:30:00Z' }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> SortByKey [ctx: Db] [sort_keys: time DESC]\n       └────> Scan [ctx: Db] [source: session]\n''''

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

EXPLAIN SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC LIMIT 3;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC LIMIT 3;
EXPLAIN SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC;
SELECT * FROM session WHERE time > d'2000-01-01T00:00:00Z' ORDER BY time ASC;

EXPLAIN SELECT * FROM session ORDER BY time ASC LIMIT 3;
SELECT * FROM session ORDER BY time ASC LIMIT 3;
EXPLAIN SELECT * FROM session ORDER BY time ASC;
SELECT * FROM session ORDER BY time ASC;

EXPLAIN SELECT * FROM session ORDER BY time DESC LIMIT 3;
SELECT * FROM session ORDER BY time DESC LIMIT 3;
EXPLAIN SELECT * FROM session ORDER BY time DESC;
SELECT * FROM session ORDER BY time DESC;
