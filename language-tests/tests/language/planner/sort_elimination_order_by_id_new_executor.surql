/**
[test]
reason = "Sort elimination: ORDER BY id ASC should be eliminated when scanning in forward direction"

[[test.results]]
value = "[{ id: test:1, val: 'c' }]"

[[test.results]]
value = "[{ id: test:2, val: 'a' }]"

[[test.results]]
value = "[{ id: test:3, val: 'b' }]"

[[test.results]]
value = "[{ id: test:1, val: 'c' }, { id: test:2, val: 'a' }, { id: test:3, val: 'b' }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: test, direction: Forward]
''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *] {rows: 3}
    TableScan [ctx: Db] [table: test, direction: Forward] {rows: 3}

Total rows: 3''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *] {rows: 3}
    TableScan [ctx: Db] [table: test, direction: Backward] {rows: 3}

Total rows: 3''''

*/
CREATE test:1 SET val = 'c';
CREATE test:2 SET val = 'a';
CREATE test:3 SET val = 'b';
-- ORDER BY id ASC matches forward table scan, sort should be eliminated
SELECT * FROM test ORDER BY id ASC;
EXPLAIN SELECT * FROM test ORDER BY id ASC;
EXPLAIN ANALYZE SELECT * FROM test ORDER BY id ASC;
EXPLAIN ANALYZE SELECT * FROM test ORDER BY id DESC;
