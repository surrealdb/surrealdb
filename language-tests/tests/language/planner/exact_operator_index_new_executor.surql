/**
[test]
reason = "Test exact equality operator (==) uses index same as (=) (new executor)"

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
value = ''''Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: b == true] {rows: 1}
       └────> IndexScan [ctx: Db] [index: idx, access: = true, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ b: false, i: 2, id: t:2 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: i == 2] {rows: 1}
       └────> IndexScan [ctx: Db] [index: uniq, access: = 2, direction: Forward] {rows: 1}

Total rows: 1''''
*/

DEFINE INDEX idx ON TABLE t COLUMNS b;
DEFINE INDEX uniq ON TABLE t COLUMNS i;
CREATE t:1 set b = true, i = 1;
CREATE t:2 set b = false, i = 2;
SELECT * FROM t WHERE b == true;
EXPLAIN ANALYZE SELECT * FROM t WHERE b == true;
SELECT * FROM t WHERE i == 2;
EXPLAIN ANALYZE SELECT * FROM t WHERE i == 2;
