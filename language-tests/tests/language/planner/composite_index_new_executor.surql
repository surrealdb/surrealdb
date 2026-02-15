/**
[test]
reason = "Test composite index with partial and full key matching (new executor)"

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
value = ''''Project [ctx: Db] {rows: 2}
└────> Filter [ctx: Db] [predicate: on = true] {rows: 2}
       └────> IndexScan [ctx: Db] [index: t_idx, access: [true], direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: t:1, on: true, value: 1 }, { id: t:3, on: true, value: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: on = false AND `value` = 2] {rows: 1}
       └────> IndexScan [ctx: Db] [index: t_idx, access: [false, 2], direction: Forward] {rows: 1}

Total rows: 1''''

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
value = ''''Project [ctx: Db] {rows: 2}
└────> Filter [ctx: Db] [predicate: on = true] {rows: 2}
       └────> IndexScan [ctx: Db] [index: t_idx, access: [true], direction: Forward] {rows: 2}

Total rows: 2''''

[[test.results]]
value = "[{ id: t:1, on: true, value: 1 }, { id: t:3, on: true, value: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db] {rows: 1}
└────> Filter [ctx: Db] [predicate: on = false AND `value` = 2] {rows: 1}
       └────> IndexScan [ctx: Db] [index: t_idx, access: [false, 2], direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = "[{ id: t:4, on: false, value: 2 }]"
*/

-- Standard composite index
DEFINE INDEX t_idx ON TABLE t COLUMNS on, value;
CREATE t:1 SET on = true, value = 1;
CREATE t:2 SET on = false, value = 1;
CREATE t:3 SET on = true, value = 2;
CREATE t:4 SET on = false, value = 2;
EXPLAIN ANALYZE SELECT * FROM t WHERE on = true;
SELECT * FROM t WHERE on = true;
EXPLAIN ANALYZE SELECT * FROM t WHERE on = false AND value = 2;
SELECT * FROM t WHERE on = false AND value = 2;

-- Unique composite index
REMOVE TABLE t;
DEFINE INDEX t_idx ON TABLE t COLUMNS on, value UNIQUE;
CREATE t:1 SET on = true, value = 1;
CREATE t:2 SET on = false, value = 1;
CREATE t:3 SET on = true, value = 2;
CREATE t:4 SET on = false, value = 2;
EXPLAIN ANALYZE SELECT * FROM t WHERE on = true;
SELECT * FROM t WHERE on = true;
EXPLAIN ANALYZE SELECT * FROM t WHERE on = false AND value = 2;
SELECT * FROM t WHERE on = false AND value = 2;
