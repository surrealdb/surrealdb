/**
[env]
planner-strategy = ["all-ro"]

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, v: 1 }]"

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx, access: >1, direction: Forward]
''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx, access: >1, direction: Forward]
''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx, access: >1, direction: Forward]
''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx, access: >1, direction: Forward]
''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]
    IndexScan [ctx: Db] [index: idx, access: >1, direction: Forward]
''''

*/
DEFINE INDEX idx ON t FIELDS v;
CREATE t:1 set v = 1;
CREATE t:2 set v = 2;
LET $p1 = 1;
LET $p3 = 3;
SELECT * FROM t WHERE v > math::max([0, 1]);
EXPLAIN SELECT * FROM t WHERE v > math::max([0, 1]);
SELECT * FROM t WHERE v > 3 - math::max([0, 2]);
EXPLAIN SELECT * FROM t WHERE v > 3 - math::max([0, 2]);
SELECT * FROM t WHERE v > 3 - math::max([0, 1]) - 1;
EXPLAIN SELECT * FROM t WHERE v > 3 - math::max([0, 1]) - 1;
SELECT * FROM t WHERE v > 3 - ( math::max([0, 1]) + 1 );
EXPLAIN SELECT * FROM t WHERE v > 3 - ( math::max([0, 1]) + 1 );
SELECT * FROM t WHERE v > $p3 - ( math::max([0, $p1]) + $p1 );
EXPLAIN SELECT * FROM t WHERE v > $p3 - ( math::max([0, $p1]) + $p1 );
