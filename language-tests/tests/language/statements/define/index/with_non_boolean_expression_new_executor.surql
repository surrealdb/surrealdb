/**
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
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: v > math::max(...)]\n''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: v > 3 - math::max(...)]\n''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: v > 3 - math::max(...) - 1]\n''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: v > 3 - math::max(...) + 1]\n''''

[[test.results]]
value = "[{ id: t:2, v: 2 }]"

[[test.results]]
value = ''''Project [ctx: Db]\n└────> Scan [ctx: Db] [source: t, predicate: v > $p3 - math::max(...) + $p1]\n''''

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
