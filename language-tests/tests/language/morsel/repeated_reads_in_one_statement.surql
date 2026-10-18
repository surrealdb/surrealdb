/**
[env]
namespace = true
database = true

[test]
reason = "two reads of the same stored value in one statement are one value only while no write of the statement can reach them: a write, a function or closure that writes, or a subquery that writes between them separates the two answers, while the clock and entropy reads between them do not"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[10, 10]"

[[test.results]]
value = "[[10], [10]]"

[[test.results]]
value = "[10, true, 10, false, 10]"

[[test.results]]
value = "[10, [11], 11]"

[[test.results]]
value = "[[11], [12], [12]]"

[[test.results]]
value = "[12, 13, 13]"

[[test.results]]
value = "[[13], 14, [14]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[14, 19, 19]"

[[test.results]]
value = "[[19], 24, [24]]"

[[test.results]]
value = "[{ a: 24, b: 24, n: 0 }, { a: 24, b: 24, n: 1 }, { a: 24, b: 24, n: 2 }, { a: 24, b: 24, n: 3 }]"

[[test.results]]
value = "[{ a: 24, b: 25, bumped: 25, n: 0 }, { a: 25, b: 26, bumped: 26, n: 1 }, { a: 26, b: 27, bumped: 27, n: 2 }, { a: 27, b: 28, bumped: 28, n: 3 }]"

[[test.results]]
value = "[{ a: [28], b: [29], bumped: 29, n: 0 }, { a: [29], b: [30], bumped: 30, n: 1 }, { a: [30], b: [31], bumped: 31, n: 2 }, { a: [31], b: [32], bumped: 32, n: 3 }]"

[[test.results]]
value = "[{ a: 32, b: 33, bumped: 33, n: 0 }, { a: 33, b: 34, bumped: 34, n: 1 }, { a: 34, b: 35, bumped: 35, n: 2 }, { a: 35, b: 36, bumped: 36, n: 3 }]"

[[test.results]]
value = "[{ a: 36, b: 36, clock: true, n: 0 }, { a: 36, b: 36, clock: true, n: 1 }, { a: 36, b: 36, clock: true, n: 2 }, { a: 36, b: 36, clock: true, n: 3 }]"

[[test.results]]
value = "[{ n: 0, shown: 40 }, { n: 1, shown: 40 }, { n: 2, shown: 40 }, { n: 3, shown: 40 }]"

[[test.results]]
value = "[{ after: 41, before: 40, bumped: 41, n: 0 }, { after: 42, before: 41, bumped: 42, n: 1 }, { after: 43, before: 42, bumped: 43, n: 2 }, { after: 44, before: 43, bumped: 44, n: 3 }]"

[[test.results]]
value = "[{ after: 44, before: 44, n: 0 }, { after: 44, before: 44, n: 1 }, { after: 44, before: 44, n: 2 }, { after: 44, before: 44, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 44, b: 45, id: seen:0, w: 45 }, { a: 45, b: 46, id: seen:1, w: 46 }, { a: 46, b: 47, id: seen:2, w: 47 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 47, b: 47, id: seen:0, w: 45 }, { a: 47, b: 47, id: seen:1, w: 46 }, { a: 47, b: 47, id: seen:2, w: 47 }]"

[[test.results]]
value = "[47]"
*/

DEFINE TABLE log;
DEFINE TABLE t;
DEFINE TABLE ctr;
DEFINE TABLE seen;
DEFINE FUNCTION fn::bump() { UPSERT ctr:1 SET v = (ctr:1.v ?? 0) + 1 RETURN NONE; RETURN ctr:1.v };
FOR $i IN 0..4 { CREATE type::record('t', $i) SET n = $i RETURN NONE };
UPSERT ctr:1 SET v = 10 RETURN NONE;
-- no write between the reads
RETURN [ctr:1.v, ctr:1.v];
RETURN [(SELECT VALUE v FROM ctr), (SELECT VALUE v FROM ctr)];
-- the clock and entropy between the reads
RETURN [ctr:1.v, type::is_datetime(time::now()), ctr:1.v, rand::uuid() = rand::uuid(), ctr:1.v];
-- a subquery that writes between the reads
RETURN [ctr:1.v, (UPSERT ctr:1 SET v += 1 RETURN VALUE v), ctr:1.v];
RETURN [(SELECT VALUE v FROM ctr), (UPSERT ctr:1 SET v += 1 RETURN VALUE v), (SELECT VALUE v FROM ctr)];
-- a function that writes between the reads
RETURN [ctr:1.v, fn::bump(), ctr:1.v];
RETURN [(SELECT VALUE v FROM ctr), fn::bump(), (SELECT VALUE v FROM ctr)];
-- a closure that writes between the reads
LET $bump = |$by: int| { UPSERT ctr:1 SET v += $by RETURN NONE; ctr:1.v };
RETURN [ctr:1.v, $bump(5), ctr:1.v];
RETURN [(SELECT VALUE v FROM ctr), $bump(5), (SELECT VALUE v FROM ctr)];
-- a read that no row's write can reach, then one a row's write reaches
SELECT n, ctr:1.v AS a, ctr:1.v AS b FROM t ORDER BY n;
SELECT n, ctr:1.v AS a, fn::bump() AS bumped, ctr:1.v AS b FROM t ORDER BY n;
SELECT n, (SELECT VALUE v FROM ctr) AS a, fn::bump() AS bumped, (SELECT VALUE v FROM ctr) AS b FROM t ORDER BY n;
SELECT n, ctr:1.v AS a, $bump(1) AS bumped, ctr:1.v AS b FROM t ORDER BY n;
SELECT n, ctr:1.v AS a, type::is_datetime(time::now()) AS clock, ctr:1.v AS b FROM t ORDER BY n;
-- the same reads in a filter and a projection
SELECT n, ctr:1.v AS shown FROM t WHERE ctr:1.v > 0 AND fn::bump() > 0 ORDER BY n;
-- writes from a statement over rows
UPDATE t SET before = ctr:1.v, bumped = fn::bump(), after = ctr:1.v RETURN n, before, bumped, after;
UPDATE t SET before = ctr:1.v, after = ctr:1.v RETURN n, before, after;
-- a loop that reads, writes through a function, and reads again
FOR $i IN 0..3 { LET $a = ctr:1.v; LET $w = fn::bump(); LET $b = ctr:1.v; CREATE type::record('seen', $i) SET a = $a, w = $w, b = $b RETURN NONE };
SELECT * FROM seen ORDER BY id;
FOR $i IN 0..3 { LET $a = ctr:1.v; LET $b = ctr:1.v; UPSERT type::record('seen', $i) SET a = $a, b = $b RETURN NONE };
SELECT * FROM seen ORDER BY id;
SELECT VALUE v FROM ctr;
