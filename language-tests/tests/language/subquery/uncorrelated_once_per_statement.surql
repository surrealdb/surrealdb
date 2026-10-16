/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A statement in value position that reads nothing of the row it is evaluated for runs once per statement, and answers the values and errors it answers when run for every row"

[[test.results]]
value = "[{ id: t:1, n: 1, s: 'a' }]"

[[test.results]]
value = "[{ id: t:2, n: 5, s: 'b' }]"

[[test.results]]
value = "[{ id: t:3, n: 3, s: 'c' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, m: 5 }, { id: t:2, m: 5 }, { id: t:3, m: 5 }]"

[[test.results]]
value = "[{ id: t:1, m: [5] }, { id: t:2, m: [5] }]"

[[test.results]]
value = "[{ id: t:1, m: [{ count: 3 }] }, { id: t:2, m: [{ count: 3 }] }, { id: t:3, m: [{ count: 3 }] }]"

[[test.results]]
value = "[{ id: t:3, m: 9 }, { id: t:2, m: 9 }]"

[[test.results]]
value = "[{ id: t:1, m: 1 }, { id: t:2, m: 1 }, { id: t:3, m: 1 }]"

[[test.results]]
value = "[{ id: t:2 }]"

[[test.results]]
error = "Could not cast into `int` using input `'a'`"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'a'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: [1], i: 1 }, { c: [2], i: 2 }, { c: [3], i: 3 }]"

[[test.results]]
value = "[[1], [2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[[0], [x:1], [1]]]"

[[test.results]]
value = "[[[1], [x:2], [2]], [[2], [x:2], [2]], [[2], [x:2], [2]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, i: 1 }, { c: 2, i: 2 }, { c: 3, i: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: [1], i: 1 }, { c: [2], i: 2 }, { c: [3], i: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 2 }, { i: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 1, i: 1 }, { c: 2, i: 2 }, { c: 3, i: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 6 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

CREATE t:1 SET n = 1, s = 'a';
CREATE t:2 SET n = 5, s = 'b';
CREATE t:3 SET n = 3, s = 'c';
DEFINE INDEX t_n ON t FIELDS n;
SELECT id, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] AS m FROM t;
SELECT id, (SELECT VALUE math::max(n) FROM t GROUP ALL) AS m FROM t LIMIT 2;
SELECT id, (SELECT count() FROM t GROUP ALL) AS m FROM t;
SELECT id, (SELECT VALUE math::sum(n) FROM t GROUP ALL)[0] AS m FROM t ORDER BY id DESC LIMIT 2;
SELECT id, (SELECT VALUE n FROM t LIMIT 1)[0] AS m FROM t;
SELECT id FROM t WHERE n = (SELECT VALUE math::max(n) FROM t GROUP ALL)[0];
-- A statement that fails raises its error, and one evaluated for no row raises nothing.
SELECT id, (SELECT VALUE <int> s FROM t)[0] AS m FROM t;
SELECT id, (SELECT VALUE <int> s FROM t)[0] AS m FROM t WHERE n > 100;
SELECT id, (SELECT VALUE math::max(n) FROM t GROUP ALL)[0] AS m FROM t WHERE (SELECT VALUE <int> s FROM t)[0] = 1;
-- A value kept for one run of a statement is read afresh by the next run.
FOR $i IN [1, 2, 3] { CREATE w SET i = $i; LET $c = SELECT VALUE (SELECT count() FROM w GROUP ALL)[0].count FROM t LIMIT 1; CREATE log SET i = $i, c = $c; };
SELECT i, c FROM log ORDER BY i;
{ CREATE v:1; LET $a = SELECT VALUE (SELECT count() FROM v GROUP ALL)[0].count FROM t LIMIT 1; CREATE v:2; LET $b = SELECT VALUE (SELECT count() FROM v GROUP ALL)[0].count FROM t LIMIT 1; RETURN [$a, $b]; };
DEFINE TABLE x SCHEMALESS;
DEFINE FUNCTION fn::cnt() { RETURN SELECT VALUE (SELECT count() FROM x GROUP ALL)[0].count FROM t LIMIT 1 };
SELECT VALUE [fn::cnt(), (CREATE x:1).id, fn::cnt()] FROM t:1;
SELECT VALUE [fn::cnt(), (UPSERT x:2).id, fn::cnt()] FROM t ORDER BY id;
-- A statement held by a statement in a loop's body is read afresh at every iteration.
FOR $i IN [1, 2, 3] { CREATE a; FOR $v IN (SELECT VALUE count() FROM a GROUP ALL) { CREATE loga SET i = $i, c = $v }; };
SELECT i, c FROM loga ORDER BY i;
FOR $i IN [1, 2, 3] { CREATE b; LET $c = (SELECT VALUE count() FROM b GROUP ALL); CREATE logb SET i = $i, c = $c; };
SELECT i, c FROM logb ORDER BY i;
FOR $i IN [1, 2, 3] { CREATE c; IF (SELECT VALUE count() FROM c GROUP ALL)[0] >= 2 { CREATE logc SET i = $i } };
SELECT i FROM logc ORDER BY i;
FOR $i IN [1, 2, 3] { CREATE d; CREATE logd SET i = $i, c = (SELECT VALUE count() FROM d GROUP ALL)[0]; };
SELECT i, c FROM logd ORDER BY i;
FOR $i IN [1, 2, 3] { CREATE e; UPSERT loge:1 SET c += (SELECT VALUE count() FROM e GROUP ALL)[0]; };
SELECT c FROM loge;
FOR $i IN [1, 2, 3] { CREATE f; RETURN (SELECT VALUE count() FROM f GROUP ALL)[0]; };
-- A statement that orders its rows at random is evaluated anew by each run of the statement that
-- holds it, and once for every row of a read.
FOR $i IN 0..50 { FOR $v IN (SELECT * FROM <array> 0..10 ORDER BY RAND() LIMIT 1) { CREATE logr SET v = $v } };
RETURN array::len(array::distinct(SELECT VALUE v FROM logr)) > 1;
FOR $i IN 0..50 { CREATE logs SET v = (SELECT * FROM <array> 0..10 ORDER BY RAND() LIMIT 1)[0] };
RETURN array::len(array::distinct(SELECT VALUE v FROM logs)) > 1;
RETURN array::len(array::distinct(SELECT VALUE (SELECT * FROM <array> 0..1000 ORDER BY RAND() LIMIT 1)[0] FROM <array> 0..20)) = 1;
