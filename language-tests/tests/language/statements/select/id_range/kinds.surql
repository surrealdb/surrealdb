/**
[env]
namespace = true
database = true

[test]
reason = """
	A table scan under a predicate that bounds `id` reads only the stretch of keys between the
	bounds. Every kind of key a record id holds (numbers, strings, uuids, arrays, objects) sits where
	the comparison of record ids puts it, so a bound at each kind boundary, an empty range and an
	inverted range answer what a scan of the whole table would, and the rows come in the same order
	and direction. A write whose WHERE bounds `id` keeps its subjects.
"""

[[test.results]]
value = "[{ id: t:-3, n: -3 }]"

[[test.results]]
value = "[{ id: t:1, n: 1 }]"

[[test.results]]
value = "[{ id: t:2, n: 2 }]"

[[test.results]]
value = "[{ id: t:5, n: 5 }]"

[[test.results]]
value = "[{ id: t:10, n: 10 }]"

[[test.results]]
value = "[{ id: t:a, n: 'a' }]"

[[test.results]]
value = "[{ id: t:b, n: 'b' }]"

[[test.results]]
value = "[{ id: t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', n: 'uuid' }]"

[[test.results]]
value = "[{ id: t:[1, 2], n: 'array' }]"

[[test.results]]
value = "[{ id: t:{ a: 1 }, n: 'object' }]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:2, t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1]"

[[test.results]]
value = "[t:-3, t:1, t:2]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:2, t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1]"

[[test.results]]
value = "[t:-3, t:1, t:2]"

[[test.results]]
value = "[t:2, t:5]"

[[test.results]]
value = "[t:1, t:2, t:5, t:10]"

[[test.results]]
value = "[t:1, t:2, t:5]"

[[test.results]]
value = "[t:2, t:5, t:10]"

[[test.results]]
value = "[t:2, t:5, t:10]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1, t:2]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:{ a: 1 }, t:[1, 2], t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:b, t:a, t:10, t:5]"

[[test.results]]
value = "[t:a, t:10]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:2, t:5]"

[[test.results]]
value = "[t:2, t:5]"

[[test.results]]
value = "[t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1, t:2, t:5, t:10]"

[[test.results]]
value = "[t:-3, t:1, t:2, t:5, t:10, t:a]"

[[test.results]]
value = "[t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1, t:2, t:5, t:10, t:a, t:b]"

[[test.results]]
value = "[t:b]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:5]"

[[test.results]]
value = "[t:5]"

[[test.results]]
value = "[t:5, t:10]"

[[test.results]]
value = "[t:-3, t:2, t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1, t:2]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:-3, t:1, t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:2, t:5]"

[[test.results]]
value = "[t:2]"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[t:5, t:10, t:a, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"

[[test.results]]
value = "[{ id: t:10, n: 10, touched: true }, { id: t:a, n: 'a', touched: true }]"

[[test.results]]
value = "[t:-3, t:1, t:2, t:5, t:b, t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d', t:[1, 2], t:{ a: 1 }]"
*/

CREATE t:-3 SET n = -3;
CREATE t:1 SET n = 1;
CREATE t:2 SET n = 2;
CREATE t:5 SET n = 5;
CREATE t:10 SET n = 10;
CREATE t:a SET n = 'a';
CREATE t:b SET n = 'b';
CREATE t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d' SET n = 'uuid';
CREATE t:[1, 2] SET n = 'array';
CREATE t:{ a: 1 } SET n = 'object';
-- Each comparison, in both spellings.
SELECT VALUE id FROM t WHERE id > t:2;
SELECT VALUE id FROM t WHERE id >= t:2;
SELECT VALUE id FROM t WHERE id < t:2;
SELECT VALUE id FROM t WHERE id <= t:2;
SELECT VALUE id FROM t WHERE t:2 < id;
SELECT VALUE id FROM t WHERE t:2 <= id;
SELECT VALUE id FROM t WHERE t:2 > id;
SELECT VALUE id FROM t WHERE t:2 >= id;
-- Two-sided.
SELECT VALUE id FROM t WHERE id > t:1 AND id < t:10;
SELECT VALUE id FROM t WHERE id >= t:1 AND id <= t:10;
SELECT VALUE id FROM t WHERE id >= t:1 AND id < t:10;
SELECT VALUE id FROM t WHERE id > t:1 AND id <= t:10;
-- Several bounds on one side keep the tightest.
SELECT VALUE id FROM t WHERE id > t:1 AND id >= t:2 AND id > t:-3 AND id < t:20 AND id <= t:10;
SELECT VALUE id FROM t WHERE id >= t:2 AND id > t:2;
SELECT VALUE id FROM t WHERE id <= t:5 AND id < t:5;
-- Order and paging.
SELECT VALUE id FROM t WHERE id > t:2 ORDER BY id;
SELECT VALUE id FROM t WHERE id > t:2 ORDER BY id DESC;
SELECT VALUE id FROM t WHERE id < t:b ORDER BY id DESC LIMIT 2;
SELECT VALUE id FROM t WHERE id > t:-3 LIMIT 2;
SELECT VALUE id FROM t WHERE id > t:-3 START 1 LIMIT 2;
SELECT VALUE id FROM t WHERE id > t:-3 ORDER BY id START 1 LIMIT 2;
-- Bounds at every kind boundary.
SELECT VALUE id FROM t WHERE id > t:10;
SELECT VALUE id FROM t WHERE id >= t:10;
SELECT VALUE id FROM t WHERE id < t:a;
SELECT VALUE id FROM t WHERE id <= t:a;
SELECT VALUE id FROM t WHERE id > t:b;
SELECT VALUE id FROM t WHERE id >= t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d';
SELECT VALUE id FROM t WHERE id < t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d';
SELECT VALUE id FROM t WHERE id > t:a AND id < t:u'7a4b2e1c-3d5f-4a6b-8c9d-0e1f2a3b4c5d';
SELECT VALUE id FROM t WHERE id > t:-3 AND id < t:1;
-- Empty and inverted ranges.
SELECT VALUE id FROM t WHERE id > t:5 AND id < t:2;
SELECT VALUE id FROM t WHERE id > t:5 AND id < t:5;
SELECT VALUE id FROM t WHERE id >= t:5 AND id <= t:5;
SELECT VALUE id FROM t WHERE id > t:5 AND id <= t:5;
SELECT VALUE id FROM t WHERE id > t:10 AND id < t:-3;
-- Bounds beside other conjuncts.
SELECT VALUE id FROM t WHERE id > t:1 AND n = 5;
SELECT VALUE id FROM t WHERE n = 5 AND id > t:1;
SELECT VALUE id FROM t WHERE id > t:1 AND (n = 5 OR n = 10);
SELECT VALUE id FROM t WHERE id > t:1 OR n = -3;
SELECT VALUE id FROM t WHERE NOT (id > t:2);
-- Twins the bound cannot take: the comparison is not a bare conjunct, or is no comparison.
SELECT VALUE id FROM t WHERE (id > t:2) = true;
SELECT VALUE id FROM t WHERE id > t:2 AND true;
SELECT VALUE id FROM t WHERE id != t:2;
SELECT VALUE id FROM t WHERE id IN [t:2, t:5];
SELECT VALUE id FROM t WHERE id = t:2;
-- The fast shape answers what its twin does.
RETURN (SELECT VALUE id FROM t WHERE id > t:2 AND id <= t:b) = (SELECT VALUE id FROM t WHERE (id > t:2) = true AND (id <= t:b) = true);
RETURN (SELECT VALUE id FROM t WHERE id < t:10 ORDER BY id DESC) = (SELECT VALUE id FROM t WHERE (id < t:10) = true ORDER BY id DESC);
-- An UPDATE or a DELETE whose WHERE bounds id keeps its subjects.
UPDATE t SET touched = true WHERE id > t:2 RETURN VALUE id;
SELECT VALUE id FROM t WHERE touched = true;
DELETE t WHERE id >= t:10 AND id < t:b RETURN BEFORE;
SELECT VALUE id FROM t;
