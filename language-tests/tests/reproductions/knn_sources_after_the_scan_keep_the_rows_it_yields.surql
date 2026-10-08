/**
[env]
namespace = true
database = true

[test]
reason = """
A KNN predicate over several FROM sources holds for a row whose record a
KNN scan of the statement yielded before the row was read: a record, a
range, a value or a statement read after the scan keeps the rows the scan
yielded and drops the rest, and one read before every scan keeps none.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, g: 'x', id: t:1, v: [1, 2] }]"

[[test.results]]
value = "[{ a: 2, g: 'x', id: t:2, v: [1, 3] }]"

[[test.results]]
value = "[{ a: 2, g: 'y', id: t:3, v: [9, 9] }]"

[[test.results]]
value = "[t:1, t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "[t:1, t:1]"

[[test.results]]
value = "[t:1, t:1]"

[[test.results]]
value = "[t:1, t:2, t:1, t:2]"

[[test.results]]
value = "[t:1, t:2, t:2]"

[[test.results]]
value = "[{ a: 2, g: 'x', id: t:2, v: [1, 3] }, { a: 2, g: 'y', id: t:3, v: [9, 9] }, { a: 2, g: 'x', id: t:2, v: [1, 3] }, { a: 2, g: 'y', id: t:3, v: [9, 9] }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 1, id: t:1 }, { a: 2, id: t:2 }, { a: 2, id: t:2 }]"

[[test.results]]
value = "[[t:1, t:1], [t:1], [t:1]]"

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[t:1, t:2, t:1]"
*/
DEFINE INDEX hv ON t FIELDS v HNSW DIMENSION 2;
CREATE t:1 SET v = [1, 2], a = 1, g = 'x'; CREATE t:2 SET v = [1, 3], a = 2, g = 'x'; CREATE t:3 SET v = [9, 9], a = 2, g = 'y';
SELECT VALUE id FROM t, t:1 WHERE v <|1,40|> [1, 2];
SELECT VALUE id FROM t, t:2 WHERE v <|1,40|> [1, 2];
SELECT VALUE id FROM t:1, t WHERE v <|1,40|> [1, 2];
SELECT VALUE id FROM t, [t:1, t:2] WHERE v <|1,40|> [1, 2];
SELECT VALUE id FROM t, [{ id: t:1, v: [1, 2] }, 5, 'x', t:9] WHERE v <|1,40|> [1, 2];
SELECT VALUE id FROM t, t:1..=3 WHERE v <|2,40|> [1, 2];
SELECT VALUE id FROM t, (SELECT * FROM t:2) WHERE v <|2,40|> [1, 2];
SELECT * FROM t, [t:2, t:3] WHERE v <|2,40|> [1, 2] AND a = 2;
SELECT id, a FROM t, t:1..=3 WHERE v <|2,40|> [1, 2] ORDER BY id;
SELECT VALUE (SELECT VALUE id FROM t, $parent.id WHERE v <|1,40|> [1, 2]) FROM t;
SELECT count() FROM t, t:1 WHERE v <|2,40|> [1, 2] GROUP ALL;
SELECT VALUE id FROM t, t:1, t:2 WHERE v <|2,40|> [1, 2] LIMIT 3;
