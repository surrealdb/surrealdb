/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A predicate `id IN <list>` over a table reads the listed records of that table by key, in key order and once each, and returns the rows the table scan filtered by the same predicate returns: repeats, record ids of other tables, values that are not record ids and records that do not exist name no row, the rest of the predicate still judges each record, and the order, limit and start of the statement apply as over a scan. A write keeps its scan, and a list holding a composite key is scanned."
issue = 5220

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:a, n: 100, parity: 0 }]"

[[test.results]]
value = "[{ id: t:b, n: 101, parity: 1 }]"

[[test.results]]
value = "[{ id: t:[1, 2], n: 200, parity: 0 }]"

[[test.results]]
value = "[{ id: other:3, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:2, n: 2, parity: 0 }, { id: t:7, n: 7, parity: 1 }, { id: t:a, n: 100, parity: 0 }, { id: t:b, n: 101, parity: 1 }]"

[[test.results]]
value = """{ attributes: { projections: '*' }, children: [{ attributes: { record_id: '[t:2, t:7, t:99, t:a, t:b]' }, context: 'Db', operator: 'RecordIdScan' }], context: 'Db', operator: 'SelectProject' }"""

[[test.results]]
value = "true"

[[test.results]]
value = "[t:1, t:3]"

[[test.results]]
value = "[t:1, t:3]"

[[test.results]]
value = "[t:4, t:5]"

[[test.results]]
value = "[t:5, t:6]"

[[test.results]]
value = "[9, 5, 3, 1]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[3, 5]"

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[t:1, t:[1, 2]]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'id INSIDE [t:[1, 2], t:1]' }, children: [{ attributes: { direction: 'Forward', table: 't' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[t:8, t:9]"

[[test.results]]
value = "[t:8, t:9]"
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE other SCHEMALESS;
FOR $i IN 1..=9 { CREATE type::record('t', $i) SET n = $i, parity = $i % 2; };
CREATE t:a SET n = 100, parity = 0;
CREATE t:b SET n = 101, parity = 1;
CREATE t:[1, 2] SET n = 200, parity = 0;
CREATE other:3 SET n = 3;

LET $all = SELECT * FROM t WITH NOINDEX;
LET $list = [t:7, t:2, t:7, other:3, 'x', 3, t:99, t:b, t:a];

-- The listed records of the table, in key order, each once.
SELECT * FROM t WHERE id IN $list;
SELECT * FROM t WHERE id IN $list EXPLAIN;
RETURN (SELECT * FROM t WHERE id IN $list) == $all[WHERE id IN $list];

-- The rest of the predicate judges each listed record.
SELECT VALUE id FROM t WHERE id IN [t:1, t:2, t:3, t:4] AND parity = 1;
SELECT VALUE id FROM t WHERE parity = 1 AND id IN [t:1, t:2, t:3, t:4];

-- The list written the other way round, and as a set.
SELECT VALUE id FROM t WHERE [t:5, t:4] CONTAINS id;
SELECT VALUE id FROM t WHERE id IN <set>[t:6, t:5, t:6];

-- Order, limit and start apply to the listed rows as to a scan's.
SELECT VALUE n FROM t WHERE id IN [t:1, t:5, t:3, t:9] ORDER BY n DESC;
SELECT VALUE n FROM t WHERE id IN [t:1, t:5, t:3, t:9] LIMIT 2;
SELECT VALUE n FROM t WHERE id IN [t:1, t:5, t:3, t:9] LIMIT 2 START 1;
SELECT count() FROM t WHERE id IN [t:1, t:5, t:3, t:9, t:404] GROUP ALL;

-- A list naming no record of the table reads no row.
SELECT * FROM t WHERE id IN [other:3, t:404, NONE];

-- A list holding a composite key is scanned, and returns the same rows.
SELECT VALUE id FROM t WHERE id IN [t:[1, 2], t:1];
SELECT VALUE id FROM t WHERE id IN [t:[1, 2], t:1] EXPLAIN;

-- A write reads its subjects by a scan its stages judge.
UPDATE t SET touched = true WHERE id IN [t:8, t:9] RETURN VALUE id;
SELECT VALUE id FROM t WHERE touched = true;
