/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A read whose WHERE compares an indexed field with `=` to a parameter the plan
holds only once the statement runs -- a LET of a block computed at run time,
a FOR variable, a function or closure argument -- reads through the field's
index: an IndexLookup that evaluates the parameter when it opens. A top-level
LET and a literal LET are substituted before planning and keep their
IndexScan.

The lookup is a superset of the rows: the whole predicate stays above it. A
value the index does not list every equal record under (a float, a decimal,
an array) reads the table whole, so every value finds the records a forced
table scan finds. A WITH hint is the allow-list it is for every read.
"""
issue = 7645

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { access: '= 1', direction: 'Forward', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { access: '= 1', direction: 'Forward', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexScan' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[item:2, item:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[item:2, item:3]"

[[test.results]]
value = "[[item:1], [item:2, item:3], []]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[[1, 2, 3], [1, 2, 3]], [[1, 2, 3], [1, 2, 3]], [[1, 2, 3], [1, 2, 3]], [[4], [4]], [[5], [5]], [[6], [6]], [[7], [7]], [[8], [8]], [[9], [9]], [[1, 2, 3], [1, 2, 3]], [[10], [10]]]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'uniq_k', table: 'uniq' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[[uniq:1], [uniq:2], [uniq:3, uniq:4], []]"

[[test.results]]
value = "[item:3]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $p' }, children: [{ attributes: { access: 'k = $p', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD k ON item TYPE int;
DEFINE INDEX item_k ON item FIELDS k;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;
CREATE item:3 SET k = 2 RETURN NONE;

-- Bound before planning: an IndexScan.
LET $top = math::abs(1);
SELECT VALUE id FROM item WHERE k = $top EXPLAIN;
{ LET $p = 1; RETURN SELECT VALUE id FROM item WHERE k = $p EXPLAIN };

-- Bound only when the statement runs: an IndexLookup keyed by the parameter.
{ LET $p = math::abs(1); RETURN SELECT VALUE id FROM item WHERE k = $p EXPLAIN };
{ LET $p: int = (SELECT VALUE k FROM ONLY item:2); RETURN SELECT VALUE id FROM item WHERE k = $p EXPLAIN };
FOR $p IN [2] { RETURN SELECT VALUE id FROM item WHERE k = $p EXPLAIN };
DEFINE FUNCTION fn::by_k($p: int) { RETURN SELECT VALUE id FROM item WHERE k = $p EXPLAIN; };
fn::by_k(2);
LET $by_k = |$p: int| { RETURN SELECT VALUE id FROM item WHERE k = $p EXPLAIN; };
$by_k(2);

-- The rows each form reads.
{ LET $p = (SELECT VALUE k FROM ONLY item:2); RETURN SELECT VALUE id FROM item WHERE k = $p ORDER BY id };
DEFINE FUNCTION fn::ids($p: int) { RETURN SELECT VALUE id FROM item WHERE k = $p ORDER BY id; };
fn::ids(2);
array::map([1, 2, 3], |$p| (SELECT VALUE id FROM item WHERE k = $p ORDER BY id));

-- Every value finds the records a forced table scan finds.
DEFINE TABLE mixed SCHEMALESS;
DEFINE INDEX mixed_k ON mixed FIELDS k;
INSERT INTO mixed [
	{ id: 1, k: 1 }, { id: 2, k: 1.0 }, { id: 3, k: 1dec }, { id: 4, k: 2.5 },
	{ id: 5, k: '1' }, { id: 6, k: [1, 2] }, { id: 7, k: NONE }, { id: 8, k: NULL },
	{ id: 9, k: [] }, { id: 10, k: d'2026-01-01T00:00:00Z' }
] RETURN NONE;
LET $values = [1, 1.0, 1dec, 2.5, '1', [1, 2], NONE, NULL, [], 1, d'2026-01-01T00:00:00Z'];
array::map($values, |$v| [
	(SELECT VALUE record::id(id) FROM mixed WHERE k = $v ORDER BY id),
	(SELECT VALUE record::id(id) FROM mixed WITH NOINDEX WHERE k = $v ORDER BY id)
]);
array::all(array::map($values, |$v| (SELECT VALUE id FROM mixed WHERE k = $v ORDER BY id) == (SELECT VALUE id FROM mixed WITH NOINDEX WHERE k = $v ORDER BY id)));

-- A unique index.
DEFINE TABLE uniq SCHEMALESS;
DEFINE INDEX uniq_k ON uniq FIELDS k UNIQUE;
INSERT INTO uniq [{ id: 1, k: 1 }, { id: 2, k: 2 }, { id: 3 }, { id: 4 }] RETURN NONE;
FOR $p IN [2] { RETURN SELECT VALUE id FROM uniq WHERE k = $p EXPLAIN };
array::map([1, 2, NONE, 3], |$p| (SELECT VALUE id FROM uniq WHERE k = $p ORDER BY id));

-- The rest of the predicate is judged over every row the lookup yields.
{ LET $p = math::abs(2); RETURN SELECT VALUE id FROM item WHERE k = $p AND id != item:2 };

-- A hint is the allow-list it is for every read.
{ LET $p = math::abs(1); RETURN SELECT VALUE id FROM item WITH NOINDEX WHERE k = $p EXPLAIN };
{ LET $p = math::abs(1); RETURN SELECT VALUE id FROM item WITH INDEX item_k WHERE k = $p EXPLAIN };
