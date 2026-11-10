/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A read whose WHERE compares an indexed field with `=` to a path of fields into
a parameter the plan holds only once the statement runs -- a field of a
function or closure argument, of a row a block LET reads, of a record argument
-- reads through the field's index: an IndexLookup that evaluates the path when
it opens, as it does for the parameter itself.

The lookup is a superset of the rows: the whole predicate stays above it, a
conjunct beside an OR included. A field of a value that may be a record reads
the stored record, so a statement that writes keeps its scan: the record may
change between the lookup and a row. Every value, a missing field and every
record finds the records a forced table scan finds.
"""
issue = 7663

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $o.k' }, children: [{ attributes: { access: 'k = $o.k', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $o.k' }, children: [{ attributes: { access: 'k = $o.k', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $row.k' }, children: [{ attributes: { access: 'k = $row.k', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $r.k' }, children: [{ attributes: { access: 'k = $r.k', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $s.k AND ($s.open OR flag = true)' }, children: [{ attributes: { access: 'k = $s.k', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $key.k' }, children: [{ attributes: { access: 'k = $key.k', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { projections: 'id, w' }, children: [{ attributes: { predicate: 'k = $r.k' }, children: [{ attributes: { direction: 'Forward', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "NONE"

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
value = "[item:2, item:3]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"
*/

DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD k ON item TYPE int;
DEFINE FIELD flag ON item TYPE option<bool>;
DEFINE INDEX item_k ON item FIELDS k;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;
CREATE item:3 SET k = 2 RETURN NONE;

-- A path of fields into a parameter bound when the statement runs: an IndexLookup keyed by it.
DEFINE FUNCTION fn::by_object($o: object) { RETURN SELECT VALUE id FROM item WHERE k = $o.k EXPLAIN; };
fn::by_object({ k: 2 });
LET $by_object = |$o| { RETURN SELECT VALUE id FROM item WHERE k = $o.k EXPLAIN; };
$by_object({ k: 2 });
{ LET $row = (SELECT * FROM ONLY item:2); RETURN SELECT VALUE id FROM item WHERE k = $row.k EXPLAIN };
DEFINE FUNCTION fn::by_record($r: record<item>) { RETURN SELECT VALUE id FROM item WHERE k = $r.k EXPLAIN; };
fn::by_record(item:2);
DEFINE FUNCTION fn::beside_or($s: object) { RETURN SELECT VALUE id FROM item WHERE k = $s.k AND ($s.open OR flag = true) EXPLAIN; };
fn::beside_or({ k: 2, open: true });
array::map([{ k: 2 }], |$key| (SELECT VALUE id FROM item WHERE k = $key.k EXPLAIN));

-- A statement that writes keeps the scan for a path that reads a stored record.
DEFINE FUNCTION fn::by_record_writing($r: record<item>) { RETURN SELECT id, (CREATE side RETURN NONE) AS w FROM item WHERE k = $r.k EXPLAIN; };
fn::by_record_writing(item:2);

-- The rows each form reads.
DEFINE FUNCTION fn::ids_object($o: object) { RETURN SELECT VALUE id FROM item WHERE k = $o.k ORDER BY id; };
fn::ids_object({ k: 2 });
DEFINE FUNCTION fn::ids_record($r: record<item>) { RETURN SELECT VALUE id FROM item WHERE k = $r.k ORDER BY id; };
fn::ids_record(item:2);
array::map([{ k: 1 }, { k: 2 }, { k: 3 }], |$o| (SELECT VALUE id FROM item WHERE k = $o.k ORDER BY id));
DEFINE FUNCTION fn::ids_beside_or($s: object) { RETURN SELECT VALUE id FROM item WHERE k = $s.k AND ($s.open OR flag = true) ORDER BY id; };
fn::ids_beside_or({ k: 2, open: true });
fn::ids_beside_or({ k: 2, open: false });

-- Every value, a missing field and every record finds the records a forced table scan finds.
DEFINE TABLE mixed SCHEMALESS;
DEFINE INDEX mixed_k ON mixed FIELDS k;
INSERT INTO mixed [
	{ id: 1, k: 1 }, { id: 2, k: 1.0 }, { id: 3, k: 1dec }, { id: 4, k: 2.5 },
	{ id: 5, k: '1' }, { id: 6, k: [1, 2] }, { id: 7, k: NONE }, { id: 8, k: NULL },
	{ id: 9, k: [] }, { id: 10, k: d'2026-01-01T00:00:00Z' }
] RETURN NONE;
LET $values = [1, 1.0, 1dec, 2.5, '1', [1, 2], NONE, NULL, [], d'2026-01-01T00:00:00Z'];
array::all(array::map($values, |$v| {
	LET $o = { k: $v };
	RETURN (SELECT VALUE id FROM mixed WHERE k = $o.k ORDER BY id) == (SELECT VALUE id FROM mixed WITH NOINDEX WHERE k = $o.k ORDER BY id);
}));
array::all(array::map([{}, { k: NONE }, { k: NULL }], |$o| (SELECT VALUE id FROM mixed WHERE k = $o.k ORDER BY id) == (SELECT VALUE id FROM mixed WITH NOINDEX WHERE k = $o.k ORDER BY id)));
array::all(array::map((SELECT VALUE id FROM mixed), |$r| (SELECT VALUE id FROM mixed WHERE k = $r.k ORDER BY id) == (SELECT VALUE id FROM mixed WITH NOINDEX WHERE k = $r.k ORDER BY id)));
