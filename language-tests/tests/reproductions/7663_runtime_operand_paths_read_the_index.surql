/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 30000

[test]
reason = """
A read whose WHERE compares an indexed field with `=` to a path of fields into
a parameter the plan holds only once the statement runs -- a field of a
function or closure argument, of a row a block LET reads, of a record argument
-- reads through the field's index: an IndexLookup that evaluates the path when
it opens, as it does for the parameter itself.

Over a composite index the lookup is keyed by every leading column an equality
pins, each with a run-time operand or a literal: the first, a middle, the last
or every column. It is one read where a literal prefix reads every record
under the prefix. A record id written out pins its column as the same record
id held by a parameter would, except one keyed by an array or an object: that
one pins no column, and the lookup keys only the columns before it. A value in
any column that the index may not list every equal record under reads the
whole table.

UPDATE, UPSERT and DELETE read their subjects through the same lookup, and the
write judges each one on its WHERE; an UPSERT whose WHERE admits none creates
its record as it does WITH NOINDEX.

The lookup is a superset of the rows: the whole predicate stays above it, a
conjunct beside an OR included. A field of a value that may be a record reads
the record, and the lookup is guarded by each value a step reads. A statement
that writes may change the record between the lookup and a row: there the
lookup reads the whole table when a guard holds a record id. A read changes no
record, but a computed field is derived on each fetch and may answer each
differently: there the lookup reads the whole table when a guard holds a record
id whose table defines the stepped field as computed, and keeps the index for a
stored field. Every value, a missing field and every record finds the records a
forced table scan finds, and every write leaves the state WITH NOINDEX leaves.
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
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $o.k' }, children: [{ attributes: { access: 'k = $o.k', guard: '$o', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $o.k' }, children: [{ attributes: { access: 'k = $o.k', guard: '$o', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $row.k' }, children: [{ attributes: { access: 'k = $row.k', guard: '$row', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $r.k' }, children: [{ attributes: { access: 'k = $r.k', guard: '$r', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $s.k AND ($s.open OR flag = true)' }, children: [{ attributes: { access: 'k = $s.k', guard: '$s', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $key.k' }, children: [{ attributes: { access: 'k = $key.k', guard: '$key', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { projections: 'id, w' }, children: [{ attributes: { predicate: 'k = $r.k' }, children: [{ attributes: { access: 'k = $r.k', guard: '$r', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'SelectProject' }"

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
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $o.a AND b = 1 AND k = 1' }, children: [{ attributes: { access: 'a = $o.a AND b = 1 AND k = 1', guard: '$o', index: 'pair_abk', table: 'pair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = 1 AND b = $b AND k = 1' }, children: [{ attributes: { access: 'a = 1 AND b = $b AND k = 1', index: 'pair_abk', table: 'pair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = 1 AND b = 1 AND k = $k' }, children: [{ attributes: { access: 'a = 1 AND b = 1 AND k = $k', index: 'pair_abk', table: 'pair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "[{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $o.a AND b = $o.b AND k = $o.k' }, children: [{ attributes: { access: 'a = $o.a AND b = $o.b AND k = $o.k', guard: '$o', index: 'pair_abk', table: 'pair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $o.a AND k = 1' }, children: [{ attributes: { access: 'a = $o.a', guard: '$o', index: 'pair_abk', table: 'pair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'tenant = $tenant AND scope = $key.scope AND generation = $key.generation AND doc = $key.doc AND pack = $pack' }, children: [{ attributes: { access: 'tenant = $tenant AND scope = $key.scope AND generation = $key.generation', guard: '$key', index: 'field_value_by_generation', table: 'field_value' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[pair:2]"

[[test.results]]
value = "[[pair:1, pair:2], [pair:3], []]"

[[test.results]]
value = "[[field_value:1], [field_value:3], []]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

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
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $p', index: 'wi_k', table: 'wi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $o.k', guard: '$o', index: 'wi_k', table: 'wi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $p', index: 'wi_k', table: 'wi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $o.k', guard: '$o', index: 'wi_k', table: 'wi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'a = $o.a AND b = $o.b AND k = 1', guard: '$o', index: 'pair_abk', table: 'pair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $r.k', guard: '$r', index: 'wi_k', table: 'wi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

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
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $o.a' }, children: [{ attributes: { access: 'a = $o.a', guard: '$o', index: 'upair_ab', table: 'upair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $p AND b = $q' }, children: [{ attributes: { access: 'a = $p', index: 'twin_a', table: 'twin' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $o.r.k', guard: '$o, $o.r', index: 'hi_k', table: 'hi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "true"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { limit: '1' }, children: [{ attributes: { keep: '1', sort_keys: 'b ASC' }, children: [{ attributes: { predicate: 'a = 1 AND k = $k' }, children: [{ attributes: { access: 'a = 1 AND k = $k', index: 'opair_ak', table: 'opair' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'Sort' }], context: 'Db', operator: 'Limit' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[opair:2]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPSERT' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $o.k', guard: '$o', index: 'wi_k', table: 'wi' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "[[1, 4], [1f, 4], ['1', 1], [2, 0]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $r.coin' }, children: [{ attributes: { access: 'k = $r.coin', guard: '$r', index: 'vol_k', table: 'vol' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

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
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $p AND b = (pair:1)' }, children: [{ attributes: { access: 'a = $p AND b = pair:1', index: 'ri_ab', table: 'ri' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $p AND b = (pair:[1, 2])' }, children: [{ attributes: { access: 'a = $p', index: 'ri_ab', table: 'ri' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 12]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[true, true, true, true, true, true], [true, true, true, true, true, true], [true, true, true, true, true, true], [true, true, true, true, true, true], [true, true, true, true, true, true], [true, true, true, true, true, true]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'a = $p AND b = pair:1', index: 'ri_ab', table: 'ri' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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

-- In a statement that writes, a path that may read a stored record is guarded: the lookup reads
-- the whole table when the parameter holds a record id.
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

-- A composite index: the lookup is keyed by every leading column an equality pins, a run-time
-- operand or a literal, in the first, a middle, the last or every column.
DEFINE TABLE pair SCHEMAFULL;
DEFINE FIELD a ON pair TYPE int;
DEFINE FIELD b ON pair TYPE int;
DEFINE FIELD k ON pair TYPE int;
DEFINE INDEX pair_abk ON pair FIELDS a, b, k;
INSERT INTO pair [
	{ id: 1, a: 1, b: 1, k: 1 }, { id: 2, a: 1, b: 1, k: 2 }, { id: 3, a: 1, b: 2, k: 1 }, { id: 4, a: 2, b: 1, k: 1 }
] RETURN NONE;
DEFINE FUNCTION fn::pair_first($o: object) { RETURN SELECT VALUE id FROM pair WHERE a = $o.a AND b = 1 AND k = 1 EXPLAIN; };
fn::pair_first({ a: 1 });
DEFINE FUNCTION fn::pair_middle($b: int) { RETURN SELECT VALUE id FROM pair WHERE a = 1 AND b = $b AND k = 1 EXPLAIN; };
fn::pair_middle(1);
DEFINE FUNCTION fn::pair_last($k: int) { RETURN SELECT VALUE id FROM pair WHERE a = 1 AND b = 1 AND k = $k EXPLAIN; };
fn::pair_last(2);
array::map([{ a: 1, b: 1, k: 1 }], |$o| (SELECT VALUE id FROM pair WHERE a = $o.a AND b = $o.b AND k = $o.k EXPLAIN));
-- The key stops at the first column no equality pins.
DEFINE FUNCTION fn::pair_prefix($o: object) { RETURN SELECT VALUE id FROM pair WHERE a = $o.a AND k = 1 EXPLAIN; };
fn::pair_prefix({ a: 1 });

-- One read per key of a closure over a three-column index, two conjuncts beside it.
DEFINE TABLE field_value SCHEMALESS;
DEFINE INDEX field_value_by_generation ON field_value FIELDS tenant, scope, generation;
INSERT INTO field_value [
	{ id: 1, tenant: 'acme', scope: 's1', generation: 2, doc: 'd1', pack: 'p1' },
	{ id: 2, tenant: 'acme', scope: 's1', generation: 2, doc: 'd2', pack: 'p1' },
	{ id: 3, tenant: 'acme', scope: 's1', generation: 1, doc: 'd1', pack: 'p1' },
	{ id: 4, tenant: 'other', scope: 's1', generation: 2, doc: 'd1', pack: 'p1' },
	{ id: 5, tenant: 'acme', scope: 's2', generation: 2, doc: 'd1', pack: 'p2' }
] RETURN NONE;
LET $tenant = 'acme';
LET $pack = 'p1';
array::map([{ scope: 's1', generation: 2, doc: 'd1' }], |$key| (SELECT VALUE id FROM field_value WHERE tenant = $tenant AND scope = $key.scope AND generation = $key.generation AND doc = $key.doc AND pack = $pack EXPLAIN));

-- The rows each composite form reads.
DEFINE FUNCTION fn::pair_ids($o: object) { RETURN SELECT VALUE id FROM pair WHERE a = $o.a AND b = $o.b AND k = $o.k ORDER BY id; };
fn::pair_ids({ a: 1, b: 1, k: 2 });
array::map([{ a: 1, b: 1 }, { a: 1, b: 2 }, { a: 2, b: 2 }], |$o| (SELECT VALUE id FROM pair WHERE a = $o.a AND b = $o.b ORDER BY id));
array::map([{ scope: 's1', generation: 2, doc: 'd1' }, { scope: 's1', generation: 1, doc: 'd1' }, { scope: 's2', generation: 2, doc: 'd1' }], |$key| (SELECT VALUE id FROM field_value WHERE tenant = $tenant AND scope = $key.scope AND generation = $key.generation AND doc = $key.doc AND pack = $pack ORDER BY id));

-- Every value in either column, a float and an array in the first one included, a missing field
-- and every record find the records a forced table scan finds.
DEFINE TABLE mpair SCHEMALESS;
DEFINE INDEX mpair_ab ON mpair FIELDS a, b;
LET $classes = [1, 1.0, 1dec, 2.5, '1', true, [1, 2], NONE, NULL, [], d'2026-01-01T00:00:00Z', pair:1];
FOR $v IN $classes {
	CREATE mpair SET a = $v, b = 1 RETURN NONE;
	CREATE mpair SET a = 1, b = $v RETURN NONE;
	CREATE mpair SET a = $v, b = $v RETURN NONE;
};
array::all(array::map($classes, |$v| {
	LET $o = { a: $v, b: 1 };
	RETURN (SELECT VALUE id FROM mpair WHERE a = $o.a AND b = $o.b ORDER BY id) == (SELECT VALUE id FROM mpair WITH NOINDEX WHERE a = $o.a AND b = $o.b ORDER BY id);
}));
array::all(array::map($classes, |$v| {
	LET $o = { a: 1, b: $v };
	RETURN (SELECT VALUE id FROM mpair WHERE a = $o.a AND b = $o.b ORDER BY id) == (SELECT VALUE id FROM mpair WITH NOINDEX WHERE a = $o.a AND b = $o.b ORDER BY id);
}));
array::all(array::map($classes, |$v| {
	LET $o = { a: $v, b: $v };
	RETURN (SELECT VALUE id FROM mpair WHERE a = $o.a AND b = $o.b ORDER BY id) == (SELECT VALUE id FROM mpair WITH NOINDEX WHERE a = $o.a AND b = $o.b ORDER BY id);
}));
array::all(array::map($classes, |$v| (SELECT VALUE id FROM mpair WHERE a = 1 AND b = $v ORDER BY id) == (SELECT VALUE id FROM mpair WITH NOINDEX WHERE a = 1 AND b = $v ORDER BY id)));
array::all(array::map([{}, { a: 1 }, { b: 1 }, { a: NONE, b: 1 }, { a: NULL, b: NULL }], |$o| (SELECT VALUE id FROM mpair WHERE a = $o.a AND b = $o.b ORDER BY id) == (SELECT VALUE id FROM mpair WITH NOINDEX WHERE a = $o.a AND b = $o.b ORDER BY id)));
array::all(array::map((SELECT VALUE id FROM mpair), |$r| (SELECT VALUE id FROM mpair WHERE a = $r.a AND b = $r.b ORDER BY id) == (SELECT VALUE id FROM mpair WITH NOINDEX WHERE a = $r.a AND b = $r.b ORDER BY id)));

-- UPDATE and DELETE read their subjects through the index for a run-time operand: a parameter or
-- a path of fields into one, over one column or several.
DEFINE TABLE wi SCHEMALESS;
DEFINE INDEX wi_k ON wi FIELDS k;
DEFINE TABLE wn SCHEMALESS;
DEFINE INDEX wn_k ON wn FIELDS k;
INSERT INTO wi [
	{ id: 1, k: 1, n: 0 },
	{ id: 2, k: 1.0, n: 0 },
	{ id: 3, k: 1dec, n: 0 },
	{ id: 4, k: 2.5, n: 0 },
	{ id: 5, k: '1', n: 0 },
	{ id: 6, k: true, n: 0 },
	{ id: 7, k: [1, 2], n: 0 },
	{ id: 8, k: NONE, n: 0 },
	{ id: 9, k: NULL, n: 0 },
	{ id: 10, k: [], n: 0 },
	{ id: 11, k: d'2026-01-01T00:00:00Z', n: 0 },
	{ id: 12, k: pair:1, n: 0 },
	{ id: 13, n: 0 },
	{ id: 14, k: 1, n: 0 },
	{ id: 15, k: '1', n: 0 }
] RETURN NONE;
INSERT INTO wn [
	{ id: 1, k: 1, n: 0 },
	{ id: 2, k: 1.0, n: 0 },
	{ id: 3, k: 1dec, n: 0 },
	{ id: 4, k: 2.5, n: 0 },
	{ id: 5, k: '1', n: 0 },
	{ id: 6, k: true, n: 0 },
	{ id: 7, k: [1, 2], n: 0 },
	{ id: 8, k: NONE, n: 0 },
	{ id: 9, k: NULL, n: 0 },
	{ id: 10, k: [], n: 0 },
	{ id: 11, k: d'2026-01-01T00:00:00Z', n: 0 },
	{ id: 12, k: pair:1, n: 0 },
	{ id: 13, n: 0 },
	{ id: 14, k: 1, n: 0 },
	{ id: 15, k: '1', n: 0 }
] RETURN NONE;
DEFINE FUNCTION fn::wi_bump_plan($p: any) { RETURN UPDATE wi SET n += 1 WHERE k = $p EXPLAIN; };
fn::wi_bump_plan(1);
DEFINE FUNCTION fn::wi_bump_path_plan($o: object) { RETURN UPDATE wi SET n += 1 WHERE k = $o.k EXPLAIN; };
fn::wi_bump_path_plan({ k: 1 });
DEFINE FUNCTION fn::wi_drop_plan($p: any) { RETURN DELETE wi WHERE k = $p EXPLAIN; };
fn::wi_drop_plan(1);
DEFINE FUNCTION fn::wi_drop_path_plan($o: object) { RETURN DELETE wi WHERE k = $o.k EXPLAIN; };
fn::wi_drop_path_plan({ k: 1 });
DEFINE FUNCTION fn::pair_touch_plan($o: object) { RETURN UPDATE pair SET k = 1 WHERE a = $o.a AND b = $o.b AND k = 1 EXPLAIN; };
fn::pair_touch_plan({ a: 1, b: 1 });
-- A path that may read a record is guarded in a statement that writes: the write may change the
-- record between one row and the next, so a record id in the parameter reads the whole table.
DEFINE FUNCTION fn::wi_bump_record_plan($r: record<wi>) { RETURN UPDATE wi SET n += 1 WHERE k = $r.k EXPLAIN; };
fn::wi_bump_record_plan(wi:1);

-- Each write through the index leaves the state and returns the records the same write with
-- WITH NOINDEX does, for every value, a missing field, a write that changes the indexed field, a
-- record path and a delete.
DEFINE FUNCTION fn::wi_bump($o: object) { RETURN array::sort(UPDATE wi SET n += 1 WHERE k = $o.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wn_bump($o: object) { RETURN array::sort(UPDATE wn WITH NOINDEX SET n += 1 WHERE k = $o.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wi_bump_p($p: any) { RETURN array::sort(UPDATE wi SET n += 1 WHERE k = $p RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wn_bump_p($p: any) { RETURN array::sort(UPDATE wn WITH NOINDEX SET n += 1 WHERE k = $p RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wi_bump_r($r: record<wi>) { RETURN array::sort(UPDATE wi SET k = 'moved', n += 1 WHERE k = $r.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wn_bump_r($r: record<wn>) { RETURN array::sort(UPDATE wn WITH NOINDEX SET k = 'moved', n += 1 WHERE k = $r.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wi_move($o: object) { RETURN array::sort(UPDATE wi SET k = $o.to, n += 1 WHERE k = $o.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wn_move($o: object) { RETURN array::sort(UPDATE wn WITH NOINDEX SET k = $o.to, n += 1 WHERE k = $o.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::wi_drop($o: object) { RETURN array::sort(array::map((DELETE wi WHERE k = $o.k RETURN BEFORE), |$r| record::id($r.id))); };
DEFINE FUNCTION fn::wn_drop($o: object) { RETURN array::sort(array::map((DELETE wn WITH NOINDEX WHERE k = $o.k RETURN BEFORE), |$r| record::id($r.id))); };
DEFINE FUNCTION fn::w_same() { RETURN (SELECT VALUE [record::id(id), k, n] FROM wi ORDER BY id) == (SELECT VALUE [record::id(id), k, n] FROM wn ORDER BY id); };
fn::w_same();
array::all(array::map($classes, |$v| fn::wi_bump({ k: $v }) == fn::wn_bump({ k: $v }) AND fn::w_same()));
array::all(array::map($classes, |$v| fn::wi_bump_p($v) == fn::wn_bump_p($v) AND fn::w_same()));
array::all(array::map([{}, { k: NONE }, { k: NULL }], |$o| fn::wi_bump($o) == fn::wn_bump($o) AND fn::w_same()));
array::all(array::map($classes, |$v| fn::wi_move({ k: $v, to: 1 }) == fn::wn_move({ k: $v, to: 1 }) AND fn::w_same()));
array::all(array::map((SELECT VALUE id FROM wi ORDER BY id), |$r| fn::wi_bump_r($r) == fn::wn_bump_r(type::record('wn', record::id($r))) AND fn::w_same()));
array::all(array::map(array::concat($classes, ['moved']), |$v| fn::wi_drop({ k: $v }) == fn::wn_drop({ k: $v }) AND fn::w_same()));

-- A composite index, with every value in either column.
DEFINE TABLE ci SCHEMALESS;
DEFINE INDEX ci_ab ON ci FIELDS a, b;
DEFINE TABLE cn SCHEMALESS;
DEFINE INDEX cn_ab ON cn FIELDS a, b;
INSERT INTO ci [
	{ id: 1, a: 1, b: 1, n: 0 },
	{ id: 2, a: 1, b: 1, n: 0 },
	{ id: 3, a: 1.0, b: 1, n: 0 },
	{ id: 4, a: 1, b: 1.0, n: 0 },
	{ id: 5, a: 1dec, b: 1, n: 0 },
	{ id: 6, a: 1, b: 1dec, n: 0 },
	{ id: 7, a: 2.5, b: 1, n: 0 },
	{ id: 8, a: 1, b: 2.5, n: 0 },
	{ id: 9, a: '1', b: 1, n: 0 },
	{ id: 10, a: 1, b: '1', n: 0 },
	{ id: 11, a: true, b: 1, n: 0 },
	{ id: 12, a: 1, b: true, n: 0 },
	{ id: 13, a: [1, 2], b: 1, n: 0 },
	{ id: 14, a: 1, b: [1, 2], n: 0 },
	{ id: 15, a: NONE, b: 1, n: 0 },
	{ id: 16, a: 1, b: NONE, n: 0 },
	{ id: 17, a: NULL, b: 1, n: 0 },
	{ id: 18, a: 1, b: NULL, n: 0 },
	{ id: 19, a: [], b: 1, n: 0 },
	{ id: 20, a: 1, b: [], n: 0 },
	{ id: 21, a: d'2026-01-01T00:00:00Z', b: 1, n: 0 },
	{ id: 22, a: 1, b: d'2026-01-01T00:00:00Z', n: 0 },
	{ id: 23, a: pair:1, b: 1, n: 0 },
	{ id: 24, a: 1, b: pair:1, n: 0 }
] RETURN NONE;
INSERT INTO cn [
	{ id: 1, a: 1, b: 1, n: 0 },
	{ id: 2, a: 1, b: 1, n: 0 },
	{ id: 3, a: 1.0, b: 1, n: 0 },
	{ id: 4, a: 1, b: 1.0, n: 0 },
	{ id: 5, a: 1dec, b: 1, n: 0 },
	{ id: 6, a: 1, b: 1dec, n: 0 },
	{ id: 7, a: 2.5, b: 1, n: 0 },
	{ id: 8, a: 1, b: 2.5, n: 0 },
	{ id: 9, a: '1', b: 1, n: 0 },
	{ id: 10, a: 1, b: '1', n: 0 },
	{ id: 11, a: true, b: 1, n: 0 },
	{ id: 12, a: 1, b: true, n: 0 },
	{ id: 13, a: [1, 2], b: 1, n: 0 },
	{ id: 14, a: 1, b: [1, 2], n: 0 },
	{ id: 15, a: NONE, b: 1, n: 0 },
	{ id: 16, a: 1, b: NONE, n: 0 },
	{ id: 17, a: NULL, b: 1, n: 0 },
	{ id: 18, a: 1, b: NULL, n: 0 },
	{ id: 19, a: [], b: 1, n: 0 },
	{ id: 20, a: 1, b: [], n: 0 },
	{ id: 21, a: d'2026-01-01T00:00:00Z', b: 1, n: 0 },
	{ id: 22, a: 1, b: d'2026-01-01T00:00:00Z', n: 0 },
	{ id: 23, a: pair:1, b: 1, n: 0 },
	{ id: 24, a: 1, b: pair:1, n: 0 }
] RETURN NONE;
DEFINE FUNCTION fn::ci_bump($o: object) { RETURN array::sort(UPDATE ci SET n += 1 WHERE a = $o.a AND b = $o.b RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::cn_bump($o: object) { RETURN array::sort(UPDATE cn WITH NOINDEX SET n += 1 WHERE a = $o.a AND b = $o.b RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::c_same() { RETURN (SELECT VALUE [record::id(id), a, b, n] FROM ci ORDER BY id) == (SELECT VALUE [record::id(id), a, b, n] FROM cn ORDER BY id); };
array::all(array::map($classes, |$v| fn::ci_bump({ a: $v, b: 1 }) == fn::cn_bump({ a: $v, b: 1 }) AND fn::c_same()));
array::all(array::map($classes, |$v| fn::ci_bump({ a: 1, b: $v }) == fn::cn_bump({ a: 1, b: $v }) AND fn::c_same()));

-- A unique composite index: a key that pins a prefix of its columns reads every record under it.
DEFINE TABLE upair SCHEMALESS;
DEFINE INDEX upair_ab ON upair FIELDS a, b UNIQUE;
INSERT INTO upair [
	{ id: 1, a: 1, b: 1 },
	{ id: 2, a: 1, b: 2 },
	{ id: 3, a: 1.0, b: 3 },
	{ id: 4, a: 1.0, b: 4 },
	{ id: 5, a: 1dec, b: 5 },
	{ id: 6, a: 1dec, b: 6 },
	{ id: 7, a: 2.5, b: 7 },
	{ id: 8, a: 2.5, b: 8 },
	{ id: 9, a: '1', b: 9 },
	{ id: 10, a: '1', b: 10 },
	{ id: 11, a: true, b: 11 },
	{ id: 12, a: true, b: 12 },
	{ id: 13, a: [1, 2], b: 13 },
	{ id: 14, a: [1, 2], b: 14 },
	{ id: 15, a: NONE, b: 15 },
	{ id: 16, a: NONE, b: 16 },
	{ id: 17, a: NULL, b: 17 },
	{ id: 18, a: NULL, b: 18 },
	{ id: 19, a: [], b: 19 },
	{ id: 20, a: [], b: 20 },
	{ id: 21, a: d'2026-01-01T00:00:00Z', b: 21 },
	{ id: 22, a: d'2026-01-01T00:00:00Z', b: 22 },
	{ id: 23, a: pair:1, b: 23 },
	{ id: 24, a: pair:1, b: 24 }
] RETURN NONE;
DEFINE FUNCTION fn::upair_prefix_plan($o: object) { RETURN SELECT VALUE id FROM upair WHERE a = $o.a EXPLAIN; };
fn::upair_prefix_plan({ a: 1 });
array::all(array::map($classes, |$v| {
	LET $o = { a: $v };
	RETURN (SELECT VALUE id FROM upair WHERE a = $o.a ORDER BY id) == (SELECT VALUE id FROM upair WITH NOINDEX WHERE a = $o.a ORDER BY id);
}));
array::all(array::map($classes, |$v| {
	LET $o = { a: $v, b: 4 };
	RETURN (SELECT VALUE id FROM upair WHERE a = $o.a AND b = $o.b ORDER BY id) == (SELECT VALUE id FROM upair WITH NOINDEX WHERE a = $o.a AND b = $o.b ORDER BY id);
}));

-- The guard at work: the record the key reads changes during the write, and a later row holds its
-- new value. The write judges each row on a fresh read of the record, so it reaches that row only
-- through a read of the whole table.
DEFINE TABLE gi SCHEMALESS;
DEFINE INDEX gi_k ON gi FIELDS k;
DEFINE TABLE gn SCHEMALESS;
DEFINE INDEX gn_k ON gn FIELDS k;
INSERT INTO gi [{ id: 1, k: 1, n: 0 }, { id: 2, k: 'moved', n: 0 }, { id: 3, k: 1, n: 0 }] RETURN NONE;
INSERT INTO gn [{ id: 1, k: 1, n: 0 }, { id: 2, k: 'moved', n: 0 }, { id: 3, k: 1, n: 0 }] RETURN NONE;
DEFINE FUNCTION fn::gi_move($r: record<gi>) { RETURN array::sort(UPDATE gi SET k = 'moved', n += 1 WHERE k = $r.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::gn_move($r: record<gn>) { RETURN array::sort(UPDATE gn WITH NOINDEX SET k = 'moved', n += 1 WHERE k = $r.k RETURN VALUE record::id(id)); };
fn::gi_move(gi:1);
fn::gn_move(gn:1);
(SELECT VALUE [record::id(id), k, n] FROM gi ORDER BY id) == (SELECT VALUE [record::id(id), k, n] FROM gn ORDER BY id);

-- Each keyed field with an index of its own: the lookup reads one of them.
DEFINE TABLE twin SCHEMALESS;
DEFINE INDEX twin_a ON twin FIELDS a;
DEFINE INDEX twin_b ON twin FIELDS b;
DEFINE FUNCTION fn::twin_plan($p: int, $q: int) { RETURN SELECT VALUE id FROM twin WHERE a = $p AND b = $q EXPLAIN; };
fn::twin_plan(1, 2);

-- A record id one step into the parameter is guarded as one in the parameter is: each value a
-- step of the path reads is a guard.
DEFINE TABLE hi SCHEMALESS;
DEFINE INDEX hi_k ON hi FIELDS k;
DEFINE TABLE hn SCHEMALESS;
DEFINE INDEX hn_k ON hn FIELDS k;
INSERT INTO hi [{ id: 1, k: 1, n: 0 }, { id: 2, k: 'moved', n: 0 }, { id: 3, k: 1, n: 0 }] RETURN NONE;
INSERT INTO hn [{ id: 1, k: 1, n: 0 }, { id: 2, k: 'moved', n: 0 }, { id: 3, k: 1, n: 0 }] RETURN NONE;
DEFINE FUNCTION fn::hi_move($o: object) { RETURN array::sort(UPDATE hi SET k = 'moved', n += 1 WHERE k = $o.r.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::hn_move($o: object) { RETURN array::sort(UPDATE hn WITH NOINDEX SET k = 'moved', n += 1 WHERE k = $o.r.k RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::hi_move_plan($o: object) { RETURN UPDATE hi SET k = 'moved' WHERE k = $o.r.k EXPLAIN; };
fn::hi_move_plan({ r: hi:1 });
fn::hi_move({ r: hi:1 });
fn::hn_move({ r: hn:1 });
(SELECT VALUE [record::id(id), k, n] FROM hi ORDER BY id) == (SELECT VALUE [record::id(id), k, n] FROM hn ORDER BY id);

-- A write over the rows of a read: the read is a statement that writes nothing, and its rows are
-- gathered before the write changes the record its key reads.
DEFINE TABLE ji SCHEMALESS;
DEFINE INDEX ji_k ON ji FIELDS k;
DEFINE TABLE jn SCHEMALESS;
DEFINE INDEX jn_k ON jn FIELDS k;
INSERT INTO ji [{ id: 1, k: 1, n: 0 }, { id: 2, k: 'moved', n: 0 }, { id: 3, k: 1, n: 0 }] RETURN NONE;
INSERT INTO jn [{ id: 1, k: 1, n: 0 }, { id: 2, k: 'moved', n: 0 }, { id: 3, k: 1, n: 0 }] RETURN NONE;
DEFINE FUNCTION fn::ji_move($r: record<ji>) { RETURN array::sort(UPDATE (SELECT * FROM ji WHERE k = $r.k) SET k = 'moved', n += 1 RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::jn_move($r: record<jn>) { RETURN array::sort(UPDATE (SELECT * FROM jn WITH NOINDEX WHERE k = $r.k) SET k = 'moved', n += 1 RETURN VALUE record::id(id)); };
fn::ji_move(ji:1);
fn::jn_move(jn:1);
(SELECT VALUE [record::id(id), k, n] FROM ji ORDER BY id) == (SELECT VALUE [record::id(id), k, n] FROM jn ORDER BY id);

-- A composite write that moves each subject within the prefix its lookup reads.
DEFINE FUNCTION fn::ci_shift($o: object) { RETURN array::sort(UPDATE ci SET b = 99, n += 1 WHERE a = $o.a RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::cn_shift($o: object) { RETURN array::sort(UPDATE cn WITH NOINDEX SET b = 99, n += 1 WHERE a = $o.a RETURN VALUE record::id(id)); };
array::all(array::map($classes, |$v| fn::ci_shift({ a: $v }) == fn::cn_shift({ a: $v }) AND fn::c_same()));

-- A lookup over more columns replaces an index read chosen for the order it delivers under a
-- LIMIT; the sort stays above the lookup.
DEFINE TABLE opair SCHEMALESS;
DEFINE INDEX opair_ab ON opair FIELDS a, b;
DEFINE INDEX opair_ak ON opair FIELDS a, k;
INSERT INTO opair [{ id: 1, a: 1, b: 3, k: 1 }, { id: 2, a: 1, b: 1, k: 1 }, { id: 3, a: 1, b: 2, k: 2 }, { id: 4, a: 2, b: 0, k: 1 }] RETURN NONE;
DEFINE FUNCTION fn::opair_plan($k: int) { RETURN SELECT VALUE id FROM opair WHERE a = 1 AND k = $k ORDER BY b LIMIT 1 EXPLAIN; };
fn::opair_plan(1);
DEFINE FUNCTION fn::opair_first($k: int) { RETURN SELECT VALUE id FROM opair WHERE a = 1 AND k = $k ORDER BY b LIMIT 1; };
fn::opair_first(1);

-- An UPSERT with a WHERE writes through the index as the same write WITH NOINDEX does: every
-- subject its WHERE admits, or a record it creates where none does.
DELETE wi RETURN NONE;
DELETE wn RETURN NONE;
INSERT INTO wi [{ id: 101, k: 1, n: 0 }, { id: 102, k: 1.0, n: 0 }, { id: 103, k: '1', n: 0 }, { id: 104, k: 2, n: 0 }, { id: 105, n: 0 }] RETURN NONE;
INSERT INTO wn [{ id: 101, k: 1, n: 0 }, { id: 102, k: 1.0, n: 0 }, { id: 103, k: '1', n: 0 }, { id: 104, k: 2, n: 0 }, { id: 105, n: 0 }] RETURN NONE;
DEFINE FUNCTION fn::wi_upsert_plan($o: object) { RETURN UPSERT wi SET n += 1 WHERE k = $o.k EXPLAIN; };
fn::wi_upsert_plan({ k: 1 });
DEFINE FUNCTION fn::wi_upsert($o: object) { RETURN array::sort(UPSERT wi SET n += 1 WHERE k = $o.k RETURN VALUE [k, n]); };
DEFINE FUNCTION fn::wn_upsert($o: object) { RETURN array::sort(UPSERT wn WITH NOINDEX SET n += 1 WHERE k = $o.k RETURN VALUE [k, n]); };
DEFINE FUNCTION fn::w_pairs() { RETURN array::sort(SELECT VALUE [k, n] FROM wi) == array::sort(SELECT VALUE [k, n] FROM wn); };
array::all(array::map(array::concat($classes, ['absent', 1]), |$v| fn::wi_upsert({ k: $v }) == fn::wn_upsert({ k: $v }) AND fn::w_pairs()));
array::all(array::map([{}, { k: NONE }], |$o| fn::wi_upsert($o) == fn::wn_upsert($o) AND fn::w_pairs()));
SELECT VALUE [k, n] FROM wi WHERE id IN [wi:101, wi:102, wi:103, wi:104] ORDER BY id;

-- A record argument's field is read once when the lookup opens. A stored field holds one value
-- while a read runs, so the lookup keeps the index; a computed field is derived on each fetch and
-- may answer each differently, so where the record's table defines the stepped field as computed
-- the lookup reads the table whole and judges each row on a fetch of its own, as a scan does. A
-- scan returns both rows on about one call in four; a lookup keyed by one fetch never does.
DEFINE TABLE vol_cfg SCHEMALESS;
DEFINE FIELD coin ON vol_cfg COMPUTED rand::bool();
CREATE vol_cfg:one SET stored = true RETURN NONE;
DEFINE TABLE vol SCHEMALESS;
DEFINE INDEX vol_k ON vol FIELDS k;
INSERT INTO vol [{ id: 1, k: true }, { id: 2, k: false }] RETURN NONE;
DEFINE FUNCTION fn::vol_rows($r: record) { RETURN SELECT VALUE record::id(id) FROM vol WHERE k = $r.coin ORDER BY id; };
array::any(array::map(array::repeat(0, 200), |$i| fn::vol_rows(vol_cfg:one) == [1, 2]));
DEFINE FUNCTION fn::vol_plan($r: record) { RETURN SELECT VALUE id FROM vol WHERE k = $r.coin EXPLAIN; };
fn::vol_plan(vol_cfg:one);
DEFINE FUNCTION fn::vol_stored($r: record) { RETURN SELECT VALUE record::id(id) FROM vol WHERE k = $r.stored ORDER BY id; };
fn::vol_stored(vol_cfg:one);

-- A record id written out keys its column as the same record id held by a parameter would: one
-- keyed by a number, a string or a uuid joins the key beside the run-time operand, one keyed by
-- an array or an object does not. Every form finds the records a read WITH NOINDEX finds, and a write through the
-- lookup leaves what the write WITH NOINDEX leaves.
LET $ri_rows = [
	{ id: 1, a: 1, b: pair:1, n: 0 }, { id: 2, a: 1, b: pair:2, n: 0 }, { id: 3, a: 1, b: 1, n: 0 },
	{ id: 4, a: 2, b: pair:1, n: 0 }, { id: 5, a: 1, b: pair:x, n: 0 },
	{ id: 6, a: 1, b: pair:[1, 2], n: 0 }, { id: 7, a: 1, b: pair:{ k: 1 }, n: 0 },
	{ id: 8, a: 1, b: pair:u'0192e6a4-8b3c-7c11-9a2b-3c4d5e6f7a8b', n: 0 },
	{ id: 9, a: 1, b: other:1, n: 0 }, { id: 10, a: 1, b: 'pair:1', n: 0 },
	{ id: 11, a: 1, b: [pair:1], n: 0 }, { id: 12, a: 1.0, b: pair:1, n: 0 }
];
DEFINE TABLE ri SCHEMALESS;
DEFINE INDEX ri_ab ON ri FIELDS a, b;
DEFINE TABLE rn SCHEMALESS;
DEFINE INDEX rn_ab ON rn FIELDS a, b;
INSERT INTO ri $ri_rows RETURN NONE;
INSERT INTO rn $ri_rows RETURN NONE;
DEFINE FUNCTION fn::ri_plan($p: int) { RETURN SELECT VALUE id FROM ri WHERE a = $p AND b = pair:1 EXPLAIN; };
fn::ri_plan(1);
DEFINE FUNCTION fn::ri_plan_array($p: int) { RETURN SELECT VALUE id FROM ri WHERE a = $p AND b = pair:[1, 2] EXPLAIN; };
fn::ri_plan_array(1);
DEFINE FUNCTION fn::ri_ids($p: any) { RETURN SELECT VALUE record::id(id) FROM ri WHERE a = $p AND b = pair:1 ORDER BY id; };
fn::ri_ids(1);
DEFINE FUNCTION fn::ri_same($p: any) { RETURN [
	(SELECT VALUE id FROM ri WHERE a = $p AND b = pair:1 ORDER BY id) == (SELECT VALUE record::id(id) FROM rn WITH NOINDEX WHERE a = $p AND b = pair:1 ORDER BY id).map(|$i| type::record('ri', $i)),
	(SELECT VALUE id FROM ri WHERE a = $p AND b = pair:x ORDER BY id) == (SELECT VALUE record::id(id) FROM rn WITH NOINDEX WHERE a = $p AND b = pair:x ORDER BY id).map(|$i| type::record('ri', $i)),
	(SELECT VALUE id FROM ri WHERE a = $p AND b = pair:u'0192e6a4-8b3c-7c11-9a2b-3c4d5e6f7a8b' ORDER BY id) == (SELECT VALUE record::id(id) FROM rn WITH NOINDEX WHERE a = $p AND b = pair:u'0192e6a4-8b3c-7c11-9a2b-3c4d5e6f7a8b' ORDER BY id).map(|$i| type::record('ri', $i)),
	(SELECT VALUE id FROM ri WHERE a = $p AND b = pair:[1, 2] ORDER BY id) == (SELECT VALUE record::id(id) FROM rn WITH NOINDEX WHERE a = $p AND b = pair:[1, 2] ORDER BY id).map(|$i| type::record('ri', $i)),
	(SELECT VALUE id FROM ri WHERE a = $p AND b = pair:{ k: 1 } ORDER BY id) == (SELECT VALUE record::id(id) FROM rn WITH NOINDEX WHERE a = $p AND b = pair:{ k: 1 } ORDER BY id).map(|$i| type::record('ri', $i)),
	(SELECT VALUE id FROM ri WHERE a = $p AND b = other:1 ORDER BY id) == (SELECT VALUE record::id(id) FROM rn WITH NOINDEX WHERE a = $p AND b = other:1 ORDER BY id).map(|$i| type::record('ri', $i))
] };
array::map([1, 2, 1.0, 1dec, '1', NONE], |$p| fn::ri_same($p));
DEFINE FUNCTION fn::ri_bump_plan($p: int) { RETURN UPDATE ri SET n += 1 WHERE a = $p AND b = pair:1 EXPLAIN; };
fn::ri_bump_plan(1);
DEFINE FUNCTION fn::ri_bump($p: any) { RETURN array::sort(UPDATE ri SET n += 1 WHERE a = $p AND b = pair:1 RETURN VALUE record::id(id)); };
DEFINE FUNCTION fn::rn_bump($p: any) { RETURN array::sort(UPDATE rn WITH NOINDEX SET n += 1 WHERE a = $p AND b = pair:1 RETURN VALUE record::id(id)); };
array::all(array::map([1, 2, 1.0], |$p| fn::ri_bump($p) == fn::rn_bump($p))) AND (SELECT VALUE [record::id(id), n] FROM ri ORDER BY id) == (SELECT VALUE [record::id(id), n] FROM rn ORDER BY id);
