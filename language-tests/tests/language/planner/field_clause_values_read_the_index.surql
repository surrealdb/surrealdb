/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 60000

[test]
reason = """
A field clause binds the record it is about, and a VALUE, DEFAULT or ASSERT
clause also the field's value as `$value`, its old value as `$before`, once
each time it runs, before any statement nested in it runs, as a function
binds its arguments. A statement nested in the clause whose WHERE compares an
indexed field with `=` to one of them, or to a path of fields into one, reads
through the field's index: an IndexLookup that evaluates the operand when it
opens, over one column or several.

A path that steps through a record id is guarded as it is in a function: a
read reads the whole table for a field the record's table computes. Every
value, NONE and a missing field included, finds the records a forced table
scan finds, in VALUE, ASSERT and COMPUTED clauses.
"""

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
value = "[]"

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
value = "[]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $value' }, children: [{ attributes: { access: 'k = $after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $parent.k' }, children: [{ attributes: { access: 'k = $parent.k', guard: '$parent', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $parent.a AND b = $parent.b' }, children: [{ attributes: { access: 'a = $parent.a AND b = $parent.b', guard: '$parent', index: 'item_ab', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $parent.link.k' }, children: [{ attributes: { access: 'k = $parent.link.k', guard: '$parent, $parent.link', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $before' }, children: [{ attributes: { access: 'k = $before', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $parent.k' }, children: [{ attributes: { access: 'k = $parent.k', guard: '$parent', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $parent.k' }, children: [{ attributes: { access: 'k = $parent.k', guard: '$parent', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $parent.k' }, children: [{ attributes: { access: 'k = $parent.k', guard: '$parent', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

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
value = "25"

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
value = "NONE"

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

*/

-- The records the clauses read: every class of value in `k` and in either column of `a, b`.
DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_k ON item FIELDS k;
DEFINE INDEX item_ab ON item FIELDS a, b;
LET $classes = [1, 1.0, 1dec, 2.5, '1', true, [1, 2], NONE, NULL, [], d'2026-01-01T00:00:00Z', cfg:one];
FOR $v IN $classes {
	CREATE item SET k = $v, a = $v, b = 1 RETURN NONE;
	CREATE item SET k = 1, a = 1, b = $v RETURN NONE;
};
CREATE item SET note = 'no fields' RETURN NONE;
DEFINE TABLE cfg SCHEMALESS;
CREATE cfg:one SET k = 1 RETURN NONE;

-- The plan of each form, as the clause stores it in its field.
DEFINE TABLE doc SCHEMALESS;
DEFINE FIELD by_value ON doc VALUE (SELECT VALUE id FROM item WHERE k = $value EXPLAIN);
DEFINE FIELD by_record ON doc VALUE (SELECT VALUE id FROM item WHERE k = $parent.k EXPLAIN);
DEFINE FIELD by_pair ON doc VALUE (SELECT VALUE id FROM item WHERE a = $parent.a AND b = $parent.b EXPLAIN);
DEFINE FIELD by_link ON doc VALUE (SELECT VALUE id FROM item WHERE k = $parent.link.k EXPLAIN);
DEFINE FIELD by_before ON doc VALUE (SELECT VALUE id FROM item WHERE k = $before EXPLAIN);
DEFINE FIELD by_default ON doc DEFAULT (SELECT VALUE id FROM item WHERE k = $parent.k EXPLAIN);
DEFINE FIELD by_computed ON doc COMPUTED (SELECT VALUE id FROM item WHERE k = $parent.k EXPLAIN);
CREATE doc:one SET k = 1, a = 1, b = 1, link = cfg:one, by_value = 1, by_before = 1 RETURN NONE;
doc:one.by_value;
doc:one.by_record;
doc:one.by_pair;
doc:one.by_link;
doc:one.by_before;
doc:one.by_default;
doc:one.by_computed;
(SELECT VALUE by_computed FROM ONLY doc:one);

-- Each form finds the records the same read WITH NOINDEX finds, for every value, a missing
-- field and a record id; an ASSERT that compares the two passes on every write.
DEFINE TABLE chk SCHEMALESS;
DEFINE FIELD same ON chk VALUE [
	(SELECT VALUE id FROM item WHERE k = $parent.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $parent.k ORDER BY id),
	(SELECT VALUE id FROM item WHERE a = $parent.a AND b = $parent.b ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE a = $parent.a AND b = $parent.b ORDER BY id),
	(SELECT VALUE id FROM item WHERE k = $parent.link.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $parent.link.k ORDER BY id)
];
DEFINE FIELD v ON chk ASSERT (SELECT VALUE id FROM item WHERE k = $value ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $value ORDER BY id);
DEFINE FIELD c ON chk COMPUTED (SELECT VALUE id FROM item WHERE k = $parent.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $parent.k ORDER BY id);
FOR $v IN $classes {
	CREATE chk SET k = $v, a = $v, b = 1, v = $v, link = cfg:one RETURN NONE;
	CREATE chk SET k = 1, a = 1, b = $v, v = $v, link = $v RETURN NONE;
};
CREATE chk SET note = 'no fields' RETURN NONE;
UPDATE chk SET k = 'moved', a = 'moved' RETURN NONE;
count(SELECT * FROM chk);
array::all(array::flatten(SELECT VALUE same FROM chk));
array::all(SELECT VALUE c FROM chk);

-- A path that steps through a record whose table computes the stepped field reads the whole
-- table: the filter fetches the record again for each row, so a scan returns both records of
-- `vol` on about one write in four, which a lookup keyed by one fetch never does.
DEFINE TABLE vol_cfg SCHEMALESS;
DEFINE FIELD coin ON vol_cfg COMPUTED rand::bool();
CREATE vol_cfg:one RETURN NONE;
DEFINE TABLE vol SCHEMALESS;
DEFINE INDEX vol_k ON vol FIELDS k;
INSERT INTO vol [{ id: 1, k: true }, { id: 2, k: false }] RETURN NONE;
DEFINE TABLE vsrc SCHEMALESS;
DEFINE FIELD rows ON vsrc VALUE (SELECT VALUE record::id(id) FROM vol WHERE k = $parent.link.coin ORDER BY id);
FOR $i IN 0..200 { CREATE vsrc SET link = vol_cfg:one RETURN NONE; };
array::any(SELECT VALUE rows == [1, 2] FROM vsrc);
