/**
[env]
namespace = true
database = true
auth = { level = "owner" }
timeout = 60000

[test]
reason = """
An event binds `$before`, `$after`, `$value` and `$event` once each time it
runs, before any statement of its body runs, as a function binds its
arguments. A statement of the body whose WHERE compares an indexed field with
`=` to one of them, or to a path of fields into one, reads through the field's
index: an IndexLookup that evaluates the operand when it opens, over one
column or several, for SELECT, UPDATE, DELETE and UPSERT alike, in the
event's WHEN clause, and from a statement nested in an IF, a FOR or a closure
of the body.

A path that steps through a record id is guarded as it is in a function: a
read keeps the index for a stored field and reads the whole table for a field
the record's table computes. Every value, NONE and a missing field included,
finds the records a forced table scan finds, and every write leaves the state
the same write WITH NOINDEX leaves.
"""
issue = 7698

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $after.k' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $before.k' }, children: [{ attributes: { access: 'k = $before.k', guard: '$before', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'a = $after.a AND b = $after.b' }, children: [{ attributes: { access: 'a = $after.a AND b = $after.b', guard: '$after', index: 'item_ab', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'kind = $event' }, children: [{ attributes: { access: 'kind = $event', index: 'item_kind', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $value.k' }, children: [{ attributes: { access: 'k = $value.k', guard: '$parent', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $after.link.k' }, children: [{ attributes: { access: 'k = $after.link.k', guard: '$after, $after.link', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { form: 'UPDATE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'DELETE' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { form: 'UPSERT' }, children: [{ attributes: { projections: '*' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'SelectProject' }], context: 'Db', operator: 'WriteOperator' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $after.k' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $after.k' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

[[test.results]]
value = "{ attributes: { expr: 'id' }, children: [{ attributes: { predicate: 'k = $after.k' }, children: [{ attributes: { access: 'k = $after.k', guard: '$after', index: 'item_k', table: 'item' }, context: 'Db', operator: 'IndexLookup' }], context: 'Db', operator: 'Filter' }], context: 'Db', operator: 'ProjectValue' }"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ event: 'CREATE', events: 25 }, { event: 'DELETE', events: 25 }, { event: 'UPDATE', events: 25 }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ event: 'CREATE', events: 25 }, { event: 'DELETE', events: 25 }, { event: 'UPDATE', events: 25 }]"

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
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "10"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "true"

[[test.results]]
value = "10"

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
value = "NONE"

[[test.results]]
value = "true"

*/

-- The records the events read: every class of value in `k` and in either column of `a, b`, and
-- `kind` naming an event.
DEFINE TABLE item SCHEMALESS;
DEFINE INDEX item_k ON item FIELDS k;
DEFINE INDEX item_ab ON item FIELDS a, b;
DEFINE INDEX item_kind ON item FIELDS kind;
LET $classes = [1, 1.0, 1dec, 2.5, '1', true, [1, 2], NONE, NULL, [], d'2026-01-01T00:00:00Z', cfg:one];
FOR $v IN $classes {
	CREATE item SET k = $v, a = $v, b = 1, kind = 'CREATE' RETURN NONE;
	CREATE item SET k = 1, a = 1, b = $v, kind = 'UPDATE' RETURN NONE;
};
CREATE item SET kind = 'DELETE' RETURN NONE;
DEFINE TABLE cfg SCHEMALESS;
CREATE cfg:one SET k = 1 RETURN NONE;

-- The plan of each form, as an event that runs on an UPDATE prints it.
DEFINE TABLE src SCHEMALESS;
DEFINE TABLE plan SCHEMALESS;
DEFINE EVENT plans ON src WHEN $event = 'UPDATE' THEN {
	CREATE plan:1 SET at = (SELECT VALUE id FROM item WHERE k = $after.k EXPLAIN);
	CREATE plan:2 SET at = (SELECT VALUE id FROM item WHERE k = $before.k EXPLAIN);
	CREATE plan:3 SET at = (SELECT VALUE id FROM item WHERE a = $after.a AND b = $after.b EXPLAIN);
	CREATE plan:4 SET at = (SELECT VALUE id FROM item WHERE kind = $event EXPLAIN);
	CREATE plan:5 SET at = (SELECT VALUE id FROM item WHERE k = $value.k EXPLAIN);
	CREATE plan:6 SET at = (SELECT VALUE id FROM item WHERE k = $after.link.k EXPLAIN);
	CREATE plan:7 SET at = (UPDATE item SET n += 1 WHERE k = $after.k EXPLAIN);
	CREATE plan:8 SET at = (DELETE item WHERE k = $after.k EXPLAIN);
	CREATE plan:9 SET at = (UPSERT item SET n += 1 WHERE k = $after.k EXPLAIN);
	IF $after.k {
		CREATE plan:10 SET at = (SELECT VALUE id FROM item WHERE k = $after.k EXPLAIN);
	};
	FOR $x IN [1] {
		CREATE plan:11 SET at = (SELECT VALUE id FROM item WHERE k = $after.k EXPLAIN);
	};
	CREATE plan:12 SET at = array::map([1], |$x| (SELECT VALUE id FROM item WHERE k = $after.k EXPLAIN))[0];
};
CREATE src:one SET k = 1, a = 1, b = 1, link = cfg:one RETURN NONE;
UPDATE src:one SET k = 2 RETURN NONE;
plan:1.at;
plan:2.at;
plan:3.at;
plan:4.at;
plan:5.at;
plan:6.at;
plan:7.at;
plan:8.at;
plan:9.at;
plan:10.at;
plan:11.at;
plan:12.at;

-- Each form finds the records the same read WITH NOINDEX finds, for every value in either image,
-- a missing field, a record id, and each event.
DEFINE TABLE chk SCHEMALESS;
DEFINE TABLE got SCHEMALESS;
DEFINE EVENT rows ON chk WHEN true THEN {
	CREATE got SET event = $event, same = [
		(SELECT VALUE id FROM item WHERE k = $after.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $after.k ORDER BY id),
		(SELECT VALUE id FROM item WHERE k = $before.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $before.k ORDER BY id),
		(SELECT VALUE id FROM item WHERE a = $after.a AND b = $after.b ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE a = $after.a AND b = $after.b ORDER BY id),
		(SELECT VALUE id FROM item WHERE a = $before.a AND b = $before.b ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE a = $before.a AND b = $before.b ORDER BY id),
		(SELECT VALUE id FROM item WHERE kind = $event ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE kind = $event ORDER BY id),
		(SELECT VALUE id FROM item WHERE k = $after.link.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $after.link.k ORDER BY id),
		(SELECT VALUE id FROM item WHERE k = $value.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $value.k ORDER BY id)
	] RETURN NONE;
};
-- A WHEN clause is code of the event as its body is: its reads find the same records.
DEFINE TABLE when_got SCHEMALESS;
DEFINE EVENT when_rows ON chk WHEN (SELECT VALUE id FROM item WHERE k = $after.k ORDER BY id) == (SELECT VALUE id FROM item WITH NOINDEX WHERE k = $after.k ORDER BY id) THEN {
	CREATE when_got SET event = $event RETURN NONE;
};
FOR $v IN $classes {
	CREATE chk SET k = $v, a = $v, b = 1, link = cfg:one RETURN NONE;
	CREATE chk SET k = 1, a = 1, b = $v, link = $v RETURN NONE;
};
CREATE chk SET note = 'no fields' RETURN NONE;
UPDATE chk SET k = 'moved', a = 'moved' RETURN NONE;
DELETE chk RETURN NONE;
SELECT event, count() AS events FROM got GROUP BY event ORDER BY event;
array::all(array::flatten(SELECT VALUE same FROM got));
SELECT event, count() AS events FROM when_got GROUP BY event ORDER BY event;

-- Each write of an event body leaves the state the same write WITH NOINDEX leaves, written
-- directly in the body and nested in an IF.
DEFINE TABLE wi SCHEMALESS;
DEFINE INDEX wi_k ON wi FIELDS k;
DEFINE TABLE wn SCHEMALESS;
DEFINE INDEX wn_k ON wn FIELDS k;
FOR $v IN $classes {
	CREATE wi SET k = $v, n = 0 RETURN NONE;
	CREATE wn SET k = $v, n = 0 RETURN NONE;
};
CREATE wi SET n = 0 RETURN NONE;
CREATE wn SET n = 0 RETURN NONE;
DEFINE TABLE wsrc SCHEMALESS;
DEFINE EVENT writes ON wsrc WHEN $event != 'DELETE' THEN {
	UPDATE wi SET n += 1 WHERE k = $after.k;
	UPDATE wn WITH NOINDEX SET n += 1 WHERE k = $after.k;
	UPSERT wi SET n += 10 WHERE k = $before.k;
	UPSERT wn WITH NOINDEX SET n += 10 WHERE k = $before.k;
};
DEFINE EVENT deletes ON wsrc WHEN $event = 'DELETE' THEN {
	DELETE wi WHERE k = $before.k;
	DELETE wn WITH NOINDEX WHERE k = $before.k;
};
DEFINE TABLE wsrc_if SCHEMALESS;
DEFINE EVENT writes_if ON wsrc_if WHEN true THEN {
	IF $event = 'DELETE' {
		DELETE wi WHERE k = $before.k;
		DELETE wn WITH NOINDEX WHERE k = $before.k;
	} ELSE {
		UPDATE wi SET n += 1 WHERE k = $after.k;
		UPDATE wn WITH NOINDEX SET n += 1 WHERE k = $after.k;
	};
};
DEFINE FUNCTION fn::w_same() { RETURN array::sort(SELECT VALUE [k, n] FROM wi) == array::sort(SELECT VALUE [k, n] FROM wn); };
FOR $v IN $classes { CREATE wsrc SET k = $v RETURN NONE; };
fn::w_same();
UPDATE wsrc SET k = 1 RETURN NONE;
fn::w_same();
DELETE wsrc RETURN NONE;
fn::w_same();
count(SELECT * FROM wi);
FOR $v IN $classes { CREATE wsrc_if SET k = $v RETURN NONE; };
fn::w_same();
UPDATE wsrc_if SET k = 'moved' RETURN NONE;
DELETE wsrc_if RETURN NONE;
fn::w_same();
count(SELECT * FROM wi);

-- A path that steps through a record whose table computes the stepped field reads the whole
-- table: the filter fetches the record again for each row, so a scan returns both records of
-- `vol` on about one event in four, which a lookup keyed by one fetch never does.
DEFINE TABLE vol_cfg SCHEMALESS;
DEFINE FIELD coin ON vol_cfg COMPUTED rand::bool();
CREATE vol_cfg:one RETURN NONE;
DEFINE TABLE vol SCHEMALESS;
DEFINE INDEX vol_k ON vol FIELDS k;
INSERT INTO vol [{ id: 1, k: true }, { id: 2, k: false }] RETURN NONE;
DEFINE TABLE vsrc SCHEMALESS;
DEFINE TABLE vlog SCHEMALESS;
DEFINE EVENT coin ON vsrc WHEN true THEN {
	CREATE vlog SET rows = (SELECT VALUE record::id(id) FROM vol WHERE k = $after.link.coin ORDER BY id) RETURN NONE;
};
FOR $i IN 0..200 { CREATE vsrc SET link = vol_cfg:one RETURN NONE; };
array::any(SELECT VALUE rows == [1, 2] FROM vlog);
