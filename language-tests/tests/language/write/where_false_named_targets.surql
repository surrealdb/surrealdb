/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A statement `UPDATE` or `DELETE` whose condition no record satisfies runs, for each record
a named target reaches, every stage up to its `WHERE` and skips there. The
tenant guard, the existence check, the update or delete permission, the view and
lightweight-edge refusals answer as they did, a missing record answers as it did, and
what the data clause would do to a record it never reaches (an invalid cast, a missing
function, a thrown error) is never done. The result is the empty list under every
output, `ONLY` answers what it answered, and the tables, indexes, events and views are
as they were. The written literals `false`, `NONE` and `NULL` are the conditions the
statement does not run past, and the others run the whole order. `EXPLAIN` renders the
write operator as it always did.
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
value = "[{ id: person:1 }, { id: person:2 }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }]"

[[test.results]]
value = "[{ id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }]"

[[test.results]]
value = "[{ id: te:2, n: 2 }]"

[[test.results]]
error = "Cannot write to the `tv` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
error = "Cannot write to the `tv` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
error = "Cannot write to the `tv` table, as it is a view (defined with `AS SELECT`); view tables are read-only and their records are computed from the source query"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
error = "An error occurred: a LIGHTWEIGHT relation's edges cannot be updated: they carry no data beyond `id`, `in` and `out`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

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
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = "[{ id: ti:1, n: 2 }, { id: ti:2, n: 3 }]"

[[test.results]]
value = "[{ id: te:1, n: 1 }, { id: te:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: tv:[] }]"

[[test.results]]
value = "[{ id: knows:[person:1, person:2], in: person:1, out: person:2 }]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
            RecordIdScan [ctx: Db] [record_id: t:1]
            RecordIdScan [ctx: Db] [record_id: t:2]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE] {rows: 0}
    SelectProject [ctx: Db] [projections: *] {rows: 0}
        Union [ctx: Db] {rows: 0}
            RecordIdScan [ctx: Db] [record_id: t:1] {rows: 0}
            RecordIdScan [ctx: Db] [record_id: t:2] {rows: 0}

Total rows: 0''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
            RecordIdScan [ctx: Db] [record_id: t:1]
            RecordIdScan [ctx: Db] [record_id: t:2]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE] {rows: 0}
    SelectProject [ctx: Db] [projections: *] {rows: 0}
        Union [ctx: Db] {rows: 0}
            RecordIdScan [ctx: Db] [record_id: t:1] {rows: 0}
            RecordIdScan [ctx: Db] [record_id: t:2] {rows: 0}

Total rows: 0''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        RecordIdScan [ctx: Db] [record_id: ti:1]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
            RecordIdScan [ctx: Db] [record_id: t:1]
            RecordIdScan [ctx: Db] [record_id: t:2]
''''
*/

DEFINE TABLE t SCHEMALESS;
DEFINE TABLE person SCHEMALESS;
DEFINE TABLE knows TYPE RELATION IN person OUT person LIGHTWEIGHT;
-- A schema, an index and a VALUE clause: nothing after the WHERE stage needs a unit.
DEFINE TABLE ti SCHEMAFULL;
DEFINE FIELD n ON ti TYPE int VALUE $value + 1;
DEFINE INDEX ti_n ON ti FIELDS n UNIQUE;
-- An event, an index, a field and a view over the table.
DEFINE TABLE audit SCHEMALESS;
DEFINE TABLE te SCHEMALESS;
DEFINE FIELD n ON te TYPE int;
DEFINE INDEX te_n ON te FIELDS n;
DEFINE EVENT ev ON te THEN (CREATE audit SET src = $value.id, act = $event);
DEFINE TABLE tv AS SELECT count() AS c FROM te GROUP ALL;
DEFINE FUNCTION fn::f() { RETURN 1 };
CREATE person:1, person:2;
RELATE person:1->knows->person:2;
CREATE t:1 SET x = 0;
CREATE ti:1 SET n = 1;
CREATE ti:2 SET n = 2;
CREATE te:1 SET n = 1;
CREATE te:2 SET n = 2;
-- WHERE false, and the other written literals that no record satisfies.
-- WHERE false
UPDATE tv:[] SET x = 1 WHERE false;
UPDATE knows:[person:1, person:2] WHERE false;
UPDATE knows:[person:1, person:2] SET x = 1 WHERE false;
DELETE knows:[person:1, person:2] WHERE false;
DELETE t:missing WHERE false;
UPDATE t:missing SET x = 1 WHERE false;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE false RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE false;
UPDATE ONLY t:1 SET x = 1 WHERE false;
UPDATE ONLY t:1 SET x = 1 WHERE false RETURN NONE;
DELETE t:1, t:2 WHERE false RETURN BEFORE;
DELETE t:1, t:2 WHERE false RETURN NONE;
DELETE t:1, t:2 WHERE false;
DELETE ONLY t:1 WHERE false;
UPDATE t SET x = 1 WHERE false;
DELETE t WHERE false;
UPDATE t:1 SET x = <int> 'bad' WHERE false;
UPDATE t:1 SET x = fn::missing() WHERE false;
UPDATE t:1 SET x = (THROW 'boom') WHERE false;
UPDATE ti:1, ti:2 SET n = 5 WHERE false;
UPDATE ti:1, ti:2 SET n = 'bad' WHERE false;
UPDATE ti:1 SET n = 5 WHERE false RETURN AFTER;
UPDATE ti SET n = 5 WHERE false;
DELETE ti:1, ti:2 WHERE false RETURN BEFORE;
DELETE ti WHERE false;
UPDATE te SET n = 5 WHERE false;
UPDATE te:1, te:2 SET n = 5 WHERE false;
UPDATE te:1, te:2 SET n = 'bad' WHERE false;
UPDATE te:1 SET n = 5 WHERE false RETURN AFTER;
DELETE te WHERE false;
DELETE te:1, te:2 WHERE false RETURN BEFORE;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
-- WHERE NONE
UPDATE tv:[] SET x = 1 WHERE NONE;
UPDATE knows:[person:1, person:2] WHERE NONE;
UPDATE knows:[person:1, person:2] SET x = 1 WHERE NONE;
DELETE knows:[person:1, person:2] WHERE NONE;
DELETE t:missing WHERE NONE;
UPDATE t:missing SET x = 1 WHERE NONE;
UPDATE t:1, t:2 SET x = 1 WHERE NONE RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE NONE RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE NONE RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE NONE RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE NONE RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE NONE RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE NONE;
UPDATE ONLY t:1 SET x = 1 WHERE NONE;
UPDATE ONLY t:1 SET x = 1 WHERE NONE RETURN NONE;
DELETE t:1, t:2 WHERE NONE RETURN BEFORE;
DELETE t:1, t:2 WHERE NONE RETURN NONE;
DELETE t:1, t:2 WHERE NONE;
DELETE ONLY t:1 WHERE NONE;
UPDATE t SET x = 1 WHERE NONE;
DELETE t WHERE NONE;
UPDATE t:1 SET x = <int> 'bad' WHERE NONE;
UPDATE t:1 SET x = fn::missing() WHERE NONE;
UPDATE t:1 SET x = (THROW 'boom') WHERE NONE;
UPDATE ti:1, ti:2 SET n = 5 WHERE NONE;
UPDATE ti:1, ti:2 SET n = 'bad' WHERE NONE;
UPDATE ti:1 SET n = 5 WHERE NONE RETURN AFTER;
UPDATE ti SET n = 5 WHERE NONE;
DELETE ti:1, ti:2 WHERE NONE RETURN BEFORE;
DELETE ti WHERE NONE;
UPDATE te SET n = 5 WHERE NONE;
UPDATE te:1, te:2 SET n = 5 WHERE NONE;
UPDATE te:1, te:2 SET n = 'bad' WHERE NONE;
UPDATE te:1 SET n = 5 WHERE NONE RETURN AFTER;
DELETE te WHERE NONE;
DELETE te:1, te:2 WHERE NONE RETURN BEFORE;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
-- WHERE NULL
UPDATE tv:[] SET x = 1 WHERE NULL;
UPDATE knows:[person:1, person:2] WHERE NULL;
UPDATE knows:[person:1, person:2] SET x = 1 WHERE NULL;
DELETE knows:[person:1, person:2] WHERE NULL;
DELETE t:missing WHERE NULL;
UPDATE t:missing SET x = 1 WHERE NULL;
UPDATE t:1, t:2 SET x = 1 WHERE NULL RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE NULL RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE NULL RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE NULL RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE NULL RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE NULL RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE NULL;
UPDATE ONLY t:1 SET x = 1 WHERE NULL;
UPDATE ONLY t:1 SET x = 1 WHERE NULL RETURN NONE;
DELETE t:1, t:2 WHERE NULL RETURN BEFORE;
DELETE t:1, t:2 WHERE NULL RETURN NONE;
DELETE t:1, t:2 WHERE NULL;
DELETE ONLY t:1 WHERE NULL;
UPDATE t SET x = 1 WHERE NULL;
DELETE t WHERE NULL;
UPDATE t:1 SET x = <int> 'bad' WHERE NULL;
UPDATE t:1 SET x = fn::missing() WHERE NULL;
UPDATE t:1 SET x = (THROW 'boom') WHERE NULL;
UPDATE ti:1, ti:2 SET n = 5 WHERE NULL;
UPDATE ti:1, ti:2 SET n = 'bad' WHERE NULL;
UPDATE ti:1 SET n = 5 WHERE NULL RETURN AFTER;
UPDATE ti SET n = 5 WHERE NULL;
DELETE ti:1, ti:2 WHERE NULL RETURN BEFORE;
DELETE ti WHERE NULL;
UPDATE te SET n = 5 WHERE NULL;
UPDATE te:1, te:2 SET n = 5 WHERE NULL;
UPDATE te:1, te:2 SET n = 'bad' WHERE NULL;
UPDATE te:1 SET n = 5 WHERE NULL RETURN AFTER;
DELETE te WHERE NULL;
DELETE te:1, te:2 WHERE NULL RETURN BEFORE;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
-- A condition that is no written literal keeps its stages.
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2 RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2 RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2 RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2 RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2 RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2 RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE 1 > 2;
UPDATE ONLY t:1 SET x = 1 WHERE 1 > 2;
UPDATE ONLY t:1 SET x = 1 WHERE 1 > 2 RETURN NONE;
DELETE t:1, t:2 WHERE 1 > 2 RETURN BEFORE;
DELETE t:1, t:2 WHERE 1 > 2 RETURN NONE;
DELETE t:1, t:2 WHERE 1 > 2;
DELETE ONLY t:1 WHERE 1 > 2;
UPDATE t SET x = 1 WHERE 1 > 2;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
UPDATE t:1, t:2 SET x = 1 WHERE !true RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE !true RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE !true RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE !true RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE !true RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE !true RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE !true;
UPDATE ONLY t:1 SET x = 1 WHERE !true;
UPDATE ONLY t:1 SET x = 1 WHERE !true RETURN NONE;
DELETE t:1, t:2 WHERE !true RETURN BEFORE;
DELETE t:1, t:2 WHERE !true RETURN NONE;
DELETE t:1, t:2 WHERE !true;
DELETE ONLY t:1 WHERE !true;
UPDATE t SET x = 1 WHERE !true;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false RETURN BEFORE;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false RETURN AFTER;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false RETURN DIFF;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false RETURN NONE;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false RETURN NULL;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false RETURN id;
UPDATE t:1, t:2 SET x = 1 WHERE true AND false;
UPDATE ONLY t:1 SET x = 1 WHERE true AND false;
UPDATE ONLY t:1 SET x = 1 WHERE true AND false RETURN NONE;
DELETE t:1, t:2 WHERE true AND false RETURN BEFORE;
DELETE t:1, t:2 WHERE true AND false RETURN NONE;
DELETE t:1, t:2 WHERE true AND false;
DELETE ONLY t:1 WHERE true AND false;
UPDATE t SET x = 1 WHERE true AND false;
SELECT * FROM t ORDER BY id;
SELECT * FROM ti ORDER BY id;
SELECT * FROM te ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM tv;
SELECT * FROM knows ORDER BY id;
-- EXPLAIN of a statement whose stages after the WHERE are not bound.
EXPLAIN UPDATE t:1, t:2 SET x = 1 WHERE false;
EXPLAIN ANALYZE UPDATE t:1, t:2 SET x = 1 WHERE false;
EXPLAIN DELETE t:1, t:2 WHERE false;
EXPLAIN ANALYZE DELETE t:1, t:2 WHERE false;
EXPLAIN UPDATE ti:1 SET n = 1 WHERE false RETURN AFTER;
EXPLAIN UPDATE t:1, t:2 SET x = 1 WHERE rand() < 0;
