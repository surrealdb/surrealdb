/**
[env]
timeout = 30000
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
`EXPLAIN` of a statement `UPDATE` or `DELETE` over a table whose condition is the literal
`false`, alone or as a conjunct on either side of `AND`, renders the empty union as the
subject of the write, over a table with events, indexes and views, and over several
tables. A condition that is not a literal renders the table's own access path. The
statements run as they render: nothing is written and the events do not fire.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = "[{ id: t:1, x: 0 }]"

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
value = "[{ id: u:1, n: 1 }]"

[[test.results]]
value = "[{ id: u:2, n: 2 }]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
            Union [ctx: Db]
            Union [ctx: Db]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = "[{ id: u:1, n: 1 }, { id: u:2, n: 2 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ c: 2, id: uv:[] }]"

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: UPDATE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = '''
'WriteOperator [ctx: Db] [form: DELETE]
    SelectProject [ctx: Db] [projections: *]
        Union [ctx: Db]
''''

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: t:1, x: 0 }]"
*/

DEFINE TABLE t SCHEMALESS;
CREATE t:1 SET x = 0;
EXPLAIN UPDATE t SET x = 1 WHERE false;
EXPLAIN UPDATE t SET x = 1 WHERE rand() < 0;
EXPLAIN DELETE t WHERE false;
EXPLAIN DELETE t WHERE rand() < 0;
EXPLAIN UPDATE t SET x = 1 WHERE false AND true;
EXPLAIN UPDATE t SET x = 1 WHERE x = 0 AND false;
EXPLAIN UPDATE t SET x = 1 WHERE false RETURN NONE;
SELECT * FROM t;
DEFINE TABLE u SCHEMALESS;
DEFINE TABLE audit SCHEMALESS;
DEFINE EVENT ev ON TABLE u WHEN true THEN (CREATE audit SET at = time::now());
DEFINE INDEX ix ON TABLE u FIELDS n;
DEFINE TABLE uv AS SELECT count() AS c FROM u GROUP ALL;
CREATE u:1 SET n = 1;
CREATE u:2 SET n = 2;
EXPLAIN UPDATE u SET n = 5 WHERE false;
UPDATE u SET n = 5 WHERE false;
DELETE u WHERE false;
UPDATE t, u SET n = 5 WHERE false;
EXPLAIN UPDATE t, u SET n = 5 WHERE false;
UPDATE u SET n = 5 WHERE n = 1 AND false;
UPDATE u SET n = 5 WHERE false AND n = 1;
EXPLAIN UPDATE u SET n = 5 WHERE false AND n = 1;
EXPLAIN UPDATE u SET n = 5 WHERE n = 1 AND false;
SELECT * FROM u ORDER BY id;
SELECT count() FROM audit GROUP ALL;
SELECT * FROM uv;
EXPLAIN UPDATE t SET x = 1 WHERE rand() < 0 AND false;
EXPLAIN UPDATE t SET x = 1 WHERE <int> x > 0 AND false;
EXPLAIN DELETE t WHERE false AND <int> x > 0;
UPDATE t SET x = 1 WHERE <int> x > 0 AND false;
UPDATE t SET x = 1 WHERE rand() < 0 AND false;
SELECT * FROM t;
