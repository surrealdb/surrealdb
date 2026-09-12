/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """
A computed field's declared TYPE types the layer column that derives it, and a
projection of the field is typed by that column. An outer `SELECT *` over a
subquery projecting only such fields has nothing to dereference and is
dropped; over a computed field with no declared TYPE the projected kind is
unknown and the outer projection stays.
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
value = "[{ id: t:1, len: 3, name: 'abc' }]"

[[test.results]]
value = "[{ id: u:1, len: 3, name: 'abc' }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: len]
    ProjectLayer [ctx: Db] [columns: len = string::len(name) : int @computed]
        TableScan [ctx: Db] [table: t, direction: Forward]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    SelectProject [ctx: Db] [projections: len]
        ProjectLayer [ctx: Db] [columns: len = string::len(name) @computed]
            TableScan [ctx: Db] [table: u, direction: Forward]
''''

[[test.results]]
value = "[{ len: 3 }]"

[[test.results]]
value = "[{ len: 3 }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD name ON t TYPE string;
DEFINE FIELD len ON t TYPE int COMPUTED string::len(name);
DEFINE TABLE u SCHEMAFULL;
DEFINE FIELD name ON u TYPE string;
DEFINE FIELD len ON u COMPUTED string::len(name);
CREATE t:1 SET name = 'abc';
CREATE u:1 SET name = 'abc';

-- The declared kind makes the projected row plain: no outer projection.
EXPLAIN SELECT * FROM (SELECT len FROM t);
-- Without a declared kind the projected field may hold a link: kept.
EXPLAIN SELECT * FROM (SELECT len FROM u);

-- Both shapes return the same rows.
SELECT * FROM (SELECT len FROM t);
SELECT * FROM (SELECT len FROM u);
