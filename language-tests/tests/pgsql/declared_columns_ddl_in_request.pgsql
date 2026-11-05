/**
[env]
namespace = true
database = true
imports = ["pgsql/_declared_columns.surql"]

[test]
reason = "A request that alters a table is lowered before any of it runs, so the statements after the change are not checked against the columns the table had before it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, label: 'Eng', size: 10i32 }, { id: 2, label: 'Ops', size: 10i32 }]"

[[test.results]]
value = "[{ id: 1, size: 10i32 }, { id: 2, size: 10i32 }]"

[[test.results]]
value = "[{ id: 1, label: 'Eng', size: 10i32 }, { id: 2, label: 'Ops', size: 10i32 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, w: 7i32 }]"

[[test.results]]
value = "[{ w: 7i32 }]"
*/

ALTER TABLE declcol_dept ADD COLUMN size INT;
UPDATE declcol_dept SET size = 10 WHERE size IS NULL;
SELECT id, size FROM declcol_dept ORDER BY id;
SELECT * FROM declcol_dept ORDER BY id;

ALTER TABLE declcol_empty DROP COLUMN v;
ALTER TABLE declcol_empty ADD COLUMN w INT;
INSERT INTO declcol_empty (id, w) VALUES (1, 7);
SELECT w FROM declcol_empty;
