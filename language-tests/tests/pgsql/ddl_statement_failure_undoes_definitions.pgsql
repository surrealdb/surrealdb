/**
[env]
namespace = true
database = true

[test]
reason = "A PostgreSQL statement that defines several things either defines all of them or none: a CREATE TABLE that repeats a column leaves no table behind, an ALTER TABLE or DROP TABLE that fails on its last part changes nothing, as in PostgreSQL"

[[test.results]]
error = "The field 'a' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 'ddl_undo_missing' does not exist"

[[test.results]]
value = "[{ x: 1i32 }]"

[[test.results]]
error = "The field 'x' already exists"

[[test.results]]
value = "NONE"
*/

CREATE TABLE ddl_undo_repeated (a INT, a INT);

CREATE TABLE ddl_undo_repeated (a INT);

CREATE TABLE ddl_undo_kept (x INT);

DROP TABLE ddl_undo_kept, ddl_undo_missing;

INSERT INTO ddl_undo_kept (x) VALUES (1) RETURNING x;

ALTER TABLE ddl_undo_kept ADD COLUMN y INT, ADD COLUMN x INT;

ALTER TABLE ddl_undo_kept ADD COLUMN y INT;
