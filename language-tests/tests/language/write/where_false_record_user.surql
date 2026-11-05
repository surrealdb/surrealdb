/**
[env]
timeout = 30000
imports = ["./where_false_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
The permission stages of a statement that no record can satisfy still run for each record a
named target reaches, since they precede its `WHERE`: a permission predicate that raises on
a record fails the statement with its error, a record the permission refuses is skipped, and a
record the caller may update or delete is skipped by the `WHERE`. The literal `false` and a
condition that is never truthy but is no literal answer alike for every statement, and nothing
is written.
"""

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

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
error = "The table 'missing_table' does not exist"

[[test.results]]
error = "The table 'missing_table' does not exist"

[[test.results]]
error = "The table 'missing_table' does not exist"

[[test.results]]
error = "The table 'missing_table' does not exist"

[[test.results]]
value = "[{ code: 1, id: g:1 }, { code: 'x', id: g:2 }, { code: 3, id: g:3 }]"

[[test.results]]
value = "[{ code: 1, id: denied:1 }]"

[[test.results]]
value = "[{ code: 1, id: open:1 }]"
*/

UPDATE g:1 SET n = 1 WHERE false;
UPDATE g:1 SET n = 1 WHERE rand() < 0;
UPDATE g:2 SET n = 1 WHERE false;
UPDATE g:2 SET n = 1 WHERE rand() < 0;
UPDATE g:1, g:2, g:3 SET n = 1 WHERE false;
UPDATE g:1, g:2, g:3 SET n = 1 WHERE rand() < 0;
UPDATE g:3, g:1 SET n = 1 WHERE false RETURN NONE;
UPDATE g:3, g:1 SET n = 1 WHERE rand() < 0 RETURN NONE;
UPDATE g:2 SET n = 1 WHERE false RETURN NONE;
UPDATE g:2 SET n = 1 WHERE rand() < 0 RETURN NONE;
DELETE g:1 WHERE false;
DELETE g:1 WHERE rand() < 0;
DELETE g:2 WHERE false;
DELETE g:2 WHERE rand() < 0;
DELETE g:1, g:2 WHERE false RETURN BEFORE;
DELETE g:1, g:2 WHERE rand() < 0 RETURN BEFORE;
UPDATE denied:1 SET n = 1 WHERE false;
UPDATE denied:1 SET n = 1 WHERE rand() < 0;
UPDATE denied:1 SET n = 1 WHERE false RETURN NONE;
UPDATE denied:1 SET n = 1 WHERE rand() < 0 RETURN NONE;
DELETE denied:1 WHERE false;
DELETE denied:1 WHERE rand() < 0;
UPDATE denied:2 SET n = 1 WHERE false;
UPDATE denied:2 SET n = 1 WHERE rand() < 0;
UPDATE open:1, open:2 SET n = 1 WHERE false RETURN AFTER;
UPDATE open:1, open:2 SET n = 1 WHERE rand() < 0 RETURN AFTER;
DELETE open:1, open:2 WHERE false RETURN BEFORE;
DELETE open:1, open:2 WHERE rand() < 0 RETURN BEFORE;
UPDATE missing_table:1 SET n = 1 WHERE false;
UPDATE missing_table:1 SET n = 1 WHERE rand() < 0;
DELETE missing_table:1 WHERE false;
DELETE missing_table:1 WHERE rand() < 0;
SELECT * FROM g ORDER BY id;
SELECT * FROM denied;
SELECT * FROM open;
