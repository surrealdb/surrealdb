/**
[env]
timeout = 30000
imports = ["./where_false_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = """
A statement `UPDATE` or `DELETE` over a table whose condition is the literal `false` reads
no record of it: a permission predicate that raises on a stored record does not fail the
statement, and the answer is the empty list under every output. A condition that is never
truthy but is no literal reads the table and raises the predicate's error where a record
makes it raise. The literals `NONE` and `NULL` are no such condition for the table read, so
the permission predicate is judged on each record. `UPSERT` over a table creates its record
under a falsy condition. The tables are as they were.
"""

[[test.results]]
value = "[]"

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
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ code: 1, id: g:1 }, { code: 'x', id: g:2 }, { code: 3, id: g:3 }]"

[[test.results]]
value = "[{ code: 1, id: denied:1 }]"

[[test.results]]
value = "[{ code: 1, id: open:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ code: 1, id: g:1 }, { code: 'x', id: g:2 }, { code: 3, id: g:3 }]"
*/

UPDATE g SET n = 1 WHERE false;
UPDATE g SET n = 1 WHERE rand() < 0;
UPDATE g SET n = 1 WHERE false AND true;
UPDATE g SET n = 1 WHERE false RETURN NONE;
UPDATE g SET n = 1 WHERE NONE;
UPDATE g SET n = 1 WHERE NULL;
DELETE g WHERE false;
DELETE g WHERE rand() < 0;
DELETE g WHERE false RETURN BEFORE;
UPDATE denied SET n = 1 WHERE false;
UPDATE denied SET n = 1 WHERE rand() < 0;
DELETE denied WHERE false;
UPDATE open SET n = 1 WHERE false RETURN AFTER;
UPDATE open SET n = 1 WHERE rand() < 0 RETURN AFTER;
DELETE open WHERE false RETURN BEFORE;
UPDATE missing_table SET n = 1 WHERE false;
UPDATE missing_table SET n = 1 WHERE rand() < 0;
DELETE missing_table WHERE false;
DELETE missing_table WHERE rand() < 0;
UPSERT g SET n = 1 WHERE false RETURN NONE;
SELECT count() FROM g GROUP ALL;
SELECT * FROM g WHERE code != NONE ORDER BY id;
SELECT * FROM denied;
SELECT * FROM open;
UPDATE g SET n = 1 WHERE <int> code > 0 AND false;
UPDATE g SET n = 1 WHERE false AND <int> code > 0;
UPDATE g SET n = 1 WHERE rand() < 0 AND false;
DELETE g WHERE <int> code > 0 AND false;
SELECT * FROM g WHERE code != NONE ORDER BY id;
