/**
[env]
imports = ["./missing_table_record_user_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = """
A record user's reads are permission-checked, so the absence of a table is
hidden from it: an UPDATE or DELETE that names a table which is not defined, by
table name or through a dynamic target, answers an empty result whatever its
WHERE, as a SELECT of it does, rather than raising the table's absence. A target that names a record or a range of records raises.
"""

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
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
error = "The table 'nosuch' does not exist"

[[test.results]]
value = "[]"
*/
UPDATE nosuch SET a = 1;
UPDATE nosuch SET a = 1 WHERE false;
UPDATE nosuch SET a = 1 WHERE 1 = 2;
DELETE nosuch;
DELETE nosuch WHERE false;
UPDATE type::table('nosuch') SET a = 1;
UPDATE type::table('nosuch') SET a = 1 WHERE false;
UPDATE nosuch:1 SET a = 1;
DELETE nosuch:1;
UPDATE nosuch:1..5 SET a = 1 WHERE false;
SELECT * FROM nosuch;
