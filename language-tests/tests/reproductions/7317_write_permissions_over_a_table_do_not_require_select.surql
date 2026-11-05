/**
[env]
imports = ["reproductions/7317_write_permissions_do_not_require_select_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A write over a whole table is judged per record by the table's update or delete permission, never by select: every record the caller may update or delete is written, though none is returned to a caller who may not read it."
issue = 7317

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['DELETE', 'UPDATE']"
*/

UPDATE node SET touched = true;
DELETE node;
-- The event, which runs as the definer, saw both writes land.
RETURN (SELECT VALUE what FROM audit ORDER BY what);
