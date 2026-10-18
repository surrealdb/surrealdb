/**
[env]
imports = ["reproductions/7317_write_permissions_do_not_require_select_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A write is judged by the table's create, update or delete permission, never by select. The record is written even though the caller may not read it back, so the statement's own result (the row, which select would hide) is empty while the write is real; a client must not mistake the empty result for a refusal."
issue = 7317

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "['CREATE', 'DELETE', 'UPDATE']"
*/

CREATE node:new;
UPDATE node:keep SET touched = true;
DELETE node:keep;
-- The event, which runs as the definer, saw all three writes land.
RETURN (SELECT VALUE what FROM audit ORDER BY what);
