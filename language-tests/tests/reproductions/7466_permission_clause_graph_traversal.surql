/**
[env]
imports = ["reproductions/7465_permission_clause_probe_forms_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A graph traversal in a permission clause reads the edge table as definer code does, so it admits through an edge table the caller cannot read directly."
issue = 7466

[[test.results]]
value = "[doc:one]"

[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM doc;
SELECT * FROM linked;
