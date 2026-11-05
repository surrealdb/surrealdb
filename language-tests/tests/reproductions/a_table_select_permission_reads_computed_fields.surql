/**
[env]
imports = ["reproductions/a_table_select_permission_reads_computed_fields_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A table-level `PERMISSIONS FOR select` predicate that reads a COMPUTED field must see the value that field computes to, on every path that reads a row: a table scan, a record-id read, an index scan, a count, a record link, a graph edge, the output of a write, and a membership test the planner could otherwise answer with a join. A COMPUTED field is not stored, so judged against the record as stored the predicate reads `NONE` — a deny-intended clause admits every row, an allow-intended one hides every row."

[[test.results]]
value = "[doc:pub]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[page:pub]"

[[test.results]]
value = "[page:pub]"

[[test.results]]
value = "[{ d: NONE, p: 'public' }]"

[[test.results]]
value = "[[doc:pub]]"

# A write the table permits returns nothing the caller may not select.
[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: page:pub }]"

[[test.results]]
value = "[squad:b]"

*/

SELECT VALUE id FROM doc ORDER BY id;
SELECT VALUE id FROM doc:sec;
SELECT VALUE id FROM doc WHERE status = 'secret';
SELECT count() FROM doc GROUP ALL;
SELECT VALUE id FROM doc WHERE hidden = true;

SELECT VALUE id FROM page ORDER BY id;
SELECT VALUE id FROM page:pub;

SELECT doc.status AS d, page.status AS p FROM holder:1;
SELECT VALUE ->links->doc FROM holder:1;

UPDATE doc:sec SET n = 1 RETURN id;
CREATE doc:new SET status = 'secret' RETURN id;
UPDATE page:pub SET n = 1 RETURN id;

SELECT VALUE id FROM squad ORDER BY id;
