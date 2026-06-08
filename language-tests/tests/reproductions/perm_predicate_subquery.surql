/**
[env]
imports = ["reproductions/perm_predicate_subquery_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:martin" }

[test]
reason = "Subqueries inside a SELECT permission predicate must evaluate unrestricted, for both the table-permission gate (scanning a source with table-level NONE) and the field-permission gate (projecting a field with field-level NONE). A granted user must see the doc in both cases. Companion to graph_perm_predicate_traversal, covering the streaming scan-pipeline paths (`check_perm!` and `filter_fields_by_permission`)."

# (a) predicate subquery over a table-level NONE source resolves -> doc visible.
[[test.results]]
value = "[doc_tbl:d1]"

# (b) predicate subquery projecting a field-level NONE field resolves -> doc visible.
[[test.results]]
value = "[doc_fld:d1]"

# (c) predicate subquery via a record-id scan over a NONE source -> doc visible.
[[test.results]]
value = "[doc_rid:d1]"

# (d) predicate subquery via an index scan over a NONE source -> doc visible.
[[test.results]]
value = "[doc_idx:d1]"
*/

SELECT VALUE id FROM doc_tbl;
SELECT VALUE id FROM doc_fld;
SELECT VALUE id FROM doc_rid;
SELECT VALUE id FROM doc_idx;
