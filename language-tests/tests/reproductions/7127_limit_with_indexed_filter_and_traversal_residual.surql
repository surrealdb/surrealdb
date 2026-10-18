/**
[env]
imports = ["reproductions/7127_limit_with_indexed_filter_and_traversal_residual_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:u" }

[test]
reason = "LIMIT over an index-resolved predicate plus a graph-traversal residual under record-user permissions returns every qualifying row"
issue = 7127

[[test.results]]
value = "12"

[[test.results]]
value = "[memory:1, memory:2, memory:3, memory:4, memory:5]"

*/

RETURN array::len((SELECT VALUE id FROM memory WHERE <-has_memory<-project CONTAINS project:p AND archived_at = NONE LIMIT 50));
SELECT VALUE id FROM memory WHERE <-has_memory<-project CONTAINS project:p AND archived_at = NONE LIMIT 5;
