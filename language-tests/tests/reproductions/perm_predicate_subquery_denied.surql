/**
[env]
imports = ["reproductions/perm_predicate_subquery_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:other" }

[test]
reason = "Negative control for perm_predicate_subquery: `user:other` is not in either grant set, so both permission predicates must evaluate to false and the docs must be hidden. Confirms the unrestricted predicate evaluation still discriminates — the fix exempts permission-predicate-internal checks, it does not make the predicate allow-all."

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

SELECT VALUE id FROM doc_tbl;
SELECT VALUE id FROM doc_fld;
SELECT VALUE id FROM doc_rid;
SELECT VALUE id FROM doc_idx;
