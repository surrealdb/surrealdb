/**
[env]
imports = ["language/write/delete_runs_the_select_gate_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "SECURITY: DELETE evaluates the table's `PERMISSIONS FOR select` clause against the record's pre-image before it projects anything, even though a DELETE with no RETURN clause has nothing to project. The gate is therefore observable twice over: a predicate that throws makes the whole statement fail, and a denying clause suppresses `RETURN BEFORE` while the delete itself still stands. Reasoning that a DELETE returns nothing and so needs no read gate drops a permission check."

[[test.results]]
error = "An error occurred: select gate ran"

[[test.results]]
value = "[]"

[[test.results]]
value = "[quiet:1]"
*/

-- The gate runs, and the record's own select predicate can fail the statement.
DELETE loud:1;

-- A denied gate suppresses the before-image the statement asked for...
DELETE quiet:1 RETURN BEFORE;
-- ...while the delete it was hiding did happen.
SELECT VALUE who FROM witness;
