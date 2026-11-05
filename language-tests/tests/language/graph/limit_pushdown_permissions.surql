/**
[env]
imports = ["language/graph/limit_pushdown_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "A LIMIT inside a graph lookup caps the number of *visible* rows: the lowest-id edge is permission-hidden for this record user, so an execution that counted scanned candidates toward the cap — before permission filtering — would return fewer rows than requested (or none at all). Ordered and plain LIMIT shapes alike must return the visible edge."

[[test.results]]
value = "[knows:e2]"

[[test.results]]
value = "[knows:e2]"

[[test.results]]
value = "[knows:e2]"
*/

-- ORDER BY id LIMIT: the hidden lowest-id edge must not consume the cap.
SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 1).id FROM ONLY person:bob;

-- Plain LIMIT with no sort: the same counting rule applies.
SELECT VALUE ->(knows LIMIT 1) FROM ONLY person:bob;

-- A cap above the visible degree returns exactly the visible edges.
SELECT VALUE ->(SELECT id FROM knows ORDER BY id LIMIT 2).id FROM ONLY person:bob;
