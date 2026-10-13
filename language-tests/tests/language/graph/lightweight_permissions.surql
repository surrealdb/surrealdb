/**
[env]
imports = ["language/graph/lightweight_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "SECURITY: a lightweight relation's table-level permissions are enforced against the synthesized records. bob sees only the edge whose `in` is himself — from either endpoint, by traversal or point read — and the closed delete permission means deleting his own vertex must leave the edges in place (matching a directly denied DELETE), not silently erase them."

[[test.results]]
value = "[follows:[person:bob, person:alice]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ id: follows:[person:bob, person:alice], in: person:bob, out: person:alice }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[follows:[person:bob, person:alice]]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[follows:[person:bob, person:alice]]"
*/

-- Only bob's own outgoing edge is visible.
SELECT VALUE ->follows FROM ONLY person:bob;
SELECT VALUE <-follows FROM ONLY person:bob;
SELECT * FROM ONLY follows:[person:bob, person:alice];
SELECT * FROM ONLY follows:[person:carol, person:bob];

-- The delete permission is closed: a direct delete silently removes
-- nothing, and the record stays.
DELETE follows:[person:bob, person:alice];
SELECT VALUE ->follows FROM ONLY person:bob;

-- Counting respects the conditional select permission: only the edge whose
-- `in` is bob is counted.
SELECT count() FROM follows GROUP ALL;

-- The closed update permission gives the normal permission outcome — a
-- silent skip — not the lightweight rejection, which would disclose the
-- table's storage mode. The edge stays.
UPDATE follows:[person:bob, person:alice] SET weight = 1;
RELATE person:bob->follows:[person:bob, person:alice]->person:alice SET weight = 1;
SELECT VALUE ->follows FROM ONLY person:bob;
