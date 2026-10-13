/**
[env]
imports = ["language/graph/inline_cache_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "SECURITY: a cache-served traversal must enforce the same SELECT permissions as the scan it replaces. The owner's writes materialised person:bob's caches with every edge and back-link — including the non-public ones — so if the cache tier ever bypassed `resolve_record_batch`'s permission gate, the record user would see knows:e2 / comment:c2 below."

[[test.results]]
value = "[knows:e1]"

[[test.results]]
value = "[person:alice]"

[[test.results]]
value = "[comment:c1]"
*/

-- Graph traversal from the cached vertex: only the public edge.
SELECT VALUE ->knows FROM ONLY person:bob;
SELECT VALUE ->knows->person FROM ONLY person:bob;

-- Reverse-reference lookup from the cached record: only the public comment.
SELECT VALUE <~comment FROM ONLY person:bob;
