/**
[env]
imports = ["language/graph/inline_props_permissions_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "SECURITY: the adjacency payloads embed an INLINE field's raw values, and bob's field-level SELECT permission on `score` is NONE. His filtered traversal must therefore run on the record path, where the hidden field is nulled before the predicate evaluates — matching a direct SELECT with the same WHERE — rather than letting the raw payload values steer his visible result set."

[[test.results]]
value = "[{ id: likes:e1, in: person:bob, out: person:alice }, { id: likes:e2, in: person:bob, out: person:alice }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"
*/

-- Both edges are visible records, but `score` is hidden from bob.
SELECT * FROM likes ORDER BY id;

-- Filtering on the hidden field matches nothing — for the traversal and
-- the direct query alike. If the payload path had evaluated the raw
-- values, the traversal would have leaked that likes:e1 has score > 5.
SELECT VALUE ->(likes WHERE score > 5) FROM ONLY person:bob;
SELECT VALUE id FROM likes WHERE score > 5;
