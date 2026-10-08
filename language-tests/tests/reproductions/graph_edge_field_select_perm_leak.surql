/**
[env]
imports = ["reproductions/graph_edge_field_select_perm_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:bob" }

[test]
reason = "Graph/reference traversals that materialise full edge/record objects (GraphEdgeScan FullEdge, ReferenceScan FullRecord) must apply the SAME field-level SELECT permissions and read-time COMPUTED fields as a direct `SELECT *` / FETCH: a record user traversing `->(SELECT * FROM knows)` must not see the restricted `secret` field, and must see the `label` computed field."

# 1. Direct SELECT on the edge table: baseline. `secret` is hidden by the
#    field permission; `label` is computed.
[[test.results]]
value = "[{ id: knows:e1, in: person:bob, out: person:alice, public: 'pub', owner_only: 'mine', label: 'lbl_pub' }]"

# 2. Graph traversal yielding full edge objects (FullEdge mode). Must match
#    the direct SELECT exactly: `secret` hidden, `label` computed.
[[test.results]]
value = "[{ id: knows:e1, in: person:bob, out: person:alice, public: 'pub', owner_only: 'mine', label: 'lbl_pub' }]"

# 3. Direct SELECT on the referencing table: baseline. `secret` hidden,
#    `tag` computed.
[[test.results]]
value = "[{ id: comment:c1, author: person:bob, body: 'hi', tag: 'tag_hi' }]"

# 4. Reverse-reference traversal yielding full records (FullRecord mode).
#    Must match the direct SELECT exactly.
[[test.results]]
value = "[{ id: comment:c1, author: person:bob, body: 'hi', tag: 'tag_hi' }]"

# 5. Two-hop traversal materialising the TARGET VERTEX via a projection
#    (`->knows->(SELECT * FROM person)`). The vertex's restricted `pin` field
#    must be hidden — confirming the fix applies to vertex tables reached
#    through a traversal, not just edge tables.
[[test.results]]
value = "[{ id: person:alice }]"

*/

-- 1. Direct read of the edge table (scan pipeline -- the reference behaviour).
SELECT * FROM knows;

-- 2. Graph traversal materialising full edge objects via GraphEdgeScan in
--    FullEdge mode. Field permission must hide `secret`; `label` must be
--    computed.
person:bob->(SELECT * FROM knows);

-- 3. Direct read of the referencing table (scan pipeline).
SELECT * FROM comment;

-- 4. Reverse-reference traversal materialising full records via
--    ReferenceScan in FullRecord mode.
person:bob<~(SELECT * FROM comment);

-- 5. Two-hop traversal materialising the target vertex; the vertex's
--    restricted `pin` field must be hidden.
person:bob->knows->(SELECT * FROM person);
