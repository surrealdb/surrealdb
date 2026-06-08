/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Vertex-side adjacency keys carry the target vertex so `->edge->vertex` collapses to a single scan. Covers the FULL-permission fast path on `->`, `<-`, and edge-only directions. Permission-gated edges are covered separately by `graph_target_vertex_perm_gated.surql`, which runs under a record-scoped session so the WHERE clause actually filters."

# 0
[[test.results]]
value = "NONE"

# 1
[[test.results]]
value = "[person:bob, person:carol]"

# 2
[[test.results]]
value = "[likes:l1, likes:l2]"

# 3
[[test.results]]
value = "[person:alice]"
*/

-- 0: Setup. Plain edge table with default permissions takes the fast path.
{
    CREATE person:alice, person:bob, person:carol;
    RELATE person:alice->likes:l1->person:bob;
    RELATE person:alice->likes:l2->person:carol;
    NONE
};

-- 1: `->edge->vertex` returns the target vertices, single scan via TargetVertex.
person:alice->likes->person;

-- 2: `->edge` alone still returns the edge IDs (TargetId mode, unchanged).
person:alice->likes;

-- 3: Reverse direction `<-edge<-vertex` exercises the same fast path.
person:bob<-likes<-person;
