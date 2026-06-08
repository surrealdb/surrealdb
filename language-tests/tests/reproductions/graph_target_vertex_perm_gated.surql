/**
[env]
imports = ["reproductions/graph_target_vertex_perm_gated_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:alice" }

[test]
reason = "Edge tables with `Permission::Specific` SELECT must stay on the slow path so the per-row WHERE continues to gate `->edge->vertex` traversal. Signed in as person:alice, the WHERE `in.id = $auth.id` permits only edges whose `in` is alice."

# 0
[[test.results]]
value = "[person:bob, person:carol]"

# 1
[[test.results]]
value = "[]"

# 2
[[test.results]]
value = "[person:alice]"

# 3
[[test.results]]
value = "2"

# 4
[[test.results]]
value = "0"
*/

-- 0: alice's outbound friend edges (`->edge->vertex`). Slow path; the
--    WHERE passes for both because alice is `in`.
person:alice->friend->person;

-- 1: alice traversing through bob's edges. WHERE blocks them because
--    bob is `in`, not alice.
person:bob->friend->person;

-- 2: bob's inbound friend edges (`<-edge<-vertex`). One edge from
--    alice; WHERE passes because alice is `in`.
person:bob<-friend<-person;

-- 3: edge-only (`->edge`, no second hop). Permission still applies;
--    alice sees both of her edges.
array::len((SELECT VALUE ->friend FROM ONLY person:alice));

-- 4: edge-only from bob's side. Permission blocks both.
array::len((SELECT VALUE ->friend FROM ONLY person:bob));
