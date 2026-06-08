/**
[env]
imports = ["reproductions/graph_select_perm_bypass_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:alice" }

[test]
reason = "A graph traversal `->friend` over an edge table with a row-level SELECT WHERE clause must apply that clause to each edge record before yielding the result, both for the legacy `compute-only` executor and for the streaming planners. The `friend` table has `PERMISSIONS FOR select WHERE in.id = $auth.id`; querying as `person:alice` for the edges of `person:bob` must therefore return `[]` because `bob != alice`."

# Edge-only traversal: row-level SELECT WHERE excludes bob's edge from
# alice's view.
[[test.results]]
value = "[]"

# Two-hop traversal: the edge is hidden, so the next-hop target vertex
# must also be hidden.
[[test.results]]
value = "[]"

*/

SELECT VALUE ->friend FROM ONLY person:bob;
SELECT VALUE ->friend->person FROM ONLY person:bob;
