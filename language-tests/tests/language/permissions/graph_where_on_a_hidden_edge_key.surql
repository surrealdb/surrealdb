/**
[env]
imports = ["language/permissions/graph_where_on_a_hidden_edge_key_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "person:me" }

[test]
reason = "SECURITY: a record user's graph hop `WHERE` over an edge's `out` reads `out` as the user's view of the edge holds it, NONE where a field SELECT permission hides it, never the raw value the adjacency entry's key carries. Pruning edges by the raw key would answer which edge leads to person:secret."

[[test.results]]
value = "[{ id: knows:to_friend, in: person:me }, { id: knows:to_secret, in: person:me }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[knows:to_friend, knows:to_secret]"

[[test.results]]
value = "[knows:to_friend, knows:to_secret]"

[[test.results]]
value = "[knows:to_friend, knows:to_secret]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"
*/

-- Both edges are visible records, but `out` is hidden from the user.
SELECT * FROM knows ORDER BY id;

-- A comparison with the hidden value matches nothing: not through the hop,
-- not through the hop and on to its far end, and not through a direct
-- SELECT with the same WHERE.
SELECT VALUE ->(knows WHERE out = person:secret) FROM ONLY person:me;
SELECT VALUE ->knows[WHERE out = person:secret]->person FROM ONLY person:me;
SELECT VALUE id FROM knows WHERE out = person:secret;

-- The hop admits exactly the edges it admits when `out` reads NONE.
SELECT VALUE ->(knows WHERE out = NONE) FROM ONLY person:me;
SELECT VALUE ->(knows WHERE out != person:secret) FROM ONLY person:me;
SELECT VALUE ->(knows WHERE out != person:secret AND in = person:me) FROM ONLY person:me;
SELECT VALUE ->(knows WHERE out = person:secret OR out = person:friend) FROM ONLY person:me;

-- A `WHERE` reading a parameter is never judged from the key: the record
-- path the hop takes for every comparison above answers the same.
LET $s = person:secret;
SELECT VALUE ->(knows WHERE out = $s) FROM ONLY person:me;
