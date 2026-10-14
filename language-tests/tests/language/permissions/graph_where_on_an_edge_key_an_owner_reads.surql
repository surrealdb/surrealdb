/**
[env]
imports = ["language/permissions/graph_where_on_a_hidden_edge_key_import.surql"]

[test]
reason = "An owner's reads are not checked, so a graph hop `WHERE` over the edge's `out` sees the stored value the field SELECT permission hides from a record user (graph_where_on_a_hidden_edge_key)."

[[test.results]]
value = "[knows:to_secret]"

[[test.results]]
value = "[person:secret]"

[[test.results]]
value = "[knows:to_secret]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[knows:to_friend]"
*/

SELECT VALUE ->(knows WHERE out = person:secret) FROM ONLY person:me;
SELECT VALUE ->knows[WHERE out = person:secret]->person FROM ONLY person:me;
SELECT VALUE id FROM knows WHERE out = person:secret;
SELECT VALUE ->(knows WHERE out = NONE) FROM ONLY person:me;
SELECT VALUE ->(knows WHERE out != person:secret) FROM ONLY person:me;
