/**
[env]
imports = ["reproductions/graph_edge_field_select_perm_leak_import.surql"]
auth = { level = "owner" }

[test]
reason = "Companion to graph_edge_field_select_perm_leak.surql: a privileged (owner) session has permission checks disabled, so field-level SELECT permissions must NOT filter anything — but read-time COMPUTED fields must still be evaluated when a graph/reference traversal materialises full records. Guards against a regression where computed-field evaluation in `resolve_record_batch` is mistakenly gated behind `check_perms`."

# Owner sees every field (no permission filtering), and the computed `label`
# is still evaluated -- both for the direct SELECT and the graph traversal.
[[test.results]]
value = "[{ id: knows:e1, in: person:bob, out: person:alice, public: 'pub', secret: 'classified', owner_only: 'mine', label: 'lbl_pub' }]"

[[test.results]]
value = "[{ id: knows:e1, in: person:bob, out: person:alice, public: 'pub', secret: 'classified', owner_only: 'mine', label: 'lbl_pub' }]"

[[test.results]]
value = "[{ id: comment:c1, author: person:bob, body: 'hi', secret: 'classified', tag: 'tag_hi' }]"

[[test.results]]
value = "[{ id: comment:c1, author: person:bob, body: 'hi', secret: 'classified', tag: 'tag_hi' }]"

# Owner materialising the target vertex sees the restricted `pin` (no
# permission filtering).
[[test.results]]
value = "[{ id: person:alice, pin: 'secret_pin' }]"

*/

SELECT * FROM knows;
person:bob->(SELECT * FROM knows);
SELECT * FROM comment;
person:bob<~(SELECT * FROM comment);
person:bob->knows->(SELECT * FROM person);
