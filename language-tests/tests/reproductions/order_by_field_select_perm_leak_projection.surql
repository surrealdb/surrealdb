/**
[env]
imports = ["reproductions/order_by_field_select_perm_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:attacker" }
planner-strategy = ["all-ro"]

[test]
reason = "The value-ordering oracle guard must also hold for an ordinary (non-VALUE) projection that orders by a restricted field absent from the projection. Hidden order is a < c < m, so a leak would emit [secret:2, secret:3, secret:1] ascending and [secret:1, secret:3, secret:2] descending; the guard keeps the source in record-id order instead."

# ORDER BY code ASC: record-id order, not hidden-value order.
[[test.results]]
value = "[{ id: secret:1 }, { id: secret:2 }, { id: secret:3 }]"

# ORDER BY code DESC: likewise.
[[test.results]]
value = "[{ id: secret:1 }, { id: secret:2 }, { id: secret:3 }]"

# LIMIT 2 exercises the TopK / LIMIT pushdown path. A leak would give
# [secret:2, secret:3] (hidden a, c).
[[test.results]]
value = "[{ id: secret:1 }, { id: secret:2 }]"

# Confidentiality holds when the restricted field is both projected and ordered on.
[[test.results]]
value = "[{ code: NONE, id: secret:1 }, { code: NONE, id: secret:2 }, { code: NONE, id: secret:3 }]"

*/

SELECT id FROM secret ORDER BY code;
SELECT id FROM secret ORDER BY code DESC;
SELECT id FROM secret ORDER BY code LIMIT 2;
SELECT id, code FROM secret ORDER BY code;
