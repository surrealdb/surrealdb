/**
[env]
imports = ["reproductions/order_by_field_select_perm_leak_import.surql"]
auth = { level = "owner" }

[test]
reason = "Positive counterpart to order_by_field_select_perm_leak: a privileged actor (database owner) bypasses field-level SELECT permission checks, so the value-ordering guard must NOT be applied. The owner sees `code`, the planner keeps index-covered sort elimination, and ORDER BY returns the rows in true value order. This proves the guard is scoped to permission-restricted actors and does not over-apply / corrupt results for users who can legitimately read the field."

# ORDER BY code ASC: owner sees the real values (a < c < m), so the rows come back
# in true value order: [secret:2 (a), secret:3 (c), secret:1 (m)].
[[test.results]]
value = "[secret:2, secret:3, secret:1]"

# ORDER BY code DESC: reverse value order (m > c > a): [secret:1, secret:3, secret:2].
[[test.results]]
value = "[secret:1, secret:3, secret:2]"

# The field is fully visible to the owner (in record-id order, no ORDER BY).
[[test.results]]
value = "[{ code: 'm' }, { code: 'a' }, { code: 'c' }]"

*/

SELECT VALUE id FROM secret ORDER BY code;
SELECT VALUE id FROM secret ORDER BY code DESC;
SELECT code FROM secret;
