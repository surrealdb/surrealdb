/**
[env]
imports = ["reproductions/order_by_field_select_perm_leak_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:attacker" }

[test]
reason = "A record user with table SELECT but a restrictive field-level SELECT permission must not recover the ordering of the hidden field via ORDER BY when an index covers it. The field value is correctly nulled; the row order must NOT follow the hidden values (it must stay in record-id order), otherwise the index walk leaks a value-ordering oracle across other users' records."

# ORDER BY code ASC: with the leak the order would be [secret:2, secret:3, secret:1]
# (hidden a < c < m). The fix keeps the source in record-id order.
[[test.results]]
value = "[secret:1, secret:2, secret:3]"

# ORDER BY code DESC: with the leak the order would be [secret:1, secret:3, secret:2]
# (hidden m > c > a). Again the fix keeps record-id order.
[[test.results]]
value = "[secret:1, secret:2, secret:3]"

# ORDER BY code LIMIT 2: exercises the TopK / LIMIT pushdown path. The threshold
# probe is disarmed for permission-restricted fields (see field_state_blocks_raw_read
# in topk_pushdown.rs), so the scan stays in record-id order and the LIMIT takes the
# first two record-id-ordered rows rather than the two smallest hidden values. With
# the leak this would be [secret:2, secret:3] (hidden a, c).
[[test.results]]
value = "[secret:1, secret:2]"

# Confidentiality is preserved: the restricted field is nulled for the non-owner.
[[test.results]]
value = "[{ code: NONE }, { code: NONE }, { code: NONE }]"

*/

SELECT VALUE id FROM secret ORDER BY code;
SELECT VALUE id FROM secret ORDER BY code DESC;
SELECT VALUE id FROM secret ORDER BY code LIMIT 2;
SELECT code FROM secret;
