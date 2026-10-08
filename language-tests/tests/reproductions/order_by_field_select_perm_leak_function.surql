/**
[env]
imports = ["reproductions/order_by_field_select_perm_leak_function_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:attacker" }

[test]
reason = "Runtime (DynamicScan) counterpart to order_by_field_select_perm_leak: the restricted ORDER BY runs inside a DEFINE FUNCTION body, which is planned via the runtime-resolved DynamicScan fallback rather than the plan-time access path. A record user must still not recover the hidden `code` ordering — the execute-time guard in operators/scan/dynamic.rs must withhold the ORDER BY so the rows stay in record-id order. Without that guard the function's index walk leaks the hidden value order ([secret:2, secret:3, secret:1] for ASC)."

# ORDER BY code ASC inside the function: must stay in record-id order, not the
# hidden value order [secret:2, secret:3, secret:1].
[[test.results]]
value = "[secret:1, secret:2, secret:3]"

# ORDER BY code DESC inside the function: likewise record-id order, not
# [secret:1, secret:3, secret:2].
[[test.results]]
value = "[secret:1, secret:2, secret:3]"

# ORDER BY code LIMIT 2 inside the function: record-id order first two, not the
# two smallest hidden values [secret:2, secret:3].
[[test.results]]
value = "[secret:1, secret:2]"

*/

fn::secret_ids_by_code();
fn::secret_ids_by_code_desc();
fn::secret_ids_by_code_limited();
